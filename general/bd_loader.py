from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
import traceback

bundle_src_path = sys.argv[2]
sys.path.append(bundle_src_path)

from naanalytics_dataloader.Constants import Constants
from naanalytics_dataloader.utils.config_reader import ConfigReader
from naanalytics_dataloader.utils.watermark import Watermark
from naanalytics_dataloader.utils.logger import Logging
from naanalytics_dataloader.utils.aws_utils import AWSUtils
from naanalytics_dataloader.utils.job_history import JobInfo
from naanalytics_dataloader.connectors.mssql_connector import MSSQLConnector
from naanalytics_dataloader.connectors.databricks_connector import DatabricksConnector
from naanalytics_dataloader.utils.transform import Transform
from delta.tables import DeltaTable


class DataLoader:
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.spark = None
        self.logger = None
        self.watermark = None
        self.configs = None
        self.params = None
        self.load_report = []

    def check_connection(self):
        source = self.env_config['source']
        username, password = AWSUtils.get_aws_secret_details(source['secret_name'], "us-east-1")
        jdbc_url = f"jdbc:sqlserver://{source['host']}:{source['port']};databaseName={source['database']}"
        self.logger.info(f"🔌 Checking JDBC connection to {jdbc_url}")
        try:
            test_df = self.spark.read.format("jdbc")\
                .option("url", jdbc_url)\
                .option("user", username)\
                .option("password", password)\
                .option("query", "SELECT 1")\
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
                .load()
            test_df.show()
            self.logger.info("✅ JDBC connection test passed.")
        except Exception as e:
            self.logger.error(f"❌ JDBC connection test failed: {e}")
            raise

    def initialize(self):
        self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()
        self.logger = Logging.logger(self.job_name)
        self.logger.info("🛠 Initializing job configuration...")
        self.watermark = Watermark(self.spark)
        configs = ConfigReader().get_configs()
        self.env_config = configs["env_config"]
        self.data_config = configs["data_config"]
        self.logger.info(f"📜 Full ENV configuration loaded: {self.env_config}")
        self.params = ConfigReader().get_param_options()
        self.logger.info(f"📂 Loaded ENV configuration keys: {list(self.env_config.keys())}")
        self.logger.info(f"📂 Loaded DATA configuration keys: {list(self.data_config.keys())}")
        self.logger.info(f"📁 Source Config: {self.env_config.get('source', {})}")
        self.logger.info(f"📁 Destination Config: {self.env_config.get('destination', {})}")
        self.logger.info(f"🔑 Primary Keys Config: {self.env_config.get('primary_key_details', {})}")

    def delta_merge(self, target, source_df, primary_key):
        try:
            self.spark.catalog.refreshTable(target)
        except Exception as e:
            self.logger.warning(f"⚠️ Could not refresh Delta table `{target}` metadata: {e}")

        try:
            delta_table = DeltaTable.forName(self.spark, target)
        except Exception as e:
            self.logger.error(f"❌ Delta table `{target}` not found or inaccessible: {e}")
            return

        df_cols = [c.lower().replace(" ", "_") for c in source_df.columns]
        target_cols = set(c.lower().replace(" ", "_") for c in self.spark.table(target).columns)
        pk_list = [pk.strip().lower().replace(" ", "_") for pk in primary_key.split(',')
                   if pk.strip().lower().replace(" ", "_") in df_cols and pk.strip().lower().replace(" ", "_") in target_cols]

        if not pk_list:
            self.logger.warning(f"⚠️ Merge keys not found in source DataFrame for `{target}`. Skipping merge.")
            return

        merge_condition = " AND ".join([f"target.`{pk}` = source.`{pk}`" for pk in pk_list])
        self.logger.info(f"🔁 Merging into {target} using condition: {merge_condition}")

        source_df.createOrReplaceTempView("source_temp")

        matched_count = self.spark.sql(f"""
            SELECT COUNT(*) AS cnt FROM {target} target
            INNER JOIN source_temp source ON {merge_condition}
        """).collect()[0]["cnt"]

        total_count = source_df.count()
        inserted_count = total_count - matched_count
        updated_count = matched_count

        update_set = {c: f"source.`{c}`" for c in df_cols}
        insert_set = {c: f"source.`{c}`" for c in df_cols}

        delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_set).execute()

        self.logger.info(f"✅ MERGE complete for `{target}` → Inserted: {inserted_count}, Updated: {updated_count}")

    def write_raw_data(self, dataframe, volume_path, format, mode):
        self.logger.info(f"Writing raw data to volume at path: {volume_path}")
        self.logger.info(f"Format: {format}, Mode: {mode}, Record count: {dataframe.count()}")
        DatabricksConnector.volume_writer(dataframe, volume_path, format, mode)
        self.logger.info("Raw data write completed.")

    def process_table(self, table, source_conf, dest_conf, job_history_conf, region, initial_start_time):
        sc = self.spark.sparkContext
        job_info_dict = {}
        start_time = JobInfo.get_current_utc_ts()
        domain = source_conf.get('domain')
        
        join_condition = self.data_config.get("delta_join_condition", {}).get(table)
        if join_condition:
            self.logger.info(f"📌 Join condition defined for {table}: {join_condition}")

        if self.params.load_type == "full_load":
            self.logger.info(f"Full load for table: {table}")
            modification_column = self.data_config.get("timestamp_column_map", {}).get(table, "modified")
            self.logger.info(f"⏱ Using modification column '{modification_column}' for table {table}")
            query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
            last_watermark_str = "NA"
        else:
            join_query = self.data_config.get("delta_join_query_map", {}).get(table)
            if join_query:
                self.logger.info(f"🧩 Using delta join query for table `{table}`")
                query = f"({join_query}) AS {table}_alias"
                last_watermark_str = "NA"
            else:
                watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
                modification_column = self.data_config.get("timestamp_column_map", {}).get(table, "modified")
                self.logger.info(f"⏱ Using modification column '{modification_column}' for table {table}")
                last_watermark = self.watermark.fetch_watermark(watermark_table, table, Constants.LAST_FETCH_COLUMN_NAME)
                if last_watermark is None:
                    self.logger.info(f"No previous watermark found. Performing full load for table: {table}")
                    query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
                    last_watermark_str = "NA"
                else:
                    last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
                    query = f"(SELECT * FROM [dbo].[{table}] WHERE {modification_column} > '{last_watermark}' AND {modification_column} <= '{initial_start_time}') AS {table}_alias"

        username, password = AWSUtils.get_aws_secret_details(source_conf['secret_name'], region)
        self.logger.info(f"🔐 MSSQL Username being used: {username}")
        self.logger.info(f"🧾 SQL Query: {query}")

        try:
            df = MSSQLConnector.reader(
                self.spark,
                source_conf['host'],
                source_conf['port'],
                source_conf['database'],
                query,
                username,
                password,
                domain=domain
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to read from source for table {table}: {e}")
            return

        volume_path = self.watermark.latest_volume_path(dest_conf['volume_path'], table, initial_start_time)
        df = df.withColumn("load_datetimestamp", lit(initial_start_time))
        record_processed = df.count()

        if record_processed == 0:
            self.logger.warning(f"⚠️ No records fetched from source for table: {table}")
            return

        self.logger.info(f"✅ {record_processed} records fetched for table: {table}")
        df.show(5, truncate=False)
        self.write_raw_data(df, volume_path, dest_conf['file_format'], dest_conf['write_mode'])

        # Discovery layer write
        df_sanitized = Transform.sanitize_cols(df)
        target_table = f"{dest_conf['catalog']}.{dest_conf['discovery_schema']}.{table}"

        if self.params.load_type == "full_load":
            if not DeltaTable.isDeltaTable(self.spark, target_table):
                self.logger.info(f"📦 Discovery table `{target_table}` does not exist. Creating with overwrite.")
            else:
                self.logger.info(f"📝 Overwriting existing discovery table: {target_table}")

            df_sanitized.write.mode("overwrite").format("delta").saveAsTable(target_table)
        else:
            primary_key = self.env_config.get("primary_key_details", {}).get(table)
            if not primary_key:
                self.logger.warning(f"⚠️ Primary key not defined for {table}. Using all columns as merge key.")
                primary_key = ",".join([c for c in df_sanitized.columns])
            self.logger.info(f"🔀 Performing MERGE on discovery table {target_table} with key: {primary_key}")
            if not DeltaTable.isDeltaTable(self.spark, target_table):
                self.logger.info(f"📦 Discovery table `{target_table}` does not exist. Creating before merge.")
                df_sanitized.write.mode("overwrite").format("delta").saveAsTable(target_table)
            self.delta_merge(target_table, df_sanitized, primary_key)

            # Update watermark after successful merge
            watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
            self.watermark.update_watermark(watermark_table, table, initial_start_time)
            self.logger.info(f"✅ Watermark updated for table: {table}")

        self.load_report.append({
            "table_name": table,
            "load_mode": self.params.load_type.upper(),
            "record_count": record_processed
        })

        end_time = JobInfo.get_current_utc_ts()
        job_info_dict = JobInfo.get_job_info(
            spark_context=sc,
            start_time=start_time,
            end_time=end_time,
            data_source_name=self.env_config.get("application_name"),
            source_configs=source_conf,
            source_schema=source_conf['database'],
            source_table=table,
            catalog_name=dest_conf['catalog'],
            dest_schema=dest_conf['raw_schema'],
            dest_table=table.lower(),
            dest_volume_name=dest_conf['volume_path'],
            dest_volume_path=volume_path,
            record_processed=record_processed,
            record_count=record_processed,
            status=Constants.SUCCEEDED,
            job_info_dict=job_info_dict,
            last_watermark=last_watermark_str,
            new_watermark=initial_start_time.strftime("%Y-%m-%d %H:%M:%S")
        )
        JobInfo.load_job_info(self.spark, sc, job_history_conf, job_info_dict)

    def main(self):
        try:
            region = self.spark.conf.get("spark.databricks.clusterUsageTags.dataPlaneRegion")
            self.logger.info(f"📍 Spark region: {region}")
            sc = self.spark.sparkContext
            source_conf = self.env_config['source']
            dest_conf = self.env_config['destination']
            job_history_conf = self.env_config['job_history_conf']

            table_list = self.data_config.get("source_table_names", [])
            if not table_list:
                self.logger.error("❌ No tables found in 'source_table_names'. Please check the config file.")
                return
            self.logger.info(f"📋 Table list to process: {table_list}")
            watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
            initial_start_time = self.watermark.fetch_watermark(watermark_table, '', Constants.CURRENT_FETCH_COLUMN_NAME)
            self.logger.info(f"🔁 Using global job watermark from watermark table: {initial_start_time}")

            for table in table_list:
                self.process_table(table, source_conf, dest_conf, job_history_conf, region, initial_start_time)

            if self.load_report:
                df_report = self.spark.createDataFrame(self.load_report)
                self.logger.info("\n📊 Final Load Summary:")
                df_report.show(truncate=False)
            else:
                self.logger.info("No tables were updated.")

            self.logger.info(f"\n✅ Raw and discovery data load completed for all tables.")

        except Exception as e:
            self.logger.error(f"Exception: {str(e)}")
            traceback.print_exc()
            raise






    def run(self):
        self.initialize()
        self.check_connection()
        self.logger.info("🚀 DataLoader job started")
        self.main()


if __name__ == '__main__':
    DataLoader().run()
