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

    def initialize(self):
        self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()
        self.logger = Logging.logger(self.job_name)
        self.logger.info("🛠 Initializing job configuration...")
        self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()
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
        delta_table = DeltaTable.forName(self.spark, target)
        pk_list = primary_key.split(',')
        merge_condition = ' AND '.join([f"target.{pk} = source.{pk}" for pk in pk_list])

        self.logger.info(f"Executing MERGE on {target} with condition: {merge_condition}")

        delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        self.logger.info(f"MERGE completed on {target}")

    def write_raw_data(self, dataframe, volume_path, format, mode):
        self.logger.info(f"Writing raw data to volume at path: {volume_path}")
        self.logger.info(f"Format: {format}, Mode: {mode}, Record count: {dataframe.count()}")
        DatabricksConnector.volume_writer(dataframe, volume_path, format, mode)
        self.logger.info("Raw data write completed.")

    def process_table(self, table, source_conf, dest_conf, job_history_conf, region, initial_start_time):
        sc = self.spark.sparkContext
        job_info_dict = {}
        start_time = JobInfo.get_current_utc_ts()

        self.logger.info(f"\n{'='*80}\n▶️ Starting processing for table: {table}\n{'='*80}")

        if self.params.load_type == "full_load":
            self.logger.info(f"Full load for table: {table}")
            query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
            last_watermark_str = "NA"
        else:
            watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
            last_watermark = self.watermark.fetch_watermark(watermark_table, table, Constants.LAST_FETCH_COLUMN_NAME)

            if last_watermark is None:
                self.logger.info(f"No previous watermark found. Performing full load for table: {table}")
                query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
                last_watermark_str = "NA"
            else:
                last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
                query = f"(SELECT * FROM [dbo].[{table}] WHERE modified > '{last_watermark}' AND modified <= '{initial_start_time}') AS {table}_alias"

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
                password
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to read from source for table {table}: {e}")
            return

        volume_path = self.watermark.latest_volume_path(dest_conf['volume_path'], table, initial_start_time)
        df = df.withColumn("load_datetimestamp", lit(initial_start_time))
        record_processed = df.count()
        if record_processed > 0:
            self.logger.info(f"✅ {record_processed} records fetched for table: {table}")
            df.show(5, truncate=False)
        else:
            self.logger.warning(f"⚠️ No records fetched from source for table: {table}")

        if record_processed > 0:
            self.write_raw_data(df, volume_path, dest_conf['file_format'], dest_conf['write_mode'])

            # Merge into discovery table
            target_table = f"{dest_conf['catalog']}.{dest_conf['discovery_schema']}.{table}"
            df_sanitized = Transform.sanitize_cols(df)
            primary_key = self.env_config.get("primary_key_details", {}).get(table)

            if primary_key:
                self.logger.info(f"Merging into {target_table} on primary key: {primary_key}")
                self.delta_merge(target_table, df_sanitized, primary_key)

                # Update watermark only for delta load after successful merge
                if self.params.load_type != "full_load":
                    watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
                    self.watermark.update_watermark(watermark_table, table, initial_start_time)
                    self.logger.info(f"Watermark updated for table: {table}")
            else:
                self.logger.warning(f"Primary key not defined for {table}, skipping merge.")

            # Append load summary
            self.load_report.append({
                "table_name": table,
                "load_mode": self.params.load_type.upper(),
                "record_count": record_processed
            })
        else:
            self.logger.info(f"No data found for table: {table}")

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

    def run(self):
        self.main()

    def main(self):
        try:
            self.initialize()
            self.logger.info("🚀 DataLoader job started")

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
            initial_start_time = datetime.utcnow()

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


def run():
    DataLoader().run()


if __name__ == "__main__":
    run()
