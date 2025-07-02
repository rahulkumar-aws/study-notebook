import traceback

from delta.tables import DeltaTable
from pyspark.sql.functions import lit

from naanalytics_dataloader.dependencies.connectors.databricks_connector import DatabricksConnector
from naanalytics_dataloader.dependencies.connectors.mssql_connector import MSSQLConnector
from naanalytics_dataloader.dependencies.core.constants import Constants
from naanalytics_dataloader.dependencies.processors.transform import Transform
from naanalytics_dataloader.dependencies.utils.job_history import JobInfo
from naanalytics_dataloader.jobs.common.base_etl_job import BaseETLJob


class DataLoader(BaseETLJob):
    def __init__(self):
        super().__init__(job_name=self.__class__.__name__)
        self.load_report = []

    def is_delta_table(self, table_name) -> bool:
        try:
            DeltaTable.forName(self.spark, table_name)
            return True
        except Exception:
            return False

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

        update_set = {c: f"source.`{c}`" for c in df_cols if c != "load_datetimestamp"}
        insert_set = {c: f"source.`{c}`" for c in df_cols}

        delta_table.alias("target").merge(
            source_df.alias("source"), merge_condition
        ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_set).execute()

        self.logger.info(f"✅ [MERGE COMPLETE] Table `{target}`: {inserted_count} inserted, {updated_count} updated")

    def write_raw_data(self, dataframe, volume_path, format, mode):
        self.logger.info(f"Writing raw data to volume at path: {volume_path}")
        self.logger.info(f"Format: {format}, Mode: {mode}, Record count: {dataframe.count()}")
        DatabricksConnector.volume_writer(dataframe, volume_path, format, mode)
        self.logger.info("Raw data write completed.")

    def process_table(self, table, current_watermark, last_watermark):
        self.logger.info(f"Process Table Function Started for \n Table {table} \n Current Watermark {current_watermark} \n Last Watermark {last_watermark}")
        sc = self.spark.sparkContext
        job_info_dict = {}
        start_time = JobInfo.get_current_utc_ts()
        domain = self.source_conf.get('domain')

        if last_watermark is None:
            self.logger.warning("⚠️ No last watermark found. Switching to full_load mode.")
            load_type = "full_load"
        else:
            self.logger.info(f"🔁 Using last job watermark from watermark table: {last_watermark} DEBUG: TYPE: {type(last_watermark)}")
            load_type = self.params.load_type


        self.logger.info(f"🚀 Starting processing for table: {table} with load_type: {load_type}")

        last_watermark_str = "NA"
        target_table = f"{self.dest_conf['catalog']}.{self.dest_conf['discovery_schema']}.{table}"
        query = self._prepare_query(table, load_type, current_watermark, last_watermark)

        self.logger.info(f"🧾 SQL Query: {query}")

        try:
            primary_key = self.primary_key_map.get(table)
        if load_type == 'full_load' and primary_key:
            df = self._read_with_partitioning(table, query, primary_key)
        else:
            df = MSSQLConnector.reader(
                self.spark, self.source_conf['host'], self.source_conf['port'], self.source_conf['database'],
                query, self.username, self.password, domain
            )
        except Exception as e:
            self.logger.error(f"❌ Failed to read from source for table {table}: {e}")
            return

        volume_path = self.watermark.latest_volume_path(self.dest_conf['volume_path'], table, current_watermark)
        df = df.withColumn("load_datetimestamp", lit(current_watermark))
        record_processed = df.count()
        if record_processed == 0:
            self.logger.warning(f"⚠️ No records fetched from source for table: {table}")
            return

        self.logger.info(f"✅ {record_processed} records fetched for table: {table}")
        self.write_raw_data(df, volume_path, self.dest_conf['file_format'], self.dest_conf['write_mode'])

        df_sanitized = Transform.sanitize_cols(df)
        self._write_to_discovery_layer(df_sanitized, table, target_table, load_type)

        self.watermark.update_watermark(self.watermark_table, table, current_watermark)
        self.logger.info(f"🆙 Watermark updated for table: {table} to {current_watermark}")

        end_time = JobInfo.get_current_utc_ts()
        job_info_dict = JobInfo.get_job_info(
            spark_context=sc,
            start_time=start_time,
            end_time=end_time,
            data_source_name=self.env_config.get("application_name"),
            source_configs=self.source_conf,
            source_schema=self.source_conf['database'],
            source_table=table,
            catalog_name=self.dest_conf['catalog'],
            dest_schema=self.dest_conf['raw_schema'],
            dest_table=table.lower(),
            dest_volume_name=self.dest_conf['volume_path'],
            dest_volume_path=volume_path,
            record_processed=record_processed,
            record_count=record_processed,
            status=Constants.SUCCEEDED,
            job_info_dict=job_info_dict,
            last_watermark=last_watermark_str,
            new_watermark=current_watermark.strftime("%Y-%m-%d %H:%M:%S")
        )
        JobInfo.load_job_info(self.spark, sc, self.job_history_conf, job_info_dict)
        self.logger.info(f"📘 Job metadata saved for table: {table}")

        self.load_report.append({
            "table_name": table,
            "load_mode": load_type.upper(),
            "record_count": record_processed
        })

    def _prepare_query(self, table, load_type, current_watermark, last_watermark):
        _delta_table = f"{self.dest_conf['catalog']}.{self.dest_conf['discovery_schema']}.{table}"
        self.logger.info(f"[_prepare_query function] _delta_table : {_delta_table}")
        res = self.is_delta_table(f"{_delta_table}")
        print(f"self.is_delta_table {res} and current load_type {load_type}")
        if not self.is_delta_table(f"{_delta_table}") or load_type == "full_load":
            self.logger.info(f"📥 Full load query prepared for table: {table}")
            return f"(SELECT * FROM [{self.source_schema}].[{table}]) AS {table}_alias"
        elif load_type == "delta":
            return self._prepare_delta_query(table, current_watermark, last_watermark)
        else:
            self.logger.error(f"❌ Unsupported load_type: {load_type}")
            raise ValueError(f"❌ Unsupported load_type: {load_type}")

    def _prepare_delta_query(self, table, current_watermark, last_watermark):
        join_condition = self.data_config.get("delta_join_condition", {}).get(table)
        if join_condition:
            self.logger.info(f"📌 Join condition defined for {table}: {join_condition}")

        if last_watermark is None:
            self.logger.info(f"🕳 No previous watermark found. Performing full load for table: {table}")
            return f"(SELECT * FROM [{self.source_schema}].[{table}]) AS {table}_alias"

        timestamp_column_map = self.timestamp_column_map
        self.logger.info(f"inside _prepare_delta_query {timestamp_column_map}")
        join_by_servicerequestid_map = self.join_by_servicerequestid_map
        if bool(timestamp_column_map):
            modification_column = timestamp_column_map.get(table, "ModifiedTime")
            last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"💧 Last watermark for delta load: {last_watermark_str}")
            return f"(SELECT * FROM [{self.source_schema}].[{table}] WHERE {modification_column} > '{last_watermark}' AND {modification_column} <= '{current_watermark}') AS {table}_alias"

        if bool(join_by_servicerequestid_map):
            primary_key = join_by_servicerequestid_map.get(table)
            self.logger.info(f"🔗 Using join_by_servicerequestid_map for table `{table}` with key `{primary_key}`")
            srh_last_wm, srh_curr_wm = self.watermark.fetch_watermark(
                self.watermark_table, 'service_request_header',
                Constants.LAST_SUCCESS_RUNDATE,
                Constants.CURRENT_RUNDATE
            )
            self.logger.info(f"🔁 Join watermark range for Service_Request_Header: {srh_last_wm} -> {srh_curr_wm}")
            keys_query = f"""
                (
                SELECT src.{primary_key}
                FROM [{self.source_schema}].[{table}] AS src
                JOIN [{self.source_schema}].Service_Request_Header AS srh
                    ON src.ServiceRequestId = srh.ServiceRequestId
                WHERE srh.ModifiedTime > '{srh_last_wm.strftime('%Y-%m-%d %H:%M:%S')}'
                    AND srh.ModifiedTime <= '{srh_curr_wm.strftime('%Y-%m-%d %H:%M:%S')}'
                ) AS {table}_keys
            """
            self.logger.info(f"🗝️ Keys query: {keys_query}")
            key_primary_key = self.primary_key_map.get(table)
        if load_type == 'full_load' and primary_key:
            df = self._read_with_partitioning(table, query, primary_key)
        else:
            df = MSSQLConnector.reader(
                self.spark, self.source_conf['host'], self.source_conf['port'],
                self.source_conf['database'], keys_query,
                self.username, self.password, self.source_conf.get('domain')
            )
            keys = [str(row[primary_key]) for row in key_df.collect()]
            if not keys:
                self.logger.info(f"📭 No records found to process for table: {table}")
                return f"(SELECT * FROM [{self.source_schema}].[{table}] WHERE 1=0) AS {table}_alias"
            else:
                formatted_keys = ",".join(f"'{k}'" for k in keys)
                return f"(SELECT * FROM [{self.source_schema}].[{table}] WHERE {primary_key} IN ({formatted_keys})) AS {table}_alias"

    def _write_to_discovery_layer(self, df_sanitized,table, target_table, load_type):
        self.logger.info(f"💾 Started Writing data to discovery layer table: {target_table}")

        if load_type == "full_load":
            self.logger.info(f"📝 Performing overwrite to discovery table: {target_table}")
            df_sanitized.write.mode("overwrite").format("delta").saveAsTable(target_table)
        elif load_type == "append":
            self.logger.info(f"➕ Performing append to discovery table: {target_table}")
            df_sanitized.write.mode("append").format("delta").saveAsTable(target_table)
        elif load_type == "delta":
            if not self.is_delta_table(target_table):
                self.logger.info(f"📦 Creating discovery table `{target_table}`.")
                df_sanitized.write.mode("overwrite").format("delta").saveAsTable(target_table)
            else:
                self.logger.info(f"self.primary_key_map {self.primary_key_map}")
                primary_key = self.primary_key_map.get(table)
                if not primary_key:
                    self.logger.error(f"❌ Primary key not defined for {table}. Cannot perform MERGE operation.")
                    # Optionally, raise an exception or handle the error as needed
                    raise ValueError(f"Primary key not defined for {target_table}. Merge operation aborted.")
                self.logger.info(f"🔀 Performing MERGE on discovery table {target_table} with key: {primary_key}")
                self.delta_merge(target_table, df_sanitized, primary_key)

    
    def _read_with_partitioning(self, table, query, primary_key):
        """Use partitioned JDBC read for full load based on primary key."""
        import math

        self.logger.info(f"📌 Using partition column: {primary_key} for table {table}")

        bounds_query = f"(SELECT MIN([{primary_key}]) AS min_id, MAX([{primary_key}]) AS max_id FROM {query[1:-len(f' AS {table}_alias')]}) AS bounds"

        bounds_df = MSSQLConnector.reader(
            self.spark, self.source_conf['host'], self.source_conf['port'],
            self.source_conf['database'], bounds_query,
            self.username, self.password, self.source_conf.get('domain')
        )

        row = bounds_df.collect()[0]
        min_id = row["min_id"]
        max_id = row["max_id"]

        if min_id is None or max_id is None or min_id == max_id:
            self.logger.warning(f"⚠️ Cannot determine bounds for partitioning on `{table}`. Fallback to single partition.")
            return MSSQLConnector.reader(
                self.spark, self.source_conf['host'], self.source_conf['port'],
                self.source_conf['database'], query,
                self.username, self.password, self.source_conf.get('domain')
            )

        num_partitions = 4
        self.logger.info(f"📊 Partitioning range for `{table}`: {min_id} to {max_id} into {num_partitions} partitions")

        return self.spark.read.format("jdbc") \
            .option("url", f"jdbc:sqlserver://{self.source_conf['host']}:{self.source_conf['port']};database={self.source_conf['database']}") \
            .option("dbtable", query) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("partitionColumn", primary_key) \
            .option("lowerBound", str(min_id)) \
            .option("upperBound", str(max_id)) \
            .option("numPartitions", str(num_partitions)) \
            .option("fetchsize", "10000") \
            .load()

def main(self):
        try:
            table_list = self.data_config["objects"].get("source_table_names", [])

            if not table_list:
                self.logger.error("❌ No tables found in 'source_table_names'. Please check the config file.")
                return
            self.logger.info(f"📋 ++ Table list to process ++: {table_list}")
            self.logger.info(f"Primary Key info for tables {self.primary_key_map}")

            for table in table_list:
                self.logger.info(f"Data Ingestion started for Table : {table}")
                last_watermark, current_watermark = self.watermark.fetch_watermark(self.watermark_table, table,
                                                                                   Constants.LAST_SUCCESS_RUNDATE,
                                                                                   Constants.CURRENT_RUNDATE)
                self.logger.info(f"current_watermark {current_watermark} :: Last watermark {last_watermark} for table {table}")

                self.process_table(table, current_watermark, last_watermark)

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

    def start(self):
        self.initialize()
        self.logger.info("🚀 DataLoader job started Broker Desktop.")
        self.main()


def run():
    DataLoader().start()

# if __name__ == '__main__':
#     run()
