from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
import traceback

bundle_src_path = sys.argv[2]
sys.path.append(bundle_src_path)

from naanalytics_bd.Constants import Constants
from naanalytics_bd.utils.config_reader import ConfigReader
from naanalytics_bd.utils.watermark import Watermark
from naanalytics_bd.utils.logger import Logging
from naanalytics_bd.utils.aws_utils import AWSUtils
from naanalytics_bd.utils.job_history import JobInfo
from naanalytics_bd.connectors.mssql_connector import MSSQLConnector
from naanalytics_bd.connectors.databricks_connector import DatabricksConnector


class BdRawDataLoader:
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.spark = None
        self.logger = None
        self.watermark = None
        self.configs = None

    def initialize(self):
        self.logger = Logging.logger(self.job_name)
        self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()
        self.watermark = Watermark(self.spark)
        self.configs = ConfigReader().get_configs()["env_config"]

    def process_table(self, table, source_conf, dest_conf, job_history_conf, region, initial_start_time):
        sc = self.spark.sparkContext
        job_info_dict = {}
        start_time = JobInfo.get_current_utc_ts()

        self.logger.info(f"{Constants.BOLD}Processing table: {table}{Constants.RESET}")

        watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
        last_watermark = self.watermark.fetch_watermark(watermark_table, table, Constants.LAST_FETCH_COLUMN_NAME)

        if last_watermark is None:
            self.logger.info(f"Full load for table: {table}")
            query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
            last_watermark_str = "NA"
        else:
            last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
            query = f"(SELECT * FROM [dbo].[{table}] WHERE modified > '{last_watermark}' AND modified <= '{initial_start_time}') AS {table}_alias"

        username, password = AWSUtils.get_aws_secret_details(source_conf['secret_name'], region)

        df = MSSQLConnector.reader(
            self.spark,
            source_conf['host'],
            source_conf['port'],
            source_conf['database'],
            query,
            username,
            password
        )

        volume_path = self.watermark.latest_volume_path(dest_conf['volume_path'], table, initial_start_time)
        df = df.withColumn("load_datetimestamp", lit(initial_start_time))
        record_processed = df.count()

        if record_processed > 0:
            DatabricksConnector.volume_writer(df, volume_path, dest_conf['file_format'], dest_conf['write_mode'])
            self.logger.info(f"Loaded {record_processed} records into volume: {volume_path}")
        else:
            self.logger.info(f"No data found for table: {table}")

        end_time = JobInfo.get_current_utc_ts()
        job_info_dict = JobInfo.get_job_info(
            spark_context=sc,
            start_time=start_time,
            end_time=end_time,
            data_source_name=Constants.BD,
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
            self.initialize()

            region = self.spark.conf.get("spark.databricks.clusterUsageTags.dataPlaneRegion")
            sc = self.spark.sparkContext
            source_conf = self.configs['source']
            dest_conf = self.configs['destination']
            job_history_conf = self.configs['job_history_conf']

            table_list = self.configs.get("all_source_table_names", [])
            watermark_table = f"{dest_conf['catalog']}.{job_history_conf['schema']}.{dest_conf['watermark_table']}"
            initial_start_time = self.watermark.fetch_watermark(watermark_table, '', Constants.CURRENT_FETCH_COLUMN_NAME)

            for table in table_list:
                self.process_table(table, source_conf, dest_conf, job_history_conf, region, initial_start_time)

            self.logger.info(f"{Constants.BOLD}Raw data load completed for all tables.{Constants.RESET}")

        except Exception as e:
            self.logger.error(f"Exception: {str(e)}")
            traceback.print_exc()
            raise


if __name__ == "__main__":
    BdRawDataLoader().main()
