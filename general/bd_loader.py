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

    def main(self):
        try:
            logger = Logging.logger(self.job_name)
            spark = SparkSession.builder.appName(self.job_name).getOrCreate()
            sc = spark.sparkContext
            watermark = Watermark(spark)

            job_id = sc.getLocalProperty("spark.databricks.job.id")
            logger.info(f"Databricks Job ID: {job_id}")
            region = spark.conf.get("spark.databricks.clusterUsageTags.dataPlaneRegion")

            # Load config
            env_config = ConfigReader().get_configs()["env_config"]
            job_history_conf = env_config.get("job_history_conf")
            source_conf = env_config.get("source")
            dest_conf = env_config.get("destination")

            database = source_conf.get("database")
            secret_name = source_conf.get("secret_name")
            table_list = env_config.get("all_source_table_names")

            catalog = dest_conf.get("catalog")
            schema = dest_conf.get("raw_schema")
            discovery_schema = dest_conf.get("discovery_schema")
            volume_path_root = dest_conf.get("volume_path")
            file_format = dest_conf.get("file_format")
            write_mode = dest_conf.get("write_mode")
            watermark_table_name = dest_conf.get("watermark_table")
            watermark_table = f"{catalog}.{job_history_conf.get('schema')}.{watermark_table_name}"

            username, password = AWSUtils.get_aws_secret_details(secret_name, region)
            initial_start_time = watermark.fetch_watermark(watermark_table, '', Constants.CURRENT_FETCH_COLUMN_NAME)
            logger.info(f"Initial job-wide watermark: {initial_start_time}")

            for table in table_list:
                job_info_dict = {}
                start_time = JobInfo.get_current_utc_ts()
                logger.info(f"{Constants.BOLD}Processing table: {table}{Constants.RESET}")

                last_watermark = watermark.fetch_watermark(watermark_table, table, Constants.LAST_FETCH_COLUMN_NAME)
                logger.info(f"Last watermark for {table}: {last_watermark}")

                if last_watermark is None:
                    logger.info(f"Full load triggered for {table}")
                    query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
                    last_watermark_str = "NA"
                else:
                    last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
                    query = f"(SELECT * FROM [dbo].[{table}] WHERE modified > '{last_watermark}' AND modified <= '{initial_start_time}') AS {table}_alias"

                logger.info(f"Query: {query}")
                df = MSSQLConnector.reader(spark, source_conf["host"], source_conf["port"], database, query, username, password)

                volume_path = watermark.latest_volume_path(volume_path_root, table, initial_start_time)
                df = df.withColumn("load_datetimestamp", lit(initial_start_time))
                record_processed = df.count()

                if record_processed > 0:
                    logger.info(f"Writing {record_processed} rows to {volume_path}")
                    DatabricksConnector.volume_writer(df, volume_path, file_format, write_mode)
                else:
                    logger.info(f"No data found for {table}, skipping write.")

                end_time = JobInfo.get_current_utc_ts()

                job_info_dict = JobInfo.get_job_info(
                    spark_context=sc,
                    start_time=start_time,
                    end_time=end_time,
                    data_source_name=Constants.BD,
                    source_configs=source_conf,
                    source_schema=database,
                    source_table=table,
                    catalog_name=catalog,
                    dest_schema=schema,
                    dest_table=table.lower(),
                    dest_volume_name=volume_path_root,
                    dest_volume_path=volume_path,
                    record_processed=record_processed,
                    record_count=record_processed,
                    status=Constants.SUCCEEDED,
                    job_info_dict=job_info_dict,
                    last_watermark=last_watermark_str,
                    new_watermark=initial_start_time.strftime("%Y-%m-%d %H:%M:%S")
                )

                JobInfo.load_job_info(spark, sc, job_history_conf, job_info_dict)

            logger.info(f"{Constants.BOLD}Raw data load completed for all tables.{Constants.RESET}")

        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            traceback.print_exc()
            raise


if __name__ == "__main__":
    job = BdRawDataLoader()
    job.main()
