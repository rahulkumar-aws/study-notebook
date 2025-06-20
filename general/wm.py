from datetime import datetime, timezone
from pyspark.sql import SparkSession
import sys
import traceback

bundle_src_path = sys.argv[2]
sys.path.append(bundle_src_path)

from dummy.dependencies.core.constants import Constants
from dummy.dependencies.utils.job_history import JobInfo
from dummy.dependencies.core.logger import Logging
from dummy.dependencies.utils.watermark import Watermark
from dummy.dependencies.utils.config_reader import ConfigReader


class WatermarkManager:
    def __init__(self):
        self.job_name = self.__class__.__name__

    def main(self):
        logger = Logging.logger(self.job_name)
        try:
            spark_session = SparkSession.builder.appName(self.job_name).getOrCreate()
            spark_context = spark_session.sparkContext.getOrCreate()
            watermark = Watermark(spark_session)

            # Job metadata
            dbc_job_run_id = spark_context.getLocalProperty("spark.databricks.job.id")
            logger.info(f"🔁 Job Run ID: {dbc_job_run_id}")
            region_name = spark_session.conf.get("spark.databricks.clusterUsageTags.dataPlaneRegion")

            # Load configs
            env_configs = ConfigReader().get_configs()["env_config"]
            job_history_conf = env_configs["job_history_conf"]
            dest_configs = env_configs["destination"]

            application_name = env_configs.get("application_name", "default").lower()
            catalog = dest_configs["catalog"]
            metadata_schema = job_history_conf["schema"]
            base_table_name = dest_configs["watermark_table"]

            watermark_table = f"{catalog}.{metadata_schema}.{application_name}_{base_table_name}"
            all_tables = env_configs.get("all_source_table_names", [])

            logger.info(f"🧱 Ensuring watermark table exists: `{watermark_table}`")
            watermark.create_watermark_table(watermark_table)

            logger.info("🗃️ Merging all configured source tables into watermark table...")
            spark_session.createDataFrame([(t,) for t in all_tables], ["table_name"]).createOrReplaceTempView("new_table_names")
            spark_session.sql(f"""
                MERGE INTO {watermark_table} AS target
                USING new_table_names AS source
                ON target.table_name = source.table_name
                WHEN NOT MATCHED THEN INSERT (table_name) VALUES (source.table_name)
            """)

            now_ts = JobInfo.get_current_utc_ts()
            logger.info(f"🕒 Updating global watermark timestamp: {Constants.CURRENT_FETCH_COLUMN_NAME} = {now_ts}")
            watermark.update_current_watermark(watermark_table, Constants.CURRENT_FETCH_COLUMN_NAME, now_ts)

            logger.info(f"{Constants.BOLD}✅ Watermark update complete for all tables in `{application_name}`!{Constants.RESET}")

        except Exception as error:
            logger.error("❌ Error occurred during watermark initialization")
            for line in traceback.format_exception(*sys.exc_info()):
                logger.error(line)
            raise


if __name__ == "__main__":
    WatermarkManager().main()
