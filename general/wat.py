import sys
from datetime import datetime
from pyspark.sql import SparkSession
from dummy.dependencies.core.logger import Logging


class Watermark:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = Logging.logger(self.__class__.__name__)

    def create_watermark_table(self, watermark_table_name: str):
        """
        Creates the watermark table with schema:
        - table_name: STRING
        - last_updated: TIMESTAMP
        - latest_run_time: TIMESTAMP
        """
        try:
            self.logger.info(f"📦 Creating watermark table if not exists: {watermark_table_name}")
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {watermark_table_name} (
                    table_name STRING,
                    last_updated TIMESTAMP,
                    latest_run_time TIMESTAMP
                )
                USING DELTA
            """)
            self.logger.info(f"✅ Watermark table ready: {watermark_table_name}")
        except Exception as e:
            self.logger.error(f"❌ Failed to create watermark table: {e}")
            raise

    def update_current_watermark(self, watermark_table_name: str, column_name: str, current_read_timestamp: str):
        """
        Updates a global column (like current_fetch_column) in all rows.
        """
        try:
            self.logger.info(f"🕓 Setting global watermark column `{column_name}` to {current_read_timestamp}")
            self.spark.sql(f"""
                UPDATE {watermark_table_name}
                SET {column_name} = '{current_read_timestamp}'
            """)
            self.logger.info(f"✅ Global watermark `{column_name}` updated across all rows")
        except Exception as e:
            self.logger.error(f"❌ Failed to update global watermark column `{column_name}`: {e}")
            raise

    def update_watermark(self, watermark_table_name: str, table_name: str, last_updated: str, latest_run_time: str):
        """
        Updates both last_updated and latest_run_time for a specific table.
        """
        try:
            self.logger.info(f"📝 Updating watermark for table `{table_name}`")
            self.logger.debug(f"  - last_updated: {last_updated}")
            self.logger.debug(f"  - latest_run_time: {latest_run_time}")

            self.spark.sql(f"""
                MERGE INTO {watermark_table_name} AS target
                USING (SELECT '{table_name}' AS table_name,
                              '{last_updated}' AS last_updated,
                              '{latest_run_time}' AS latest_run_time) AS source
                ON target.table_name = source.table_name
                WHEN MATCHED THEN UPDATE SET
                    target.last_updated = source.last_updated,
                    target.latest_run_time = source.latest_run_time
                WHEN NOT MATCHED THEN INSERT (table_name, last_updated, latest_run_time)
                VALUES (source.table_name, source.last_updated, source.latest_run_time)
            """)
            self.logger.info(f"✅ Watermark updated for `{table_name}`")
        except Exception as e:
            self.logger.error(f"❌ Failed to update watermark for `{table_name}`: {e}")
            raise

    def fetch_watermark(self, watermark_table_name: str, table_name: str, column_name: str):
        """
        Fetches a specific watermark column (e.g., last_updated) for a given table.
        If table_name is empty, fetches distinct values from the column.
        """
        try:
            if table_name:
                self.logger.info(f"🔍 Fetching `{column_name}` for table `{table_name}`")
                query = f"""
                    SELECT {column_name} FROM {watermark_table_name}
                    WHERE LOWER(table_name) = '{table_name.lower()}'
                    LIMIT 1
                """
            else:
                self.logger.info(f"🔍 Fetching global column `{column_name}`")
                query = f"SELECT DISTINCT {column_name} FROM {watermark_table_name} LIMIT 1"

            df = self.spark.sql(query)
            result = df.collect()
            if result:
                self.logger.info(f"✅ Fetched value: {result[0][column_name]}")
                return result[0][column_name]
            else:
                self.logger.warning(f"⚠️ No watermark found for `{table_name}`.")
                return None
        except Exception as e:
            self.logger.error(f"❌ Error fetching watermark for `{table_name}.{column_name}`: {e}")
            raise

    def latest_volume_path(self, volume_name: str, table: str, current_watermark):
        """
        Constructs the delta path partitioned by dt and ts.
        Example: /volume_name/table/dt=20240620/ts=1330
        """
        try:
            self.logger.debug(f"📂 Building volume path for table `{table}` using watermark: {current_watermark}")
            if isinstance(current_watermark, str):
                dt = datetime.strptime(current_watermark, "%Y-%m-%d %H:%M:%S")
            else:
                dt = current_watermark

            date_str = dt.strftime("%Y%m%d")
            time_str = dt.strftime("%H%M")

            path = f"{volume_name}/{table}/dt={date_str}/ts={time_str}"
            self.logger.info(f"📁 Computed volume path: {path}")
            return path
        except Exception as e:
            self.logger.error(f"❌ Failed to compute volume path for `{table}`: {e}")
            raise
