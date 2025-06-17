from delta.tables import DeltaTable
from pyspark.sql import Row

def update_watermark(self, table: str, current_time, application_name: str):
    env_config = Constants.ENVIRONMENTS[self.environment]
    metadata_catalog = env_config["METADATA_CATALOG"]
    watermark_table_name = f"{metadata_catalog}.{Constants.METADATA_SCHEMA}.watermark_{application_name}"

    # Prepare DataFrame with one row to upsert
    update_df = self.spark.createDataFrame([
        Row(table_name=table, last_updated=current_time)
    ])

    try:
        delta_table = DeltaTable.forName(self.spark, watermark_table_name)
    except Exception:
        # If table doesn't exist, create it first (handle as needed)
        self.logger.error(f"Watermark table {watermark_table_name} not found.")
        raise

    # Merge update_df into watermark table
    delta_table.alias("wm").merge(
        update_df.alias("src"),
        "wm.table_name = src.table_name"
    ).whenMatchedUpdate(
        set={"last_updated": "src.last_updated"}
    ).whenNotMatchedInsert(
        values={"table_name": "src.table_name", "last_updated": "src.last_updated"}
    ).execute()

    self.logger.info(f"Watermark updated via DeltaTable API for `{table}` at `{current_time}`")
