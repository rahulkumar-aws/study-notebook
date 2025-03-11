Here’s the **final `write_to_uc()`** function that:
- ✅ Uses **`merge_tables` and `append_tables`** from YAML.
- ✅ Defaults to **`overwrite`** if a table is **not listed**.
- ✅ Handles cases where **`merge_tables` or `append_tables` are missing** from the YAML.

---

## **📌 Final `write_to_uc()`**
```python
from delta.tables import DeltaTable

def write_to_uc(self, df, table_name):
    """
    Writes DataFrame to Unity Catalog in Delta format with different modes:
    - Overwrite (default): Replaces the entire table
    - Append: Adds new rows to the table
    - Merge: Upserts data using a primary key (incremental load)

    Parameters:
    - df (DataFrame): The DataFrame to be written.
    - table_name (str): The name of the target table in Unity Catalog.
    """
    target_catalog = self.config["unity_catalog"]["target_catalog"]
    target_schema = self.config["unity_catalog"]["target_schema"]
    target_format = self.config["unity_catalog"].get("target_format", "delta")  # Default format is Delta

    merge_tables = set(self.config["unity_catalog"].get("merge_tables", []))  # Can be missing
    append_tables = set(self.config["unity_catalog"].get("append_tables", []))  # Can be missing

    mode = "overwrite"  # ✅ Default is overwrite
    primary_key = self.config["unity_catalog"].get("primary_key")  # Used for merge

    full_table_name = f"{target_catalog}.{target_schema}.{table_name}"

    # ✅ Determine mode based on configuration
    if table_name in merge_tables:
        mode = "merge"
    elif table_name in append_tables:
        mode = "append"

    self.logger.info(f"📤 Writing data to `{full_table_name}` in `{target_format}` format with mode `{mode}`...")

    try:
        if mode == "overwrite":
            (df.write
               .format(target_format)
               .mode("overwrite")
               .saveAsTable(full_table_name))
            self.logger.info(f"✅ Overwritten `{full_table_name}` successfully.")

        elif mode == "append":
            (df.write
               .format(target_format)
               .mode("append")
               .saveAsTable(full_table_name))
            self.logger.info(f"✅ Appended data to `{full_table_name}` successfully.")

        elif mode == "merge
