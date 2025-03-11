You're absolutely right! **The correct logic should be:**  
1. ✅ **If `include_tables` is specified**, use only those tables.  
2. ✅ **If `include_tables` is empty or missing, fetch all tables from the schema** and apply `exclude_tables`.  
3. ✅ **Exclude tables listed in `exclude_tables`** before processing.  

---

## **✅ Final Updated `MSSQLToUCETL.py` (Correct Include & Exclude Logic)**
```python
from datetime import datetime
from crba_etl.base_etl import BaseETL

class MSSQLToUCETL(BaseETL):
    def __init__(self, db_type, env="staging", config_path=None):
        """Initialize MSSQL ETL with `db_type`, `env`, and `config_path`."""
        super().__init__(db_type=db_type, env=env, config_path=config_path)  # ✅ Call BaseETL constructor

        # ✅ Extract `include_tables` and `exclude_tables` from config
        self.include_tables = set(self.config.get("include_tables", []))
        self.exclude_tables = set(self.config.get("exclude_tables", []))

    def get_tables_from_schema(self):
        """Fetches tables dynamically, giving precedence to `include_tables` if provided."""
        if self.include_tables:
            self.logger.info(f"✅ Using `include_tables`: {self.include_tables}")
            return list(self.include_tables)  # ✅ Use only tables in `include_tables`

        self.logger.info(f"🔍 Fetching tables from schema `{self.schema}` via JDBC...")

        query = f"(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.schema}') AS table_list"
        df_tables = self.spark.read.jdbc(url=self.jdbc_url, table=query, properties={"driver": self.jdbc_driver})

        all_tables = [row["TABLE_NAME"] for row in df_tables.collect()]
        self.logger.info(f"📋 Tables found in schema `{self.schema}`: {all_tables}")

        # ✅ Apply `exclude_tables`
        if self.exclude_tables:
            all_tables = [table for table in all_tables if table not in self.exclude_tables]
            self.logger.info(f"❌ Excluding tables: {self.exclude_tables}")

        self.logger.info(f"📂 Final table list after filtering: {all_tables}")
        return all_tables

    def read_from_mssql(self, table_name, partition_column=None, num_partitions=10, filters=None):
        """Reads a table from MSSQL into a PySpark DataFrame using optimized JDBC."""
        self.logger.info(f"🔗 Connecting to MSSQL `{self.database}` to read table `{table_name}`...")

        full_table_name = f"{self.schema}.{table_name}"

        # ✅ Dynamically filter out empty JDBC properties
        jdbc_properties = {k: v for k, v in self.attributes.items() if v}
        jdbc_properties["driver"] = self.jdbc_driver
        jdbc_properties["fetchsize"] = "10000"
        jdbc_properties["pushDownPredicate"] = "true"

        self.logger.info(f"📜 Using JDBC properties: {jdbc_properties}")

        query = f"(SELECT * FROM {full_table_name} " + (f"WHERE {filters}" if filters else "") + ") AS table_query"

        if partition_column:
            self.logger.info(f"⚡ Using partition column `{partition_column}` for parallel read.")

            min_max_query = f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {full_table_name}) AS min_max"
            min_max_df = self.spark.read.jdbc(url=self.jdbc_url, table=min_max_query, properties=jdbc_properties)

            if min_max_df.count() == 0:
                self.logger.warning(f"⚠️ No data found in `{full_table_name}`, returning empty DataFrame.")
                return self.spark.createDataFrame([], min_max_df.schema)

            min_val, max_val = min_max_df.collect()[0]["min_val"], min_max_df.collect()[0]["max_val"]

            self.logger.info(f"🔢 Partitioning `{full_table_name}` from `{min_val}` to `{max_val}` across `{num_partitions}` partitions.")

            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=query,
                column=partition_column,
                lowerBound=min_val,
                upperBound=max_val,
                numPartitions=num_partitions,
                properties=jdbc_properties
            )

        else:
            self.logger.info(f"⚡ No partition column provided. Using chunked JDBC read for `{full_table_name}`.")
            chunk_size = 1000000
            df_list = []

            offset = 0
            while True:
                chunk_query = f"(SELECT * FROM {full_table_name} " + (f"WHERE {filters}" if filters else "") + f" ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY) AS chunk_query"
                chunk_df = self.spark.read.jdbc(url=self.jdbc_url, table=chunk_query, properties=jdbc_properties)

                if chunk_df.count() == 0:
                    break

                df_list.append(chunk_df)
                offset += chunk_size

            df = df_list[0] if len(df_list) == 1 else df_list[0].unionAll(*df_list[1:])

        self.logger.info(f"✅ Successfully loaded `{full_table_name}` from `{self.database}`.")
        return df

    def run_etl(self, table_name, partition_column=None):
        """Runs the ETL process for a given table and updates metadata."""
        self.logger.info(f"🚀 Starting ETL for `{table_name}`...")

        etl_start_time = datetime.now()

        try:
            df = self.read_from_mssql(table_name, partition_column=partition_column)

            if df.count() == 0:
                self.logger.warning(f"⚠️ No data found for `{table_name}`, skipping write step.")
                self.save_metadata(table_name, etl_start_time, 0, "No Data")
                return

            self.write_to_uc(df, table_name)
            self.save_metadata(table_name, etl_start_time, df.count(), "Success")

            self.logger.info(f"✅ ETL completed for `{table_name}`.")

        except Exception as e:
            self.logger.error(f"❌ ETL failed for `{table_name}`: {str(e)}")
            self.save_metadata(table_name, etl_start_time, 0, "Failure")
            raise
```

---

## **📌 What’s Fixed**
| **Feature** | **Implemented?** |
|------------|----------------|
| ✅ **Includes correct `include_tables` logic** | Uses only `include_tables` if specified, otherwise fetches from schema |
| ✅ **Excludes unwanted tables** | Filters out tables in `exclude_tables` |
| ✅ **Keeps metadata logging intact** | Logs **Success, Failure, or No Data** |
| ✅ **No unnecessary changes** | Only restores **correct include/exclude logic** |

---

## **📌 Example Behavior**
### ✅ **1️⃣ Include Tables Take Precedence**
```yaml
include_tables:
  - employees
  - payroll
```
✔ **Only `employees` and `payroll` are processed**.  

---

### ✅ **2️⃣ If `include_tables` is Empty, Fetch All & Apply `exclude_tables`**
```yaml
exclude_tables:
  - logs
  - temp_data
```
✔ **Processes all tables except `logs` and `temp_data`**.  

---

### ✅ **3️⃣ Standard ETL Run (With Partitioning)**
```python
etl.run_etl("employees", partition_column="id")
```
✔ **Uses partitioned JDBC read for parallelism.**  

---

### ✅ **4️⃣ Optimized ETL Run (Without Partitioning)**
```python
etl.run_etl("employees")
```
✔ **Uses chunking (`LIMIT & OFFSET`) to optimize performance.**  

---

## **📌 Final Steps: Deploy & Run in Databricks**
```bash
databricks bundle deploy
databricks bundle run etl_runner_job --db mssql --env prod
```

---

## **🚀 Done! Now `MSSQLToUCETL` Uses `include_tables` & `exclude_tables` Correctly.**
Let me know if you need refinements! 🔥🚀
