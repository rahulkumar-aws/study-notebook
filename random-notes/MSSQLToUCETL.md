You're absolutely right! **If no partition column is provided, the query should still be optimized** to improve **JDBC read performance**. Here’s how we will:
- ✅ **Use chunking (`LIMIT & OFFSET`) for large tables**.
- ✅ **Push filtering to the database (`pushDownPredicate`)**.
- ✅ **Increase fetch size for better performance (`fetchsize=10000`)**.

---

## **✅ Updated `MSSQLToUCETL.py` (Optimized for Non-Partitioned Tables)**
```python
from datetime import datetime
from crba_etl.base_etl import BaseETL

class MSSQLToUCETL(BaseETL):
    def __init__(self, db_type, env="staging", config_path=None):
        """Initialize MSSQL ETL with `db_type`, `env`, and `config_path`."""
        super().__init__(db_type=db_type, env=env, config_path=config_path)  # ✅ Call BaseETL constructor

    def read_from_mssql(self, table_name, partition_column=None, num_partitions=10, filters=None):
        """Reads a table from MSSQL into a PySpark DataFrame using optimized JDBC."""
        self.logger.info(f"🔗 Connecting to MSSQL `{self.database}` to read table `{table_name}`...")

        # ✅ Ensure table name includes schema
        full_table_name = f"{self.schema}.{table_name}"

        # ✅ Dynamically filter out empty JDBC properties from `attributes`
        jdbc_properties = {k: v for k, v in self.attributes.items() if v}  # ✅ Removes empty properties
        jdbc_properties["driver"] = self.jdbc_driver  # ✅ Always add driver
        jdbc_properties["fetchsize"] = "10000"  # ✅ Increase fetch size for faster reads
        jdbc_properties["pushDownPredicate"] = "true"  # ✅ Enables filter pushdown to database

        self.logger.info(f"📜 Using JDBC properties: {jdbc_properties}")

        # ✅ Apply filtering logic (if provided)
        query = f"(SELECT * FROM {full_table_name} " + (f"WHERE {filters}" if filters else "") + ") AS table_query"

        if partition_column:
            self.logger.info(f"⚡ Using partition column `{partition_column}` for parallel read.")

            # ✅ Fetch min/max values for partitioning
            min_max_query = f"(SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {full_table_name}) AS min_max"
            min_max_df = self.spark.read.jdbc(url=self.jdbc_url, table=min_max_query, properties=jdbc_properties)

            if min_max_df.count() == 0:
                self.logger.warning(f"⚠️ No data found in `{full_table_name}`, returning empty DataFrame.")
                return self.spark.createDataFrame([], min_max_df.schema)  # ✅ Return empty DataFrame if no data

            min_val, max_val = min_max_df.collect()[0]["min_val"], min_max_df.collect()[0]["max_val"]

            self.logger.info(f"🔢 Partitioning `{full_table_name}` from `{min_val}` to `{max_val}` across `{num_partitions}` partitions.")

            # ✅ Use parallel JDBC read with partitioning
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
            # ✅ If no partition column, use chunking with LIMIT & OFFSET for faster reads
            self.logger.info(f"⚡ No partition column provided. Using chunked JDBC read for `{full_table_name}`.")
            chunk_size = 1000000  # ✅ Set chunk size (adjust as needed)
            df_list = []

            offset = 0
            while True:
                chunk_query = f"(SELECT * FROM {full_table_name} " + (f"WHERE {filters}" if filters else "") + f" ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY) AS chunk_query"
                chunk_df = self.spark.read.jdbc(url=self.jdbc_url, table=chunk_query, properties=jdbc_properties)

                if chunk_df.count() == 0:
                    break  # ✅ Stop if no more data

                df_list.append(chunk_df)
                offset += chunk_size

            df = df_list[0] if len(df_list) == 1 else df_list[0].unionAll(*df_list[1:])

        self.logger.info(f"✅ Successfully loaded `{full_table_name}` from `{self.database}`.")
        return df

    def run_etl(self, table_name, partition_column=None):
        """Runs the ETL process for a given table and updates metadata."""
        self.logger.info(f"🚀 Starting ETL for `{table_name}`...")

        etl_start_time = datetime.now()  # ✅ Start time

        try:
            # ✅ Step 1: Read data from MSSQL
            df = self.read_from_mssql(table_name, partition_column=partition_column)

            if df.count() == 0:
                self.logger.warning(f"⚠️ No data found for `{table_name}`, skipping write step.")
                self.save_metadata(table_name, etl_start_time, 0, "No Data")  # ✅ Update metadata
                return

            # ✅ Step 2: Write to Unity Catalog
            self.write_to_uc(df, table_name)

            # ✅ Step 3: Update metadata table
            self.save_metadata(table_name, etl_start_time, df.count(), "Success")  # ✅ Update metadata

            self.logger.info(f"✅ ETL completed for `{table_name}`.")

        except Exception as e:
            self.logger.error(f"❌ ETL failed for `{table_name}`: {str(e)}")
            self.save_metadata(table_name, etl_start_time, 0, "Failure")  # ✅ Update metadata
            raise
```

---

## **📌 Optimizations for Non-Partitioned Reads**
| **Optimization** | **How It Helps** | **When to Use?** |
|-----------------|------------------|-----------------|
| **Increase Fetch Size (`fetchsize=10000`)** | ✅ Reduces network overhead, fetches more rows per call | Always |
| **Enable Query Pushdown (`pushDownPredicate=true`)** | ✅ Pushes filtering to the database | When filtering data |
| **Use Chunking (`LIMIT & OFFSET`)** | ✅ Splits large tables into smaller chunks | When no partition column exists |

---

## **📌 Example Behavior**
### ✅ **1️⃣ Standard ETL Run (With Partitioning)**
```python
etl.run_etl("employees", partition_column="id")
```
✔ **Uses parallel JDBC read with partitioning.**  

---

### ✅ **2️⃣ Optimized ETL Run (Without Partitioning)**
```python
etl.run_etl("employees")
```
✔ **Reads in chunks of 1M rows using `LIMIT & OFFSET`.**  

---

## **📌 Final Steps: Deploy & Run in Databricks**
```bash
databricks bundle deploy
databricks bundle run etl_runner_job --db mssql --env prod
```

---

## **🚀 Done! Now JDBC Reads Are Optimized Even Without Partitioning.**
Let me know if you need refinements! 🔥🚀
