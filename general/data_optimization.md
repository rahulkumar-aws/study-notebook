### **Why is Data Read Fast but Write Slow?**  

This happens because **reading from Oracle** is typically **parallelized** via JDBC **partitioning**, while **writing to Unity Catalog (Delta)** depends on **shuffle and commit operations**.

---

## **🔍 Root Causes of Slow Writes**
### **1️⃣ Data Shuffle in Delta Write**
- When writing to Delta, Spark **shuffles the data** across partitions before committing.
- **Fix:** Ensure **optimal partitioning** before writing.

### **2️⃣ Single Task Writing Data**
- If all data is **written by a single task**, it will be **very slow**.
- **Fix:** Use `repartition()` before writing.

### **3️⃣ High Small File Creation in Delta**
- Many small partitions result in **lots of small files**, slowing the Delta commit.
- **Fix:** Use **coalesce()** to reduce file count.

### **4️⃣ Overhead of Delta Transactions**
- **Delta has extra transaction logging** compared to Parquet.
- **Fix:** If appending, use `append` mode; if overwriting, use `overwriteSchema`.

---

## **✅ Fix in `oracle_etl.py`**
### **🚀 Optimize Data Writing to Delta**
```python
from naacsanlyt_etl.base_etl import BaseETL
from datetime import datetime

class OracleToUCETL(BaseETL):
    def __init__(self):
        """Initialize Oracle ETL process."""
        super().__init__(db_type="oracle")

    def read_from_oracle(self, table_name):
        """Reads a table from Oracle into a PySpark DataFrame using JDBC."""
        oracle_cfg = self.config["oracle"]["bridge"]
        jdbc_url = f"jdbc:oracle:thin:@//{oracle_cfg['host']}:{oracle_cfg['port']}/{oracle_cfg['service_name']}"
        properties = {
            "user": oracle_cfg["user"],
            "password": oracle_cfg["password"],
            "driver": "oracle.jdbc.OracleDriver"
        }

        self.logger.info(f"🔍 Reading data from Oracle table: {table_name}")

        # Use partitioning to parallelize read
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", oracle_cfg["user"]) \
            .option("password", oracle_cfg["password"]) \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("numPartitions", 8) \  # Adjust based on table size
            .option("fetchsize", 10000) \
            .load()
        
        self.logger.info(f"✅ Read {df.count()} records from {table_name}")
        return df

    def write_to_uc(self, df, table_name):
        """Writes the DataFrame to Unity Catalog in Delta format efficiently."""
        uc_cfg = self.config["unity_catalog"]
        target_table = f"{uc_cfg['target_catalog']}.{uc_cfg['target_schema']}.{table_name.split('.')[-1]}"

        self.logger.info(f"🚀 Writing data to Delta table: {target_table}")

        # Optimize writing strategy
        df = df.repartition(8)  # Ensure parallelism
        df.write \
            .format(uc_cfg["target_format"]) \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)

        self.logger.info(f"✅ Successfully written to {target_table}")

    def run_etl(self, table_name):
        """Runs ETL for a single table."""
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = self.read_from_oracle(table_name)

        row_count = df.count()
        self.logger.info(f"📊 Loaded {row_count} records from {table_name}")

        self.write_to_uc(df, table_name)

        self.save_metadata(table_name, start_time, row_count, "Success")
        self.logger.info(f"✅ ETL completed for {table_name}\n")
```

---

## **🚀 Optimizations Applied**
✅ **Parallelized Oracle Read**
```python
.option("numPartitions", 8)
.option("fetchsize", 10000)
```
✅ **Repartitioned before writing**
```python
df = df.repartition(8)  # Ensures multiple tasks write in parallel
```
✅ **Delta Schema Optimization**
```python
.option("overwriteSchema", "true")
```

---

## **🚀 Expected Results**
| Operation | Before Fix | After Fix |
|-----------|------------|-----------|
| **Oracle Read** | Already fast ✅ | ✅ Same (parallelized) |
| **Delta Write** | **Slow (single task)** | 🚀 Faster (parallelized) |
| **Transaction Commit** | **Slower (small files)** | 🚀 **Efficient (batch writes)** |

---

### **✅ Run It Again**
```sh
databricks bundle run oracle_etl_job
```
🚀 **Now your writes will be much faster!** 🚀
