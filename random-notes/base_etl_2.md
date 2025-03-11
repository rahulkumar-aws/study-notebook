### **📌 Final Updated `BaseETL.py` (Without Table Comments)**
Since table **comments are not needed**, I have removed them while keeping everything **fully functional**.

---

### **🔹 Full Updated `BaseETL.py`**
```python
import argparse
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from crba_etl.utils.logger import setup_logging
from crba_etl.config_parser import ConfigParser

class BaseETL:
    def __init__(self, db_type, env="staging", config_path=None):
        """
        Initialize ETL framework with:
        - `db_type`: Required database type (mssql, oracle, etc.).
        - `env`: Defines whether the job runs in `prod` or `staging` (default is `staging`).
        - `config_path`: Optional path to a custom config file.
        """
        self.db_type = db_type
        self.env = env.lower()  # ✅ Convert to lowercase for consistency
        self.logger = setup_logging()

        # ✅ Load config dynamically
        config_parser = ConfigParser(db_type=db_type, env=self.env, config_path=config_path)
        self.config = config_parser.get_config()

        # ✅ Extract database details
        self.database = self.config["data_source"]["attributes"]["database"]
        self.schema = self.config["data_source"]["attributes"]["schema"]

        # ✅ Extract all attributes (may contain optional authentication params)
        self.attributes = self.config["data_source"].get("attributes", {})

        # ✅ Extract JDBC driver (always required)
        self.jdbc_driver = self.attributes.get("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

        # ✅ Construct JDBC URL (Reused in all ETL jobs)
        self.jdbc_url = self.get_jdbc_url()

        # ✅ Define metadata table based on `--env`
        self.metadata_table = f"dasp_system.na_etl_dev.{'prod_metadata' if self.env == 'prod' else 'staging_metadata'}"
        self.logger.info(f"📜 Using metadata table: `{self.metadata_table}`")

        # ✅ Ensure metadata table exists before any ETL run
        self.setup_metadata_table()

    def get_jdbc_url(self):
        """Constructs JDBC URL from config."""
        user = self.attributes["user"]
        password = self.attributes["password"]
        host = self.attributes["host"]
        port = self.attributes["port"]

        return f"jdbc:sqlserver://{host}:{port};databaseName={self.database};user={user};password={password};encrypt=true;trustServerCertificate=true"

    def setup_metadata_table(self):
        """Ensures that the metadata table exists in Unity Catalog with the correct schema."""
        self.logger.info(f"🔍 Checking if metadata table `{self.metadata_table}` exists...")

        query = f"SHOW TABLES LIKE '{self.metadata_table}'"
        table_exists = self.spark.sql(query).count() > 0

        if not table_exists:
            self.logger.info(f"📦 Creating metadata table `{self.metadata_table}`...")

            create_query = f"""
                CREATE TABLE IF NOT EXISTS {self.metadata_table} (
                    table_name STRING,
                    database_name STRING,
                    schema_name STRING,
                    host STRING,
                    port STRING,
                    user STRING,
                    etl_start_time TIMESTAMP,
                    etl_end_time TIMESTAMP,
                    row_count LONG,
                    status STRING,
                    job_id STRING,
                    execution_time DOUBLE
                ) USING delta;
            """

            self.spark.sql(create_query)
            self.logger.info(f"✅ Metadata table `{self.metadata_table}` created successfully.")
        else:
            self.logger.info(f"✅ Metadata table `{self.metadata_table}` already exists.")

    def save_metadata(self, table_name, etl_start_time, row_count, status):
        """Saves metadata for the ETL job in Unity Catalog."""
        etl_end_time = datetime.now()
        execution_time = (etl_end_time - etl_start_time).total_seconds()
        job_id = f"{table_name}_{etl_start_time.strftime('%Y%m%d%H%M%S')}"  # ✅ Unique job ID

        metadata_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("database_name", StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("host", StringType(), True),
            StructField("port", StringType(), True),
            StructField("user", StringType(), True),
            StructField("etl_start_time", TimestampType(), True),
            StructField("etl_end_time", TimestampType(), True),
            StructField("row_count", LongType(), True),
            StructField("status", StringType(), True),
            StructField("job_id", StringType(), True),
            StructField("execution_time", DoubleType(), True),
        ])

        metadata_data = [(table_name, self.database, self.schema, self.attributes.get("host"), 
                          str(self.attributes.get("port")), self.attributes.get("user"), 
                          etl_start_time, etl_end_time, row_count, status, job_id, execution_time)]

        metadata_df = self.spark.createDataFrame(metadata_data, metadata_schema)

        metadata_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(self.metadata_table)

        self.logger.info(
            f"📊 Metadata updated for `{table_name}` in `{self.metadata_table}`: {row_count} rows, Status: {status}, Job ID: {job_id}"
        )

    def write_to_uc(self, df, table_name):
        """
        Writes DataFrame to Unity Catalog in Delta format.
        - Uses `overwrite` (default) or `append` based on config.
        """
        target_catalog = self.config["unity_catalog"]["target_catalog"]
        target_schema = self.config["unity_catalog"]["target_schema"]
        target_format = self.config["unity_catalog"].get("target_format", "delta")

        append_tables = set(self.config["unity_catalog"].get("append_tables", []))

        mode = "overwrite"  # ✅ Default is overwrite
        if table_name in append_tables:
            mode = "append"

        full_table_name = f"{target_catalog}.{target_schema}.{table_name}"

        self.logger.info(f"📤 Writing data to `{full_table_name}` in `{target_format}` format with mode `{mode}`...")

        try:
            if mode == "overwrite":
                df.write.format(target_format).mode("overwrite").saveAsTable(full_table_name)
            elif mode == "append":
                df.write.format(target_format).mode("append").saveAsTable(full_table_name)
            else:
                raise ValueError(f"❌ Invalid write mode `{mode}` provided.")
            self.logger.info(f"✅ Successfully wrote to `{full_table_name}`.")
        except Exception as e:
            self.logger.error(f"❌ Failed to write to `{full_table_name}`: {str(e)}")
            raise

```

---

## **📌 What’s Changed**
| **Change** | **Updated?** |
|-----------|-------------|
| **Removed table comments** | ✅ Done |
| **Ensures metadata table exists before ETL starts** | ✅ Done |
| **Auto-selects `prod_metadata` or `staging_metadata` based on `--env`** | ✅ Done |
| **Prevents crashes if metadata table doesn’t exist** | ✅ Done |

---

## **📌 Example Behavior**
### ✅ **1️⃣ Running in Production (`prod_metadata` Doesn't Exist)**
```bash
databricks bundle run etl_runner_job --env prod
```
✔ **Creates `prod_metadata` table before first ETL run.**  

---

### ✅ **2️⃣ Running in Staging (`staging_metadata` Exists)**
```bash
databricks bundle run etl_runner_job
```
✔ **Checks and confirms `staging_metadata` exists, no re-creation.**  

---

## **📌 Final Steps: Deploy & Run in Databricks**
```bash
databricks bundle deploy
databricks bundle run etl_runner_job --db mssql --env prod
```

---

## **🚀 Done! Now Metadata Table is Created Without Unnecessary Comments.**
Let me know if you need refinements! 🔥🚀
