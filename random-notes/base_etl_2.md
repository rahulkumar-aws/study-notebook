Sure! Below is the **full and corrected `BaseETL.py`** that dynamically fetches table information from **`INFORMATION_SCHEMA.TABLES` using JDBC**, applies **include/exclude table filtering**, and initializes the ETL framework properly.

---

## **📌 Full `BaseETL.py` Code**
```python
import yaml
import logging
from pyspark.sql import SparkSession
from crba_etl.utils.logger import setup_logging
from crba_etl.config_parser import ConfigParser

logger = logging.getLogger(__name__)

class BaseETL:
    def __init__(self, db_type, config_path=None):
        """
        Initialize ETL framework with:
        - `db_type`: Required database type (mssql, oracle, etc.).
        - `config_path`: Optional path to a custom config file.
        """
        self.db_type = db_type
        self.logger = setup_logging()

        # ✅ Load config dynamically based on `--db`
        config_parser = ConfigParser(db_type=db_type, config_path=config_path)
        self.config = config_parser.get_config()

        # ✅ Log which database is being used
        self.logger.info(f"📦 Using database: `{db_type}`")
        self.logger.info(f"📜 Loaded configuration: {self.config}")

        # ✅ Initialize Spark Session
        self.spark = SparkSession.builder.appName(f"{db_type.upper()}_ETL").enableHiveSupport().getOrCreate()
        self.logger.info(f"🔄 ETL Initialized for `{db_type.upper()}`")

        # ✅ Extract database details from config
        self.database = self.config["data_source"]["attributes"]["database"]
        self.schema = self.config["data_source"]["attributes"]["schema"]
        self.jdbc_url = self.get_jdbc_url()

        # ✅ Extract table list based on include/exclude logic
        self.include_tables = self.config.get("include_tables", [])
        self.exclude_tables = self.config.get("exclude_tables", [])

        self.tables = self.get_tables_from_schema()
        self.logger.info(f"📂 Final table list for `{self.schema}`: {self.tables}")

    def get_jdbc_url(self):
        """Constructs JDBC URL from config."""
        user = self.config["data_source"]["attributes"]["user"]
        password = self.config["data_source"]["attributes"]["password"]
        host = self.config["data_source"]["attributes"]["host"]
        port = self.config["data_source"]["attributes"]["port"]
        driver = self.config["data_source"]["attributes"]["driver"]

        return f"jdbc:sqlserver://{host}:{port};databaseName={self.database};user={user};password={password};encrypt=true;trustServerCertificate=true"

    def get_tables_from_schema(self):
        """Fetches table list from `INFORMATION_SCHEMA.TABLES` using JDBC."""
        self.logger.info(f"🔍 Fetching tables from schema `{self.schema}` via JDBC...")

        # ✅ Query to fetch tables from the schema
        list_tables_query = f"""
            (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{self.schema}' 
            AND TABLE_TYPE = 'BASE TABLE') AS table_list
        """

        # ✅ Read table list using JDBC
        df_tables = self.spark.read.jdbc(url=self.jdbc_url, table=list_tables_query, properties={"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"})
        
        # ✅ Convert DataFrame to Python list
        all_tables = [row["TABLE_NAME"] for row in df_tables.collect()]
        self.logger.info(f"📋 Tables found in schema `{self.schema}`: {all_tables}")

        # ✅ Apply include/exclude table logic
        if self.include_tables:
            self.logger.info(f"✅ Using `include_tables`: {self.include_tables}")
            return [table for table in all_tables if table in self.include_tables]

        if self.exclude_tables:
            self.logger.info(f"❌ Excluding tables: {self.exclude_tables}")
            return [table for table in all_tables if table not in self.exclude_tables]

        return all_tables  # ✅ If no filters, return all tables

    def setup_metadata_table(self):
        """Creates the Delta metadata table if it doesn't exist."""
        metadata_table = "dasp_system.na_etl_dev.etl_metadata"

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {metadata_table} (
                db_type STRING,
                table_name STRING,
                etl_start_time TIMESTAMP,
                etl_end_time TIMESTAMP,
                row_count LONG,
                status STRING
            ) USING delta;
        """)
        self.logger.info(f"✅ Metadata table `{metadata_table}` is ready.")

    def save_metadata(self, table_name, start_time, row_count, status):
        """Save metadata after processing a table."""
        from datetime import datetime
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

        end_time = datetime.now()

        metadata_schema = StructType([
            StructField("db_type", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("etl_start_time", TimestampType(), True),
            StructField("etl_end_time", TimestampType(), True),
            StructField("row_count", LongType(), True),
            StructField("status", StringType(), True),
        ])

        metadata_data = [(self.db_type, table_name, start_time, end_time, row_count, status)]
        metadata_df = self.spark.createDataFrame(metadata_data, metadata_schema)

        metadata_table = "dasp_system.na_etl_dev.etl_metadata"
        metadata_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(metadata_table)

        self.logger.info(
            f"📊 Metadata saved for `{table_name}` in `{self.db_type}`: {row_count} rows, Status: {status}"
        )
```

---

## **📌 Key Fixes and Improvements**
| **Issue** | **Fix Applied** |
|-----------|---------------|
| **Was hardcoding table retrieval** | ✅ Now dynamically fetches tables via **JDBC query** from `INFORMATION_SCHEMA.TABLES` |
| **Did not filter tables properly** | ✅ Applies **`include_tables` and `exclude_tables`** logic correctly |
| **Did not fetch schema dynamically** | ✅ Uses `self.schema` from the YAML config to fetch only the tables in the **correct schema** |
| **Did not use proper JDBC URL** | ✅ Constructs **correct JDBC URL** using **host, port, user, password** |
| **No proper logging** | ✅ Logs **database, schema, table list, and filtering decisions** |
| **Did not create metadata table** | ✅ Ensures **metadata table exists before writing logs** |
| **Did not save ETL metadata** | ✅ Saves **table name, start time, row count, and status** to Delta |

---

## **📌 How It Works Now**
### ✅ **1️⃣ Extracts Tables Dynamically from the Schema**
- Uses **`INFORMATION_SCHEMA.TABLES`** via **JDBC query**.
- Filters tables based on **schema** from YAML.

### ✅ **2️⃣ Applies `include_tables` and `exclude_tables` Correctly**
- If **`include_tables`** is specified, it **loads only those tables**.
- If **`exclude_tables`** is present, it **removes those tables from the list**.
- If **both are missing**, it **loads all tables** in the schema.

### ✅ **3️⃣ Logs Metadata and Saves ETL Execution Details**
- Logs **which tables are included/excluded**.
- Ensures **metadata table is created**.
- Saves **table processing stats (row count, start time, status, etc.)**.

---

## **📌 Final Steps: Deploy and Run the Job**
### **1️⃣ Deploy the Fix**
```bash
databricks bundle deploy
```
### **2️⃣ Run the Job**
```bash
databricks bundle run etl_runner_job
```

---

## **🚀 Now Your ETL Dynamically Fetches Tables via JDBC and Handles Filtering Correctly!**
Let me know if you need further refinements! 🔥🚀
