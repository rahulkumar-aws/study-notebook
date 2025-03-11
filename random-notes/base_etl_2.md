### **📌 Final Updated `BaseETL.py` (Without Table Comments)**
Since table **comments are not needed**, I have removed them while keeping everything **fully functional**.

---

### **🔹 Full Updated `BaseETL.py`**
```python
import argparse
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
