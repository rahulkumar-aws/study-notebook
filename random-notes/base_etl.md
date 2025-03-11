### **📌 Fix: Ensure `include_tables` and `exclude_tables` Logic in ETL**
We need to ensure that:
1. ✅ **If `include_tables` is specified**, only those tables are loaded.
2. ✅ **If `include_tables` is empty or not present**, load all tables from the schema.
3. ✅ **If `exclude_tables` is specified**, exclude those tables while loading all others.
4. ✅ **If both `include_tables` and `exclude_tables` are missing, load all tables.**

---

### **1️⃣ Update `BaseETL.py` to Handle `include_tables` and `exclude_tables`**
Modify `BaseETL.py` to **check the schema and fetch tables dynamically**.

#### ✅ **Updated `BaseETL.py`**
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

        # ✅ Get schema details from config
        self.database = self.config["data_source"]["attributes"]["database"]
        self.schema = self.config["data_source"]["attributes"]["schema"]

        # ✅ Extract table list based on include/exclude logic
        self.include_tables = self.config.get("include_tables", [])
        self.exclude_tables = self.config.get("exclude_tables", [])

        self.tables = self.get_tables_from_schema()
        self.logger.info(f"📂 Final table list for `{self.schema}`: {self.tables}")

    def get_tables_from_schema(self):
        """Fetches table list from the schema, considering `include_tables` and `exclude_tables` rules."""
        # ✅ If `include_tables` is present and not empty, use it
        if self.include_tables:
            self.logger.info(f"✅ Using `include_tables`: {self.include_tables}")
            return self.include_tables

        # ✅ Otherwise, fetch all tables from the schema
        query = f"""
            SELECT table_name FROM {self.database}.INFORMATION_SCHEMA.TABLES 
            WHERE table_schema = '{self.schema}'
        """
        all_tables = [row.table_name for row in self.spark.sql(query).collect()]

        # ✅ If `exclude_tables` is present, remove those tables
        if self.exclude_tables:
            self.logger.info(f"❌ Excluding tables: {self.exclude_tables}")
            all_tables = [table for table in all_tables if table not in self.exclude_tables]

        return all_tables
```

---

### **2️⃣ Update `MSSQLToUCETL.py` to Use New Table Fetching Logic**
Modify `MSSQLToUCETL` to **call `BaseETL.get_tables_from_schema()` instead of relying on hardcoded values**.

#### ✅ **Updated `mssql_etl.py`**
```python
from crba_etl.base_etl import BaseETL

class MSSQLToUCETL(BaseETL):
    def __init__(self, db_type, config_path=None):
        """Initialize MSSQL ETL with `db_type` and `config_path`."""
        super().__init__(db_type=db_type, config_path=config_path)  # ✅ Correctly calls BaseETL's constructor

    def run_etl(self, table_name):
        """Runs ETL for a single table."""
        self.logger.info(f"🚀 Extracting data from `{table_name}` in `{self.db_type}`")

        df = self.read_from_mssql(table_name)  # Assume this reads MSSQL data

        row_count = df.count()
        self.logger.info(f"📊 Loaded `{row_count}` records from `{table_name}`")

        self.write_to_uc(df, table_name)
        self.logger.info(f"✅ Successfully loaded `{table_name}` into Unity Catalog\n")
```

---

### **3️⃣ Verify `config.yml` to Ensure It Works as Expected**
Make sure **your YAML file includes `include_tables` and `exclude_tables`** properly.

#### ✅ **Example `config.yml`**
```yaml
data_source:
  attributes:
    database: "sg_CRBA"
    schema: "sch_crba"
    user: "svc_flyway"
    password: "Kms$742_#aGaiS#i"

unity_catalog:
  target_catalog: "crba_workbench"
  target_schema: "staging_crba"

include_tables:  # ✅ Only these tables will be loaded
  - employees
  - payroll

exclude_tables:  # ✅ If `include_tables` is empty, exclude these instead
  - logs
  - temp_data
```

---

### **4️⃣ Run in Databricks Assets Bundle**
Now, **deploy and run the updated ETL job in Databricks**.

#### ✅ **Deploy the Fix**
```bash
databricks bundle deploy
```
#### ✅ **Run the Job**
```bash
databricks bundle run etl_runner_job
```

---

### **📌 Expected Behavior After Fix**
| **Scenario** | **Expected Behavior** |
|-------------|---------------------|
| `include_tables` is specified | ✅ Load only the tables in `include_tables` |
| `include_tables` is empty or missing | ✅ Load all tables from schema |
| `exclude_tables` is specified | ✅ Exclude listed tables while loading all others |
| Both `include_tables` and `exclude_tables` are missing | ✅ Load all tables in schema |

---

### **🚀 Now Your ETL Handles `include_tables` and `exclude_tables` Correctly!**
Let me know if you need further refinements! 🔥🚀
