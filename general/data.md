Here is the **full working ETL framework** with the **masked `oracle.yml`** while keeping the rest of the code unchanged.

---

## **📂 Final Project Structure**
```
/src
│── naacsanlyt_etl/
│   │── __init__.py
│   │── base_etl.py
│   │── etl_runner.py
│   ├── etl_jobs/
│   │   │── __init__.py
│   │   │── oracle_etl.py
│   ├── config/
│   │   │── oracle.yml
```

---

## **✅ 1. `oracle.yml` (Fully Masked)**
```yaml
oracle:
  bridge:
    host: "xxxxx"
    port: "xxxxx"
    service_name: "xxxxx"
    user: "xxxxx"
    password: "xxxxx"
    tables:
      - name: "xxxxx"

unity_catalog:
  target_catalog: "xxxxx"
  target_schema: "xxxxx"
  target_format: "xxxxx"
```

---

## **✅ 2. `base_etl.py` (Loads Config and Prints It)**
```python
import os
import yaml
import logging
import importlib.resources as pkg_resources
import naacsanlyt_etl.config  # Import config resources

class BaseETL:
    def __init__(self, db_type="oracle"):
        """Initialize ETL framework and only load config."""
        self.db_type = db_type
        self.logger = self.setup_logging()  # Initialize logging first
        self.config = self.load_config(db_type)  # Load configuration

        # Print and log the loaded config
        self.logger.info(f"📜 Loaded configuration for {db_type}: {self.config}")
        print("✅ Config Loaded Successfully!")
        print(self.config)

    def setup_logging(self):
        """Setup logging to print messages to console and file."""
        log_file = "/dbfs/logs/config_test.log"
        os.makedirs("/dbfs/logs", exist_ok=True)

        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        return logging.getLogger(__name__)

    def load_config(self, db_type):
        """Loads the YAML configuration file from DBFS first, else falls back to packaged resources."""
        config_filename = f"{db_type}.yml"
        dbfs_path = f"/dbfs/configs/{config_filename}"  # External location

        if os.path.exists(dbfs_path):
            with open(dbfs_path, "r") as file:
                return yaml.safe_load(file)

        try:
            with pkg_resources.open_text(naacsanlyt_etl.config, config_filename) as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"❌ Configuration file '{config_filename}' not found in DBFS or package resources.")
```

---

## **✅ 3. `oracle_etl.py` (Calls `BaseETL` to Print Config)**
```python
from naacsanlyt_etl.base_etl import BaseETL

class OracleToUCETL(BaseETL):
    def __init__(self):
        """Initialize Oracle ETL process and only load config."""
        super().__init__(db_type="oracle")
```

---

## **✅ 4. `etl_runner.py` (Runs `OracleToUCETL` to Print Config)**
```python
import argparse
import logging
from naacsanlyt_etl.etl_jobs.oracle_etl import OracleToUCETL

class ETLRunner:
    def __init__(self, db_type="oracle"):
        """Initialize ETL Runner and just print config."""
        self.db_type = db_type
        self.logger = self.setup_logging()

        # Initialize Oracle ETL
        self.etl = OracleToUCETL()

        self.logger.info(f"🔄 ETL Runner initialized for {db_type.upper()}")

    def setup_logging(self):
        """Setup logging configuration."""
        log_file = "/dbfs/logs/etl_runner.log"
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        return logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Run ETL for specified database")
    parser.add_argument("--db", type=str, choices=["oracle", "mssql"], required=True, help="Database type (oracle/mssql)")

    args = parser.parse_args()
    runner = ETLRunner(db_type=args.db)

if __name__ == "__main__":
    main()
```

---

## **🚀 5. Running the Code**
### **1️⃣ Run Locally**
```sh
python src/naacsanlyt_etl/etl_runner.py --db oracle
```
✅ **It should print the config (with everything masked).**

### **2️⃣ Run on Databricks**
```sh
databricks bundle deploy
databricks bundle run oracle_etl_job
```
✅ **It should print the masked config in Databricks logs.**

---

## **✅ 6. Expected Output**
```
✅ Config Loaded Successfully!
{'oracle': {'bridge': {'host': 'xxxxx', 'port': 'xxxxx', 'service_name': 'xxxxx', 'user': 'xxxxx', 'password': 'xxxxx', 'tables': [{'name': 'xxxxx'}]}}, 'unity_catalog': {'target_catalog': 'xxxxx', 'target_schema': 'xxxxx', 'target_format': 'xxxxx'}}
```

🚀 **Now everything is masked in the config file, but the framework remains unchanged!** 🚀
