Here is the **full working code** with the **password masked (`xxxxx`)** in the configuration file. This includes:
- ✅ `BaseETL` (Loads & prints config)
- ✅ `OracleToUCETL` (Extends `BaseETL`)
- ✅ `ETLRunner` (Calls `OracleToUCETL`)
- ✅ `oracle.yml` (Password masked)

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

## **✅ 1. `base_etl.py` (Reads Config and Prints It)**
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
        self.logger.info(f"📜 Loaded configuration for {db_type}: {self.mask_sensitive_info(self.config)}")
        print("✅ Config Loaded Successfully!")
        print(self.mask_sensitive_info(self.config))

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

    def mask_sensitive_info(self, config):
        """Replaces sensitive values like passwords with 'xxxxx'."""
        if "oracle" in config and "bridge" in config["oracle"] and "password" in config["oracle"]["bridge"]:
            config["oracle"]["bridge"]["password"] = "xxxxx"
        return config
```

---

## **✅ 2. `oracle_etl.py` (Calls `BaseETL` to Print Config)**
```python
from naacsanlyt_etl.base_etl import BaseETL

class OracleToUCETL(BaseETL):
    def __init__(self):
        """Initialize Oracle ETL process and only load config."""
        super().__init__(db_type="oracle")
```

---

## **✅ 3. `etl_runner.py` (Runs `OracleToUCETL` to Print Config)**
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

## **✅ 4. `oracle.yml` (Config File with Password Masked)**
```yaml
oracle:
  bridge:
    host: "nclpvngdbo10011.cmp.aon.net"
    port: 1526
    service_name: "rspaprt1"
    user: "svc_databricks_readonly"
    password: "xxxxx"
    tables:
      - name: "AONDBA.CLIENT_ACCOUNT"

unity_catalog:
  target_catalog: "dasp_system"
  target_schema: "na_etl_dev"
  target_format: "delta"
```

---

## **🚀 5. Running the Code**
### **1️⃣ Run Locally**
```sh
python src/naacsanlyt_etl/etl_runner.py --db oracle
```
✅ **It should print the config (with password masked).**

### **2️⃣ Run on Databricks**
```sh
databricks bundle deploy
databricks bundle run oracle_etl_job
```
✅ **It should print the config in Databricks logs.**

---

## **✅ 6. Expected Output**
```
✅ Config Loaded Successfully!
{'oracle': {'bridge': {'host': 'nclpvngdbo10011.cmp.aon.net', 'port': 1526, 'service_name': 'rspaprt1', 'user': 'svc_databricks_readonly', 'password': 'xxxxx', 'tables': [{'name': 'AONDBA.CLIENT_ACCOUNT'}]}}, 'unity_catalog': {'target_catalog': 'dasp_system', 'target_schema': 'na_etl_dev', 'target_format': 'delta'}}
```

🚀 **Now your entire ETL framework reads and prints the config with the password masked!** 🚀
