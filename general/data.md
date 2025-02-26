Here's the **updated `BaseETL` and `OracleToUCETL`** classes with all **data processing removed** so that it **only reads and prints the config**.

---

## **✅ 1. `base_etl.py` (Config Reader Only)**
This version:
- ✅ **Reads the config from DBFS or packaged resources**
- ✅ **Logs and prints the loaded config**
- ❌ **Removes Unity Catalog & data processing**

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

## **✅ 2. `oracle_etl.py` (Calls Config Reader)**
This class **only initializes `BaseETL`** and prints the config.

```python
from naacsanlyt_etl.base_etl import BaseETL

class OracleToUCETL(BaseETL):
    def __init__(self):
        """Initialize Oracle ETL process and only load config."""
        super().__init__(db_type="oracle")
```

---

## **✅ 3. Running the Config Loader**
### **Run Locally**
```sh
python -c "from naacsanlyt_etl.etl_jobs.oracle_etl import OracleToUCETL; OracleToUCETL()"
```

### **Run on Databricks**
```sh
databricks bundle deploy
databricks bundle run oracle_etl_job
```

---

## **✅ Expected Output**
```
✅ Config Loaded Successfully!
{'oracle': {'bridge': {'host': 'nclpvngdbo10011.cmp.aon.net', 'port': 1526, 'service_name': 'rspaprt1', 'user': 'svc_databricks_readonly', 'password': 'bu11_d0zer_uat', 'tables': [{'name': 'AONDBA.CLIENT_ACCOUNT'}]}}, 'unity_catalog': {'target_catalog': 'dasp_system', 'target_schema': 'na_etl_dev', 'target_format': 'delta'}}
```

🚀 **Now, your ETL framework only reads and prints the config!** Let me know if this works. 🚀
