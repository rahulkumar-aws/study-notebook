from pyspark.sql import SparkSession
from naanalytics_dataloader.dependencies.utils.config_reader import ConfigReader
from naanalytics_dataloader.dependencies.utils.watermark import Watermark
from naanalytics_dataloader.dependencies.utils.aws_utils import AWSUtils
from naanalytics_dataloader.dependencies.core.logger import Logging

class BaseETLJob:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.spark = None
        self.logger = None
        self.configs = None
        self.params = None
        self.env_config = None
        self.data_config = None
        self.source_conf = None
        self.dest_conf = None
        self.region = None
        self.username = None
        self.password = None
        self.application_name = None
        self.job_history_conf = None
        self.volume_name = None
        self.watermark_table = None
        self.watermark = None
        self.timestamp_column_map = None
        self.join_by_servicerequestid_map = None
        self.source_schema = None

    def initialize(self):
        self.spark = SparkSession.builder.appName(self.job_name).getOrCreate()
        self.logger = Logging.logger(self.job_name)
        self.logger.info(f"🛠 Initializing job: {self.job_name}")
        
        self.watermark = Watermark(self.spark)
        self.configs = ConfigReader().get_configs()
        self.env_config = self.configs["env_config"]
        self.data_config = self.configs["data_config"]
        self.params = ConfigReader().get_param_options()

        self.source_conf = self.env_config['source']
        self.dest_conf = self.env_config['destination']
        self.job_history_conf = self.env_config['job_history_conf']
        self.application_name = self.env_config.get("application_name")
        self.volume_name = self.dest_conf.get("volume_path")

        self.timestamp_column_map = self.data_config['objects'].get('timestamp_column_map', {})
        self.join_by_servicerequestid_map = self.data_config['objects'].get('join_by_servicerequestid_map', {})

        self.region = self.spark.conf.get("spark.databricks.clusterUsageTags.dataPlaneRegion")
        self.username, self.password = AWSUtils.get_aws_secret_details(self.source_conf['secret_name'], self.region)

        watermark_table_name = f"{self.dest_conf.get('watermark_table')}_{self.application_name}"
        self.source_schema = self.source_conf.get("schema", "aondba")
        self.watermark_table = f"{self.dest_conf['catalog']}.{self.job_history_conf['schema']}.{watermark_table_name}"

        self.logger.info(f"✅ Initialization complete for {self.job_name}")
