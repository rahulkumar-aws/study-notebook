from datetime import datetime
from pyspark.sql.functions import col
import boto3
import json
from naanalytics_bd.Constants import Constants
from naanalytics_bd.job_executor import JOBExecutor
from naanalytics_bd.watermark_manager import WatermarkTableManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

class BdTimestampDeltaLoad(JOBExecutor):
    DELTA_TABLE_WITH_WATERMARK = [
        "AHI_Client_DR_RequestTypes",
    ]

    def __init__(self, spark, environment):
        super().__init__(spark, environment)
        self.spark = spark
        self.environment = environment
        self.application_name = Constants.APPLICATION_NAME
        self.env_config = Constants.ENVIRONMENTS[self.environment]["CONFIG"]
        self.database = self.env_config.database
        self.watermark_mgr = WatermarkTableManager(
            spark=self.spark,
            application_name=self.application_name,
            database=self.database,
            environment=self.environment
        )
        self.watermark_mgr.create_watermark_table()
        self.load_report = []

    def execute(self):
        self.process_raw_layer()

    def get_secret(self, secret_name):
        client = boto3.client('secretsmanager', region_name='us-east-1')
        try:
            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response['SecretString'])
        except Exception as e:
            self.logger.error(f"Error retrieving secret: {e}")
            return None

    def get_dataframe(self, jdbc_url, jdbc_props, query):
        return self.spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", jdbc_props["user"]) \
            .option("password", jdbc_props["password"]) \
            .option("driver", jdbc_props["driver"]) \
            .option("numPartitions", 8) \
            .option("fetchsize", 100000) \
            .option("authenticationScheme", jdbc_props["authenticationScheme"]) \
            .option("domain", jdbc_props["domain"]) \
            .option("integratedSecurity", jdbc_props["integratedSecurity"]) \
            .option("trustServerCertificate", jdbc_props["trustServerCertificate"]) \
            .load()

    def process_table(self, table, jdbc_url, jdbc_props, application_name, schema_name):
        env_config = Constants.ENVIRONMENTS[self.environment]
        target_discovery_table = f"{env_config['DISCOVERY_CATALOG']}.{Constants.DISCOVERY_SCHEMA_BD}.{table.lower()}"
        watermark_table = f"{env_config['METADATA_CATALOG']}.{Constants.METADATA_SCHEMA}.watermark_{application_name}"

        try:
            wm_df = self.spark.sql(f"SELECT last_updated FROM {watermark_table} WHERE table_name = '{table}'")
            is_first_run = wm_df.count() == 0
            last_ts = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') if is_first_run else wm_df.collect()[0]["last_updated"]
            self.logger.info(f"Watermark last_updated for {table}: {last_ts}")
            load_mode = 'FULL' if is_first_run else 'DELTA'
        except Exception as e:
            self.logger.warn(f"Watermark fetch failed, assuming full load: {e}")
            is_first_run = True
            load_mode = 'FULL'
            last_ts = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

        mod_col_query = f"""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{schema_name}'
            AND COLUMN_NAME IN ('ModifyOn', 'ModifiedTime', 'ModifiedDateTime', 'Date', 'CreatedTime', 'CreatedDate')
        """
        mod_col_df = self.get_dataframe(jdbc_url, jdbc_props, mod_col_query)
        modification_column = None if mod_col_df.count() == 0 else mod_col_df.collect()[0]["COLUMN_NAME"]

        if not modification_column:
            self.logger.error(f"No valid modification column found for {table}")
            return

        current_time = datetime.now(pytz.timezone("US/Central"))

        query = f"SELECT * FROM {schema_name}.[{table}]" if is_first_run else \
                f"SELECT * FROM {schema_name}.[{table}] WHERE {modification_column} > '{last_ts.strftime('%Y-%m-%d %H:%M:%S')}'"

        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
        record_count = df.count()

        self.logger.info(f"📦 Retrieved {record_count} records for {table}")
        if record_count > 0:
            self.logger.info(f"📊 Sample data for {table}:")
            df.show(5, truncate=False)

        if record_count == 0:
            return

        self.load_report.append({"table_name": table, "load_mode": load_mode, "record_count": record_count})

        try:
            self.spark.sql(f"SELECT * FROM {target_discovery_table} LIMIT 1")
            table_exists = True
        except:
            table_exists = False

        if not table_exists:
            df.write.format("delta").mode("overwrite").saveAsTable(target_discovery_table)
        else:
            pk_columns = self.get_primary_keys(jdbc_url, jdbc_props, schema_name, table)
            df_columns = [c.lower() for c in df.columns]

            self.logger.info(f"🔍 Expected PKs: {pk_columns}")
            self.logger.info(f"🧾 DataFrame columns: {df_columns}")

            missing_pks = [col for col in pk_columns if col not in df_columns]
            if missing_pks:
                error_msg = f"❌ Missing PK columns for merge in table `{table}`: {missing_pks}"
                self.logger.error(error_msg)
                raise Exception(error_msg)

            try:
                self.merge_data(df, target_discovery_table, pk_columns)
            except Exception as merge_error:
                self.logger.error(f"❌ Merge failed for table `{table}`: {merge_error}")
                self.logger.error(f"MERGE DF columns: {df.columns}")
                raise

        self.watermark_mgr.update_watermark(table, current_time, application_name)

    def get_primary_keys(self, jdbc_url, jdbc_props, schema_name, table):
        pk_query = f"""
            SELECT c.name AS COLUMN_NAME
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE kc.type = 'PK' AND kc.parent_object_id = OBJECT_ID('{schema_name}.{table}')
        """
        df = self.get_dataframe(jdbc_url, jdbc_props, pk_query)
        return [r["COLUMN_NAME"].lower() for r in df.collect()]

    def merge_data(self, df, target_discovery_table, pk_columns):
        if not pk_columns:
            raise ValueError("❌ Cannot perform merge without primary key columns.")

        df_columns_set = set(df.columns)
        missing_pks = [pk for pk in pk_columns if pk not in df_columns_set]
        if missing_pks:
            raise ValueError(f"❌ Missing primary key columns in DataFrame: {missing_pks}")

        df.createOrReplaceTempView("source")

        on_clause = " AND ".join([f"target.{c} = source.{c}" for c in pk_columns])
        set_clause = ", ".join([f"target.{c} = source.{c}" for c in df.columns])

        if not on_clause.strip():
            raise ValueError("❌ Empty ON clause generated for MERGE. Check PK extraction logic.")

        merge_sql = f"""
            MERGE INTO {target_discovery_table} AS target
            USING source AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """
        self.logger.info(f"MERGE SQL:\n{merge_sql}")
        self.spark.sql(merge_sql)

def process_raw_layer(self):
    conn = self.get_secret(self.env_config.secret_name)
    jdbc_url = f"jdbc:sqlserver://{self.env_config.host}:{self.env_config.port};databaseName={self.database}"
    jdbc_props = {
        "user": conn.get("username"),
        "password": conn.get("password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "authenticationScheme": "NTLM",
        "domain": "AONNET",
        "integratedSecurity": "true",
        "trustServerCertificate": "true"
    }

    # 🔁 Map futures to table names for traceback
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(
                self.process_table, table, jdbc_url, jdbc_props, self.application_name, "dbo"
            ): table for table in self.DELTA_TABLE_WITH_WATERMARK
        }

        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as e:
                self.logger.error(f"❌ Error processing table `{table}`: {str(e)}")
                raise

    if self.load_report:
        df = self.spark.createDataFrame(self.load_report)
        self.logger.info("📊 Load Summary Report:")
        df.show(truncate=False)
    else:
        self.logger.info("📭 No tables were updated in this run.")

