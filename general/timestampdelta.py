from datetime import datetime
from pyspark.sql.functions import col, to_timestamp
import boto3
import json
from naanalytics_bd.Constants import Constants
from naanalytics_bd.job_executor import JOBExecutor
from naanalytics_bd.watermark_manager import WatermarkTableManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

class BdTimestampDeltaLoad(JOBExecutor):
    DELTA_TABLE_WITH_WATERMARK = [
        # Add your table list here...
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
        self.job_start_time = datetime.now(pytz.timezone("US/Central"))
        self._process_raw_layer()

    def _process_raw_layer(self):
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

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(self.process_table, table.lower(), jdbc_url, jdbc_props, self.application_name, "dbo"): table
                for table in self.DELTA_TABLE_WITH_WATERMARK
            }
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"❌ Error processing table `{table}`: {str(e)}")

        if self.load_report:
            for r in self.load_report:
                r['job_start_time'] = self.job_start_time.strftime('%Y-%m-%d %H:%M:%S')
            df = self.spark.createDataFrame(self.load_report)
            df.createOrReplaceTempView("load_report")
            report_table = f"{Constants.METADATA_CATALOG}.{Constants.METADATA_SCHEMA}.load_report_{self.application_name.lower()}"
            df.write.format("delta").mode("append").saveAsTable(report_table)
            self.logger.info(f"📊 Load Summary Report appended to {report_table}:")
            self.spark.sql("SELECT * FROM load_report ORDER BY table_name").show(truncate=False)
        else:
            self.logger.info("📭 No tables were updated in this run.")

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

    def get_primary_keys(self, jdbc_url, jdbc_props, schema_name, table):
        pk_query = f"""
            SELECT c.name AS COLUMN_NAME
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
            WHERE kc.type = 'PK' AND kc.parent_object_id = OBJECT_ID('{schema_name}.{table}')
        """
        df = self.get_dataframe(jdbc_url, jdbc_props, pk_query)
        return [r["COLUMN_NAME"].lower() for r in df.collect()]

    def merge_data(self, df, target_discovery_table, pk_columns, table_name):
        target_cols = set(self.get_table_schema(target_discovery_table))
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
        df = df.select([col(c) for c in df.columns if c in target_cols])

        missing_pk = list(set(pk_columns) - set(df.columns))
        if missing_pk:
            self.logger.warning("⚠️ Merge keys not found in source df for `%s`: %s", table_name, missing_pk)
            return

        df_columns = df.columns
        df.createOrReplaceTempView("source")

        on_clause = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk_columns if c in df.columns])
        set_clause = ", ".join([f"target.`{c}` = source.`{c}`" for c in df_columns])
        insert_cols = ", ".join([f"`{c}`" for c in df_columns])
        insert_vals = ", ".join([f"source.`{c}`" for c in df_columns])

        merge_sql = f"""
            MERGE INTO {target_discovery_table} AS target
            USING source AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        self.logger.info(f"🔄 Executing MERGE for `{target_discovery_table}`")

        try:
            self.spark.sql(merge_sql)

            updated_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM source WHERE EXISTS (SELECT 1 FROM {target_discovery_table} AS target WHERE {on_clause})").collect()[0]["cnt"]
            inserted_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM source WHERE NOT EXISTS (SELECT 1 FROM {target_discovery_table} AS target WHERE {on_clause})").collect()[0]["cnt"]

            operation_type = "INSERT"
            if updated_count > 0 and inserted_count > 0:
                operation_type = "INSERT & UPDATE"
            elif updated_count > 0:
                operation_type = "UPDATE"

            self.load_report.append({
                "table_name": table_name.lower(),
                "operation": operation_type,
                "inserted": inserted_count,
                "updated": updated_count,
                "record_count": inserted_count + updated_count
            })

        except Exception as e:
            self.logger.error(f"❌ MERGE failed for `{target_discovery_table}`: {e}")

    def get_table_schema(self, table_name):
        return [field.name for field in self.spark.table(table_name).schema.fields]
