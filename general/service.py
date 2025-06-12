from datetime import datetime
from pyspark.sql.functions import col, to_timestamp
import boto3
import json
from naanalytics_bd.Constants import Constants
from naanalytics_bd.job_executor import JOBExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

class BdServiceRequestHeaderDeltaLoad(JOBExecutor):
    join_condition_map = {
        "service_request_invoices": "ServiceRequestId",
        "service_request_distribution": "ServiceRequestId",
        "service_request_history": "ServiceRequestId",
        "holiday_rpt": "ServiceRequestId"
    }

    INCREMENTAL_LOAD_TABLES = [
        "service_request_invoices",
        "service_request_distribution",
        "service_request_history",
        "holiday_rpt",
        "client_ast",
        "client_level_changes",
        "client_sic_codes",
        "client_practice_group",
        "internal_processing_document_index",
        "discrepancy",
        "client_lobs",
        "client_quote",
        "client_maintenance_header",
        "fee_billing_header",
        "internal_processing_policy_documentation",
        "invoice_policy_billing_audit_retro",
        "internal_processing_policy_ast_update",
        "internal_processing_usersetup_genpactuser",
        "invoice_correction",
        "invoice_discrepancy"
    ]

    def __init__(self, spark, environment):
        super().__init__(spark, environment)
        self.spark = spark
        self.environment = environment
        self.application_name = "broker_desktop"
        self.env_config = Constants.ENVIRONMENTS[self.environment]["CONFIG"]
        self.database = self.env_config.database
        self.load_report = []

    def execute(self):
        self.job_start_time = datetime.now(pytz.timezone("US/Central"))
        self.process_raw_layer()

    def get_secret(self, secret_name):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        try:
            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response["SecretString"])
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
        query = f"""
            SELECT c.name AS COLUMN_NAME
            FROM sys.key_constraints kc
            JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
            WHERE kc.type = 'PK' AND kc.parent_object_id = OBJECT_ID('{schema_name}.{table}')
        """
        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        return [r["COLUMN_NAME"].lower() for r in df.collect()]

    def get_table_schema(self, table_name):
        return [field.name for field in self.spark.table(table_name).schema.fields]

    def get_service_request_watermark(self):
        watermark_table = f"{Constants.ENVIRONMENTS[self.environment]['METADATA_CATALOG']}.{Constants.METADATA_SCHEMA}.watermark_broker_desktop"
        df = self.spark.table(watermark_table).filter(col("table_name") == "service_request_header")
        if df.count() == 0:
            return None
        return df.select("max_ts").collect()[0]["max_ts"]

    def merge_data(self, df, target_table, pk_columns, table_name):
        target_cols = set(self.get_table_schema(target_table))
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
        df = df.select([col(c) for c in df.columns if c in target_cols])

        missing_pk = list(set(pk_columns) - set(df.columns))
        if missing_pk:
            self.logger.warning("⚠️ Merge keys not found in source df for `%s`: %s", table_name, missing_pk)
            return

        df.createOrReplaceTempView("source")
        df_columns = df.columns

        on_clause = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk_columns])
        set_clause = ", ".join([f"target.`{c}` = source.`{c}`" for c in df_columns])
        insert_cols = ", ".join([f"`{c}`" for c in df_columns])
        insert_vals = ", ".join([f"source.`{c}`" for c in df_columns])

        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING source AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        self.logger.info(f"🔄 Executing MERGE for `{target_table}`")

        try:
            self.spark.sql(merge_sql)
            updated_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM source WHERE EXISTS (SELECT 1 FROM {target_table} target WHERE {on_clause})").collect()[0]["cnt"]
            inserted_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM source WHERE NOT EXISTS (SELECT 1 FROM {target_table} target WHERE {on_clause})").collect()[0]["cnt"]

            operation = "INSERT & UPDATE" if updated_count and inserted_count else "UPDATE" if updated_count else "INSERT"

            self.load_report.append({
                "table_name": table_name.lower(),
                "operation": operation,
                "inserted": inserted_count,
                "updated": updated_count,
                "record_count": inserted_count + updated_count,
                "job_start_time": self.job_start_time.strftime('%Y-%m-%d %H:%M:%S')
            })

        except Exception as e:
            self.logger.error(f"❌ MERGE failed for `{target_table}`: {e}")

    def process_table(self, table, jdbc_url, jdbc_props, app_name, schema_name):
        env_config = Constants.ENVIRONMENTS[self.environment]
        target_table = f"{env_config['DISCOVERY_CATALOG']}.{Constants.DISCOVERY_SCHEMA_BD}.{table.lower()}"

        try:
            self.spark.sql(f"SELECT * FROM {target_table} LIMIT 1")
            table_exists = True
        except:
            table_exists = False

        join_key = self.join_condition_map.get(table.lower())
        if not join_key:
            df_sample = self.get_dataframe(jdbc_url, jdbc_props, f"SELECT TOP 1 * FROM {schema_name}.[{table}]")
            possible_keys = [c for c in df_sample.columns if "servicerequestid" in c.lower()]
            if possible_keys:
                join_key = possible_keys[0]
                self.logger.warning(f"⚠️ Using inferred join key `{join_key}` for table {table}")
            else:
                self.logger.error(f"❌ No join key defined or inferred for table {table}")
                return

        last_ts = self.get_service_request_watermark()
        if not last_ts:
            self.logger.warning("⚠️ No watermark found; doing full load for `%s`", table)
            query = f"SELECT * FROM {schema_name}.[{table}]"
        else:
            query = f"""
                SELECT s.* FROM {schema_name}.[{table}] s
                JOIN {schema_name}.[service_request_header] h
                ON s.[{join_key}] = h.[ServiceRequestId]
                WHERE h.ModifiedTime > '{last_ts}'
            """

        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        self.logger.info("📘 Source schema for `%s`: %s", table, df.dtypes)

        if df.count() == 0:
            self.logger.info(f"⚠️ No new data found for table `{table}`. Skipping.")
            return

        df = df.withColumnRenamed(join_key, join_key.lower())
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

        if not table_exists:
            self.logger.info("📗 Target schema for `%s`: %s", table, self.get_table_schema(target_table))
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            self.load_report.append({
                "table_name": table.lower(),
                "operation": "INSERT",
                "inserted": df.count(),
                "updated": 0,
                "record_count": df.count(),
                "job_start_time": self.job_start_time.strftime('%Y-%m-%d %H:%M:%S')
            })
        else:
            pk_columns = self.get_primary_keys(jdbc_url, jdbc_props, schema_name, table)
            self.logger.info("📗 Target schema for `%s`: %s", table, self.get_table_schema(target_table))
            self.merge_data(df, target_table, pk_columns, table)

    def process_raw_layer(self):
        conn = self.get_secret(self.env_config.secret_name)
        jdbc_url = f"jdbc:sqlserver://{self.env_config.host}:{self.env_config.port};databaseName={self.env_config.database}"
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
                executor.submit(self.process_table, table, jdbc_url, jdbc_props, self.application_name, "dbo"): table
                for table in self.INCREMENTAL_LOAD_TABLES
            }
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"❌ Error processing table `{table}`: {e}")

        if self.load_report:
            df = self.spark.createDataFrame(self.load_report)
            report_table = f"{Constants.ENVIRONMENTS[self.environment]['METADATA_CATALOG']}.{Constants.METADATA_SCHEMA}.load_report_{self.application_name.lower()}"
            df.write.format("delta").mode("append").saveAsTable(report_table)
            self.logger.info("📊 Load Summary Report:")
            df.show(truncate=False)
        else:
            self.logger.info("📭 No tables updated.")
