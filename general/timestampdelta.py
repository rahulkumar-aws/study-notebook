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
        "Service_Request_Header", "Account_Handling", "AHI_AI_Waive_LP", "AHI_Auto_Id",
        "AHI_Captive_Info", "AHI_Cert_Info", "Ahi_Cert_Limit", "AHI_Client_Direct_Release",
        "AHI_COI_Endorsement", "AHI_Comments", "AHI_Contact_Info", "AHI_Distribution",
        "AHI_Email_Notifications", "AHI_Invoice_Endorsement_Audits", "AHI_Limits", "AHI_Lob",
        "AHI_PITD_Delivery", "AHI_Spl_Account_Instructions", "Client_Master", "Client_Series_Mapping",
        "Delivery_Contact", "Disbursement_Payable", "Holiday_RPT", "Policy_Comment", "Policy_Contact",
        "Service_Request_Distribution", "Service_Request_Group_Header", "Service_Request_History",
        "Service_Request_Invoices", "Service_Request_Issue_Details", "WorkSpace_Additional_Delivery",
        "WorkSpace_Comments", "WorkSpace_Delivery_Contact", "WorkSpace_Header",
        "WorkSpace_Header_Distribution", "WorkSpace_Policy_Contact", "WorkSpace_Policy_Container",
        "AHI_Request_Compliance", "Compliance_Out_Of_Scope_Details"
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
            df = self.spark.createDataFrame(self.load_report)
            df.createOrReplaceTempView("load_report")
            self.logger.info("📊 Load Summary Report:")
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
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
        df_columns = df.columns
        df.createOrReplaceTempView("source")

        on_clause = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk_columns])
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

            updated_sql = f"""
                SELECT COUNT(*) as cnt FROM source
                WHERE EXISTS (SELECT 1 FROM {target_discovery_table} AS target WHERE {on_clause})
            """
            inserted_sql = f"""
                SELECT COUNT(*) as cnt FROM source
                WHERE NOT EXISTS (SELECT 1 FROM {target_discovery_table} AS target WHERE {on_clause})
            """

            updated_count = self.spark.sql(updated_sql).collect()[0]["cnt"]
            inserted_count = self.spark.sql(inserted_sql).collect()[0]["cnt"]

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

    def process_table(self, table, jdbc_url, jdbc_props, application_name, schema_name):
        env_config = Constants.ENVIRONMENTS[self.environment]
        target_discovery_table = f"{env_config['DISCOVERY_CATALOG']}.{Constants.DISCOVERY_SCHEMA_BD}.{table}"
        watermark_table = f"{env_config['METADATA_CATALOG']}.{Constants.METADATA_SCHEMA}.watermark_{application_name}"

        # ✅ Step 1: Check if table exists before watermark
        try:
            self.spark.sql(f"SELECT * FROM {target_discovery_table} LIMIT 1")
            table_exists = True
        except:
            table_exists = False

        if not table_exists:
            self.logger.info(f"🆕 Table `{target_discovery_table}` does not exist. Proceeding with full load.")
            is_first_run = True
        else:
            try:
                wm_df = self.spark.sql(f"SELECT last_updated FROM {watermark_table} WHERE table_name = '{table}'")
                is_first_run = wm_df.count() == 0
                if is_first_run:
                    self.logger.info(f"📥 Table exists but no watermark found for `{table}`. Performing full load.")
                else:
                    self.logger.info(f"🔁 Delta load for `{table}` — watermark exists.")
                last_ts = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') if is_first_run else wm_df.collect()[0]["last_updated"]
            except:
                is_first_run = True
                self.logger.info(f"📥 Failed to read watermark for `{table}`. Performing full load.")
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

        last_ts_str = last_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        query = f"SELECT * FROM {schema_name}.[{table}]" if is_first_run else \
                f"SELECT * FROM {schema_name}.[{table}] WHERE {modification_column} > '{last_ts_str}'"

        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        df = df.withColumn(modification_column, to_timestamp(col(modification_column), "yyyy-MM-dd HH:mm:ss.SSS"))
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

        if df.count() == 0:
            self.logger.info(f"⚠️ No new data found for table `{table}`. Skipping.")
            return

        if not table_exists:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_discovery_table)
            inserted = df.count()
            self.load_report.append({
                "table_name": table.lower(),
                "operation": "INSERT",
                "inserted": inserted,
                "updated": 0,
                "record_count": inserted
            })
            self.watermark_mgr.update_watermark(table.lower(), datetime.now(pytz.timezone("US/Central")), application_name)
        elif not is_first_run:
            pk_columns = self.get_primary_keys(jdbc_url, jdbc_props, schema_name, table)
            self.logger.info(f"🔄 Executing Delta Merge for `{table}`")
            self.merge_data(df, target_discovery_table, pk_columns, table)
            self.watermark_mgr.update_watermark(table.lower(), datetime.now(pytz.timezone("US/Central")), application_name)
        else:
            self.logger.warning(f"⚠️ Skipping merge for `{table}`: Table exists but no watermark. Possible inconsistency.")

    def get_table_schema(self, table_name):
        return [field.name for field in self.spark.table(table_name).schema.fields]
