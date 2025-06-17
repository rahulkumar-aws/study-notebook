def process_table(self, table, jdbc_url, jdbc_props, application_name, schema_name):
    table_copy = copy.deepcopy(table)
    env_config = Constants.ENVIRONMENTS[self.environment]
    target_discovery_table = f"{env_config['DISCOVERY_CATALOG']}.{Constants.DISCOVERY_SCHEMA_BD}.{table_copy}"
    watermark_table = f"{env_config['METADATA_CATALOG']}.{Constants.METADATA_SCHEMA}.watermark_{application_name}"

    try:
        self.spark.sql(f"SELECT * FROM {target_discovery_table} LIMIT 1")
        table_exists = True
    except Exception:
        table_exists = False

    if not table_exists:
        self.logger.info(f"🆕 Table `{target_discovery_table}` does not exist. Proceeding with full load.")
        is_first_run = True
        last_ts = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    else:
        try:
            wm_df = self.spark.sql(f"SELECT last_updated FROM {watermark_table} WHERE table_name = '{table}'")
            is_first_run = wm_df.count() == 0
            last_ts = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S') if is_first_run else wm_df.collect()[0]["last_updated"]
        except Exception:
            is_first_run = True
            last_ts = datetime.strptime('1900-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
            self.logger.info(f"📥 Failed to read watermark for `{table_copy}`. Performing full load.")

    mod_col_query = f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_copy}' AND TABLE_SCHEMA = '{schema_name}'
        AND COLUMN_NAME IN ('ModifyOn', 'ModifiedTime', 'ModifiedDateTime', 'Date', 'CreatedTime', 'CreatedDate')
    """
    mod_col_df = self.get_dataframe(jdbc_url, jdbc_props, mod_col_query)
    modification_column = None if mod_col_df.count() == 0 else mod_col_df.collect()[0]["COLUMN_NAME"]

    if not modification_column:
        self.logger.error(f"No valid modification column found for {table}")
        return

    last_ts_str = last_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    if is_first_run and table_exists:
        # Full reload because watermark is missing for existing table
        self.logger.info(f"🆕 Table `{table}` exists but no watermark found. Performing full reload.")
        query = f"SELECT * FROM {schema_name}.[{table}]"
        self.logger.info(f"delta record query {query}")
        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        df = df.withColumn(modification_column, to_timestamp(col(modification_column), "yyyy-MM-dd HH:mm:ss.SSS"))
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

        if df.count() == 0:
            self.logger.info(f"⚠️ No data found for full reload of `{table}`. Skipping.")
            return

        self.logger.info(f"📗 Target table schema for full reload of `{table}`: {self.get_table_schema(target_discovery_table)}")

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_discovery_table)

        inserted = df.count()
        self.load_report.append({
            "table_name": table.lower(),
            "operation": "FULL LOAD",
            "inserted": inserted,
            "updated": 0,
            "record_count": inserted
        })

        self.watermark_mgr.update_watermark(table.lower(), self.job_start_time, application_name)

    elif not table_exists:
        # Initial full load for new table
        self.logger.info(f"🆕 Table `{target_discovery_table}` does not exist. Proceeding with full load.")
        query = f"SELECT * FROM {schema_name}.[{table}]"
        self.logger.info(f"delta record query {query}")
        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        df = df.withColumn(modification_column, to_timestamp(col(modification_column), "yyyy-MM-dd HH:mm:ss.SSS"))
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

        if df.count() == 0:
            self.logger.info(f"⚠️ No data found for initial load of `{table}`. Skipping.")
            return

        self.logger.info(f"📗 Target table schema for initial load of `{table}`: {self.get_table_schema(target_discovery_table)}")

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_discovery_table)

        inserted = df.count()
        self.load_report.append({
            "table_name": table.lower(),
            "operation": "FULL LOAD",
            "inserted": inserted,
            "updated": 0,
            "record_count": inserted
        })

        self.watermark_mgr.update_watermark(table.lower(), self.job_start_time, application_name)

    else:
        # Incremental merge case
        query = f"SELECT * FROM {schema_name}.[{table}] WHERE {modification_column} > '{last_ts_str}'"
        self.logger.info(f"delta record query {query}")
        df = self.get_dataframe(jdbc_url, jdbc_props, query)
        df = df.withColumn(modification_column, to_timestamp(col(modification_column), "yyyy-MM-dd HH:mm:ss.SSS"))
        df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])

        if df.count() == 0:
            self.logger.info(f"⚠️ No new data found for incremental load of `{table}`. Skipping.")
            return

        pk_columns = self.get_primary_keys(jdbc_url, jdbc_props, schema_name, table)
        self.logger.info(f"📗 Target table schema for incremental merge of `{table}`: {self.get_table_schema(target_discovery_table)}")

        self.merge_data(df, target_discovery_table, pk_columns, table)

        self.watermark_mgr.update_watermark(table.lower(), self.job_start_time, application_name)
