def process_table(self, table, source_conf, dest_conf, job_history_conf, region, initial_start_time):
    sc = self.spark.sparkContext
    job_info_dict = {}
    start_time = JobInfo.get_current_utc_ts()
    domain = source_conf.get('domain')

    load_type = self.params.load_type
    self.logger.info(f"🚀 Starting processing for table: {table} with load_type: {load_type}")
    modification_column = self.data_config.get("timestamp_column_map", {}).get(table, "ModifiedTime")
    self.logger.info(f"🕒 Using modification column: {modification_column}")

    last_watermark = None
    last_watermark_str = "NA"

    # Determine SQL query based on load type
    if load_type == "full_load":
        query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
        self.logger.info(f"📥 Full load query prepared for table: {table}")

    elif load_type == "delta":
        join_condition = self.data_config.get("delta_join_condition", {}).get(table)
        if join_condition:
            self.logger.info(f"📌 Join condition defined for {table}: {join_condition}")

        last_watermark = self.watermark.fetch_watermark(self.watermark_table, table, Constants.LAST_FETCH_COLUMN_NAME)
        if last_watermark is None:
            self.logger.info(f"🕳 No previous watermark found. Performing full load for table: {table}")
            query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
        else:
            last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"💧 Last watermark for delta load: {last_watermark_str}")
            query = f"(SELECT * FROM [dbo].[{table}] WHERE {modification_column} > '{last_watermark}' AND {modification_column} <= '{initial_start_time}') AS {table}_alias"
            self.logger.info(f"📤 Delta query prepared for table: {table}")

    elif load_type == "append":
        last_watermark = self.watermark.fetch_watermark(self.watermark_table, table, Constants.LAST_FETCH_COLUMN_NAME)
        if last_watermark is None:
            self.logger.info(f"🕳 No previous watermark found. Performing full load for table: {table}")
            query = f"(SELECT * FROM [dbo].[{table}]) AS {table}_alias"
        else:
            last_watermark_str = last_watermark.strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"💧 Last watermark for append load: {last_watermark_str}")
            query = f"(SELECT * FROM [dbo].[{table}] WHERE {modification_column} > '{last_watermark}' AND {modification_column} <= '{initial_start_time}') AS {table}_alias"
            self.logger.info(f"📤 Append query prepared for table: {table}")

    else:
        self.logger.error(f"❌ Unsupported load_type: {load_type}")
        raise ValueError(f"❌ Unsupported load_type: {load_type}")

    # Read from source
    username, password = AWSUtils.get_aws_secret_details(source_conf['secret_name'], region)
    self.logger.info(f"🔐 MSSQL Username being used: {username}")
    self.logger.info(f"🧾 SQL Query: {query}")

    try:
        df = MSSQLConnector.reader(
            self.spark, source_conf['host'], source_conf['port'], source_conf['database'],
            query, username, password, domain=domain
        )
    except Exception as e:
        self.logger.error(f"❌ Failed to read from source for table {table}: {e}")
        return

    volume_path = self.watermark.latest_volume_path(dest_conf['volume_path'], table, initial_start_time)
    df = df.withColumn("load_datetimestamp", lit(initial_start_time))
    record_processed = df.count()
    if record_processed == 0:
        self.logger.warning(f"⚠️ No records fetched from source for table: {table}")
        return

    self.logger.info(f"✅ {record_processed} records fetched for table: {table}")
    df.show(5, truncate=False)
    self.write_raw_data(df, volume_path, dest_conf['file_format'], dest_conf['write_mode'])

    # Discovery layer write
    df_sanitized = Transform.sanitize_cols(df)
    target_table = f"{dest_conf['catalog']}.{dest_conf['discovery_schema']}.{table}"
    self.logger.info(f"💾 Writing data to discovery layer table: {target_table}")

    if load_type == "full_load":
        self.logger.info(f"📝 Performing overwrite to discovery table: {target_table}")
        df_sanitized.write.mode("overwrite").format("delta").saveAsTable(target_table)

    elif load_type == "append":
        self.logger.info(f"➕ Performing append to discovery table: {target_table}")
        df_sanitized.write.mode("append").format("delta").saveAsTable(target_table)

    elif load_type == "delta":
        if not DeltaTable.isDeltaTable(self.spark, target_table):
            self.logger.info(f"📦 Creating discovery table `{target_table}`.")
            df_sanitized.write.mode("overwrite").format("delta").saveAsTable(target_table)
        else:
            primary_key = self.env_config.get("primary_key_details", {}).get(table)
            if not primary_key:
                self.logger.warning(f"⚠️ Primary key not defined for {table}. Using all columns as merge key.")
                primary_key = ",".join([c for c in df_sanitized.columns])
            self.logger.info(f"🔀 Performing MERGE on discovery table {target_table} with key: {primary_key}")
            self.delta_merge(target_table, df_sanitized, primary_key)

    self.watermark.update_watermark(self.watermark_table, table, initial_start_time)
    self.logger.info(f"🆙 Watermark updated for table: {table} to {initial_start_time}")

    end_time = JobInfo.get_current_utc_ts()
    job_info_dict = JobInfo.get_job_info(
        spark_context=sc,
        start_time=start_time,
        end_time=end_time,
        data_source_name=self.env_config.get("application_name"),
        source_configs=source_conf,
        source_schema=source_conf['database'],
        source_table=table,
        catalog_name=dest_conf['catalog'],
        dest_schema=dest_conf['raw_schema'],
        dest_table=table.lower(),
        dest_volume_name=dest_conf['volume_path'],
        dest_volume_path=volume_path,
        record_processed=record_processed,
        record_count=record_processed,
        status=Constants.SUCCEEDED,
        job_info_dict=job_info_dict,
        last_watermark=last_watermark_str,
        new_watermark=initial_start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    JobInfo.load_job_info(self.spark, sc, job_history_conf, job_info_dict)
    self.logger.info(f"📘 Job metadata saved for table: {table}")

    self.load_report.append({
        "table_name": table,
        "load_mode": load_type.upper(),
        "record_count": record_processed
    })
