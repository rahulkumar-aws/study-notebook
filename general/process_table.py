def process_table(self, table, current_watermark, last_watermark):
    self.logger.info(f"📌 Starting process for table: {table}")
    self.logger.info(f"Current Watermark: {current_watermark}, Last Watermark: {last_watermark}")
    sc = self.spark.sparkContext
    job_info_dict = {}
    start_time = JobInfo.get_current_utc_ts()
    domain = self.source_conf.get("domain", "local")

    if last_watermark is None:
        self.logger.warning("⚠️ No last watermark found. Switching to full_load mode.")
        load_type = "full_load"
    else:
        load_type = self.params.load_type
        self.logger.info(f"🔁 Using load_type: {load_type} based on config")

    target_table = f"{self.dest_conf['catalog']}.{self.dest_conf['discovery_schema']}.{table}"
    query = self._prepare_query(table, load_type, current_watermark, last_watermark)
    self.logger.info(f"🧾 Source Query Prepared:\n{query}")

    try:
        primary_key = self.primary_key_map.get(table)
        if load_type == "full_load" and primary_key:
            self.logger.info(f"📊 Attempting partitioned full-load using key `{primary_key}`")

            bounds_query = f"(SELECT MIN([{primary_key}]) AS min_id, MAX([{primary_key}]) AS max_id FROM {query[1:-len(f' AS {table}_alias')]}) AS bounds"

            bounds_df = MSSQLConnector.reader(
                spark_session=self.spark,
                hostname=self.source_conf['host'],
                port=self.source_conf['port'],
                database=self.source_conf['database'],
                table=bounds_query,
                username=self.username,
                password=self.password,
                domain=domain
            )
            row = bounds_df.collect()[0]
            min_id = row["min_id"]
            max_id = row["max_id"]

            if min_id is not None and max_id is not None and min_id != max_id:
                self.logger.info(f"✅ Partition bounds: {min_id} → {max_id}, using 4 partitions")
                df = MSSQLConnector.reader(
                    spark_session=self.spark,
                    hostname=self.source_conf['host'],
                    port=self.source_conf['port'],
                    database=self.source_conf['database'],
                    table=query,
                    username=self.username,
                    password=self.password,
                    domain=domain,
                    partition_column=primary_key,
                    lower_bound=min_id,
                    upper_bound=max_id,
                    num_partitions=4
                )
            else:
                self.logger.warning(f"⚠️ Invalid bounds for `{table}`. Switching to non-partitioned read.")
                df = MSSQLConnector.reader(
                    spark_session=self.spark,
                    hostname=self.source_conf['host'],
                    port=self.source_conf['port'],
                    database=self.source_conf['database'],
                    table=query,
                    username=self.username,
                    password=self.password,
                    domain=domain
                )
        else:
            df = MSSQLConnector.reader(
                spark_session=self.spark,
                hostname=self.source_conf['host'],
                port=self.source_conf['port'],
                database=self.source_conf['database'],
                table=query,
                username=self.username,
                password=self.password,
                domain=domain
            )

    except Exception as e:
        self.logger.error(f"❌ Failed to read from source for table {table}: {e}")
        return

    volume_path = self.watermark.latest_volume_path(self.dest_conf['volume_path'], table, current_watermark)
    df = df.withColumn("load_datetimestamp", lit(current_watermark))
    record_processed = df.count()
    if record_processed == 0:
        self.logger.warning(f"⚠️ No records fetched from source for table: {table}")
        return

    self.logger.info(f"✅ Total Records Fetched: {record_processed}")
    self.write_raw_data(df, volume_path, self.dest_conf['file_format'], self.dest_conf['write_mode'])

    df_sanitized = Transform.sanitize_cols(df)
    self._write_to_discovery_layer(df_sanitized, table, target_table, load_type)

    self.watermark.update_watermark(self.watermark_table, table, current_watermark)
    self.logger.info(f"🆙 Watermark updated for {table} to {current_watermark}")

    end_time = JobInfo.get_current_utc_ts()
    job_info_dict = JobInfo.get_job_info(
        spark_context=sc,
        start_time=start_time,
        end_time=end_time,
        data_source_name=self.env_config.get("application_name"),
        source_configs=self.source_conf,
        source_schema=self.source_conf['database'],
        source_table=table,
        catalog_name=self.dest_conf['catalog'],
        dest_schema=self.dest_conf['raw_schema'],
        dest_table=table.lower(),
        dest_volume_name=self.dest_conf['volume_path'],
        dest_volume_path=volume_path,
        record_processed=record_processed,
        record_count=record_processed,
        status=Constants.SUCCEEDED,
        job_info_dict=job_info_dict,
        last_watermark="NA" if last_watermark is None else last_watermark,
        new_watermark=current_watermark.strftime("%Y-%m-%d %H:%M:%S")
    )
    JobInfo.load_job_info(self.spark, sc, self.job_history_conf, job_info_dict)
    self.logger.info(f"📘 Job metadata saved for table: {table}")

    self.load_report.append({
        "table_name": table,
        "load_mode": load_type.upper(),
        "record_count": record_processed
    })
