def process_table(self, table, current_watermark, last_watermark):
    self.logger.info(f"Process Table Function Started for \n Table {table} \n Current Watermark {current_watermark} \n Last Watermark {last_watermark}")
    sc = self.spark.sparkContext
    num_partitions = 10
    job_info_dict = {}
    start_time = JobInfo.get_current_utc_ts()
    domain = self.source_conf.get('domain')

    load_type = self.params.load_type
    self.logger.info(f"🚀 Starting processing for table: {table} with load_type: {load_type}")

    last_watermark_str = "NA"
    target_table = f"{self.dest_conf['catalog']}.{self.dest_conf['discovery_schema']}.{table}"
    query = self._prepare_query(table, load_type, current_watermark, last_watermark)

    self.logger.info(f"🧾 SQL Query: {query}")

    raw_pk = self.primary_key_map.get(table)
    pk_list = [col.strip() for col in raw_pk.split(",")] if raw_pk else []
    partition_key = pk_list[0] if pk_list else None
    modification_column = self.mod_column_map.get(table, "ModifiedTime")
    # no longer using modification_column for partitioning
    min_id = max_id = None

    
    if partition_key:
        self.logger.info(f"🔑 Primary keys for `{table}`: {pk_list} — using `{partition_key}` for partitioning")
        try:
            if load_type == "delta":
                ts_format = "YYYY-MM-DD HH24:MI:SS"
                last_wm_str = last_watermark.strftime('%Y-%m-%d %H:%M:%S')
                curr_wm_str = current_watermark.strftime('%Y-%m-%d %H:%M:%S')
                bounds_query = f"""
                (SELECT MIN({partition_column}) AS min_id, MAX({partition_column}) AS max_id
                 FROM {self.source_schema.upper()}.{table.upper()}
                 WHERE {modification_column} > TO_TIMESTAMP('{last_wm_str}', '{ts_format}')
                   AND {modification_column} <= TO_TIMESTAMP('{curr_wm_str}', '{ts_format}')) bounds_alias
                """
            else:
                bounds_query = f"""
                    (SELECT MIN({partition_column}) AS min_id, MAX({partition_column}) AS max_id
                     FROM {self.source_schema}.{table}) bounds_alias
                """

            bounds_df = MSSQLConnector.reader(
                self.spark, self.source_conf['host'], self.source_conf['port'],
                self.source_conf['database'], bounds_query,
                self.username, self.password
            )
            row = bounds_df.collect()[0]
            min_id = row["min_id"]
            max_id = row["max_id"]
            self.logger.info(f"📊 Partition bounds for `{table}`: min={min_id}, max={max_id}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to fetch bounds for partitioning: {e}")
    else:
        self.logger.info(f"🔕 No primary key defined for `{table}`")

    try:
        if min_id and max_id and min_id != max_id:
            self.logger.info(f"🚀 Using partitioned read on `{partition_key}`: {min_id} -> {max_id}")
            _df = MSSQLConnector.reader(
                spark_session=self.spark,
                hostname=self.source_conf['host'],
                port=self.source_conf['port'],
                database=self.source_conf['database'],
                table=query,
                username=self.username,
                password=self.password,
                partition_column=partition_key,
                lower_bound=min_id,
                upper_bound=max_id,
                num_partitions=num_partitions,
                fetch_size=100000
            )
        else:
            _df = MSSQLConnector.reader(
                self.spark, self.source_conf['host'], self.source_conf['port'], self.source_conf['database'],
                query, self.username, self.password
            )

        schema = _df.schema
        columns_to_include = [field.name for field in schema if field.dataType.simpleString() != 'binary']
        df = _df.select(*columns_to_include)

    except Exception as e:
        self.logger.error(f"❌ Failed to read from source for table {table}: {e}")
        return

    df = df.repartition(num_partitions)
    volume_path = self.watermark.latest_volume_path(self.dest_conf['volume_path'], table, current_watermark)
    df = df.withColumn("load_datetimestamp", lit(current_watermark))
    record_processed = df.count()

    if record_processed == 0:
        self.logger.warning(f"⚠️ No records fetched from source for table: {table}")
        return

    self.logger.info(f"✅ {record_processed} records fetched for table: {table}")
    self.write_raw_data(df, volume_path, self.dest_conf['file_format'], self.dest_conf['write_mode'])

    df_sanitized = Transform.sanitize_cols(df)
    self._write_to_discovery_layer(df_sanitized, table, target_table, load_type)

    self.watermark.update_watermark(self.watermark_table, table, current_watermark)
    self.logger.info(f"🆙 Watermark updated for table: {table} to {current_watermark}")

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
        last_watermark=last_watermark_str,
        new_watermark=current_watermark.strftime("%Y-%m-%d %H:%M:%S")
    )
    JobInfo.load_job_info(self.spark, sc, self.job_history_conf, job_info_dict)
    self.logger.info(f"📘 Job metadata saved for table: {table}")

    self.load_report.append({
        "table_name": table,
        "load_mode": load_type.upper(),
        "record_count": record_processed
    })
