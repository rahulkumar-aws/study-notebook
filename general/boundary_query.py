try:
    if table.upper() == "CERTS_DETAIL":
        # ✅ Hardcoded query for both delta and full load
        bounds_query = """
            SELECT
                CAST(570000000000 AS NUMBER(20,0)) AS MIN_ID,
                CAST(999999999999 AS NUMBER(20,0)) AS MAX_ID
            FROM dual
        """
        self.logger.info(f"🛠️ Using hardcoded bounds query for `{table}`: {bounds_query}")
    else:
        if load_type == "delta":
            self.logger.info(f"📥 Load Type: {load_type}")
            ts_format = "%Y-%m-%d %H:%M:%S"
            last_wm_str = last_watermark.strftime(ts_format)
            curr_wm_str = current_watermark.strftime(ts_format)

            bounds_query = f"""
                SELECT
                    CAST(MIN({partition_key}) AS NUMBER(20,0)) AS min_id,
                    CAST(MAX({partition_key}) AS NUMBER(20,0)) AS max_id
                FROM {self.source_schema.upper()}.{table.upper()} bounds_alias
                WHERE {modification_column} > TO_TIMESTAMP('{last_wm_str}', '{ts_format}')
                  AND {modification_column} <= TO_TIMESTAMP('{curr_wm_str}', '{ts_format}')
            """
            self.logger.info(f"📊 Delta Load bounds query: {bounds_query}")
        else:
            bounds_query = f"""
                SELECT
                    CAST(MIN({partition_key}) AS NUMBER(20,0)) AS min_id,
                    CAST(MAX({partition_key}) AS NUMBER(20,0)) AS max_id
                FROM {self.source_schema.upper()}.{table.upper()} bounds_alias
            """
            self.logger.info(f"📊 Full Load bounds query: {bounds_query}")

    # ✅ Common execution path
    bounds_df = OracleConnector.reader(
        self.spark,
        self.source_conf['host'],
        self.source_conf['port'],
        self.source_conf['database'],
        bounds_query,
        self.username,
        self.password
    )

    row = bounds_df.collect()[0]
    min_id = row["MIN_ID"]
    max_id = row["MAX_ID"]
    self.logger.info(f"✅ Partition bounds for `{table}`: min={min_id}, max={max_id}")

except Exception as e:
    self.logger.warning(f"⚠️ Failed to fetch bounds for partitioning of `{table}`: {e}")
