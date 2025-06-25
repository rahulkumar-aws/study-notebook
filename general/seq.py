elif bool(join_by_servicerequestid_map) and table in join_by_servicerequestid_map:
    primary_key = join_by_servicerequestid_map.get(table)
    self.logger.info(f"🔗 Using join_by_servicerequestid_map for table `{table}` with key `{primary_key}`")

    # Always get watermark from Service_Request_Header
    srh_last_wm, srh_curr_wm = self.watermark.fetch_watermark(
        self.watermark_table, 'service_request_header',
        Constants.LAST_SUCCESS_RUNDATE,
        Constants.CURRENT_RUNDATE
    )

    self.logger.info(f"🔁 Join watermark range for Service_Request_Header: {srh_last_wm} -> {srh_curr_wm}")

    # MUST wrap SELECT in parentheses and alias it to avoid JDBC syntax error
    keys_query = f"""
        (
        SELECT src.{primary_key}
        FROM [{self.source_schema}].[{table}] AS src
        JOIN [{self.source_schema}].Service_Request_Header AS srh
            ON src.ServiceRequestId = srh.ServiceRequestId
        WHERE srh.ModifiedTime > '{srh_last_wm.strftime('%Y-%m-%d %H:%M:%S')}'
            AND srh.ModifiedTime <= '{srh_curr_wm.strftime('%Y-%m-%d %H:%M:%S')}'
        ) AS {table}_keys
    """

    self.logger.info(f"🗝️ Keys query: {keys_query}")
    key_df = MSSQLConnector.reader(
        self.spark, self.source_conf['host'], self.source_conf['port'],
        self.source_conf['database'], keys_query,
        self.username, self.password, domain
    )

    keys = [str(row[primary_key]) for row in key_df.collect()]
    if not keys:
        self.logger.info(f"📭 No records found to process for table: {table}")
        return

    formatted_keys = ",".join(f"'{k}'" for k in keys)
    query = f"(SELECT * FROM [{self.source_schema}].[{table}] WHERE {primary_key} IN ({formatted_keys})) AS {table}_alias"
    self.logger.info(f"📥 Final Delta Join Query for `{table}`: {query}")
