ts_format = "YYYY-MM-DD HH24:MI:SS"
last_wm_str = last_watermark.strftime('%Y-%m-%d %H:%M:%S')
curr_wm_str = current_watermark.strftime('%Y-%m-%d %H:%M:%S')

query = f"""
    (SELECT * FROM {self.source_schema}.{table}
     WHERE {modification_column} > TO_TIMESTAMP('{last_wm_str}', '{ts_format}')
       AND {modification_column} <= TO_TIMESTAMP('{curr_wm_str}', '{ts_format}')) {table}_alias
"""
self.logger.info(f"📤 Delta query prepared for table: {table}")
self.logger.info(f"🧾 SQL Query: {query}")
