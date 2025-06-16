from delta.tables import DeltaTable

def merge_data(self, df, target_discovery_table, pk_columns, table_name):
    # Normalize columns: lowercase and underscore for both source and target
    target_cols = set([c.lower().replace(" ", "_") for c in self.get_table_schema(target_discovery_table)])
    df = df.select([col(c).alias(c.lower().replace(" ", "_")) for c in df.columns])
    source_cols = set(df.columns)

    # Log columns for debugging
    self.logger.info(f"Source DF columns: {sorted(source_cols)}")
    self.logger.info(f"Target Delta table columns: {sorted(target_cols)}")

    # Check for missing columns in source DF compared to target table
    missing_cols = target_cols - source_cols
    if missing_cols:
        self.logger.error(f"❌ Missing columns in source DF compared to target table `{table_name}`: {missing_cols}")
        return

    # Reorder columns in source DF to match target table (stable ordering)
    ordered_cols = [c for c in self.get_table_schema(target_discovery_table) if c.lower().replace(" ", "_") in source_cols]
    df = df.select([col(c.lower().replace(" ", "_")) for c in ordered_cols])

    # Normalize pk_columns
    pk_columns_norm = [c.lower().replace(" ", "_") for c in pk_columns]
    missing_pk = [c for c in pk_columns_norm if c not in df.columns]
    if missing_pk:
        self.logger.warning(f"⚠️ Merge keys not found in source df for `{table_name}`: {missing_pk}")
        return

    # Refresh Delta table metadata before merge
    try:
        self.spark.catalog.refreshTable(target_discovery_table)
    except Exception as e:
        self.logger.warning(f"⚠️ Could not refresh Delta table `{target_discovery_table}` metadata: {e}")

    try:
        delta_table = DeltaTable.forName(self.spark, target_discovery_table)
    except Exception as e:
        self.logger.error(f"❌ Delta table `{target_discovery_table}` not found or inaccessible: {e}")
        return

    # Create temp view for source to run join queries
    df.createOrReplaceTempView("source_temp")

    # Compute counts of matched (update candidates) rows
    merge_condition = " AND ".join([f"target.`{c}` = source.`{c}`" for c in pk_columns_norm])
    matched_count = self.spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {target_discovery_table} target
        INNER JOIN source_temp source
        ON {merge_condition}
    """).collect()[0]["cnt"]

    total_count = df.count()
    updated_count = matched_count
    inserted_count = total_count - updated_count

    # Prepare update and insert sets as dict
    update_set = {c: f"source.`{c}`" for c in df.columns}
    insert_set = {c: f"source.`{c}`" for c in df.columns}

    # Perform merge
    self.logger.info(f"🔄 Performing DeltaTable merge on `{target_discovery_table}` for `{table_name}`")
    delta_table.alias("target").merge(
        df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_set).execute()

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
        "record_count": total_count
    })
