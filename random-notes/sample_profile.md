### **📌 Updated Code: Separate JDBC Properties for MSSQL & Oracle**
Now, the **JDBC properties will be stored separately** for MSSQL & Oracle instead of inside each host entry.  
This keeps the **host list clean** and makes it **easier to manage authentication settings**.

---

## **✅ Step 1: Define Database Connections & JDBC Properties**
```python
from pyspark.sql import SparkSession
from datetime import datetime
import logging

# ✅ Initialize Spark session in Databricks
spark = SparkSession.builder.appName("DBProfiling").enableHiveSupport().getOrCreate()

# ✅ MSSQL Hosts (without credentials inside)
mssql_hosts = [
    {"project": "project1_prod", "jdbc_url": "jdbc:sqlserver://mssql-host-1:1433;databaseName=db1"},
    {"project": "project2_UAT", "jdbc_url": "jdbc:sqlserver://mssql-host-2:1433;databaseName=db2"}
]

# ✅ Oracle Hosts (without credentials inside)
oracle_hosts = [
    {"project": "project1_prod", "jdbc_url": "jdbc:oracle:thin:@oracle-host-1:1521:orcl"},
    {"project": "project2_UAT", "jdbc_url": "jdbc:oracle:thin:@oracle-host-2:1521:orcl"}
]

# ✅ MSSQL JDBC Properties (Shared for all MSSQL hosts)
mssql_properties = {
    "user": "mssql_user",
    "password": "mssql_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# ✅ Oracle JDBC Properties (Shared for all Oracle hosts)
oracle_properties = {
    "user": "oracle_user",
    "password": "oracle_password",
    "driver": "oracle.jdbc.OracleDriver"
}

# ✅ Target Delta Table
delta_table = "dasp_system.na_etl_dev.table_profile_stats"

# ✅ Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
```

---

## **✅ Step 2: Utility Functions for Table Stats**
```python
def get_tables_from_schema(spark, jdbc_url, jdbc_properties, schema):
    """Fetches tables from the schema, using INFORMATION_SCHEMA for MSSQL and Oracle."""
    logger.info(f"🔍 Getting tables from schema `{schema}` for {jdbc_url}...")

    if "sqlserver" in jdbc_url:
        query = f"(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE') AS table_list"
    elif "oracle" in jdbc_url:
        query = f"(SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = '{schema}') AS table_list"
    else:
        raise ValueError("Unsupported database type!")

    df_tables = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
    all_tables = [row["TABLE_NAME"] for row in df_tables.collect()]
    
    logger.info(f"📋 Tables found in schema `{schema}`: {all_tables}")
    return all_tables

def get_row_count(spark, jdbc_url, jdbc_properties, schema, table_name):
    """Fetch row count for a given table using database-specific methods."""
    logger.info(f"📊 Getting row count for `{schema}.{table_name}` on {jdbc_url}...")

    if "sqlserver" in jdbc_url:
        query = f"(SELECT SUM(row_count) AS row_count FROM sys.dm_db_partition_stats WHERE object_id = OBJECT_ID('{schema}.{table_name}') AND index_id IN (0,1)) AS row_stats"
    elif "oracle" in jdbc_url:
        query = f"(SELECT NUM_ROWS AS row_count FROM ALL_TABLES WHERE TABLE_NAME = '{table_name}' AND OWNER = '{schema}') AS row_stats"
    else:
        raise ValueError("Unsupported database type!")

    df_row_count = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
    row_count = df_row_count.collect()[0]["row_count"]
    
    return row_count

def get_column_stats(spark, jdbc_url, jdbc_properties, schema, table_name):
    """Fetch column count and schema details for a table."""
    logger.info(f"📊 Getting column details for `{schema}.{table_name}`...")

    if "sqlserver" in jdbc_url:
        query = f"(SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}') AS schema_info"
    elif "oracle" in jdbc_url:
        query = f"(SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '{table_name}' AND OWNER = '{schema}') AS schema_info"
    else:
        raise ValueError("Unsupported database type!")

    df_schema = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_properties)
    schema_details = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in df_schema.collect()}

    return {"column_count": len(schema_details), "schema_details": schema_details}
```

---

## **✅ Step 3: Function to Profile a Host**
```python
def profile_host(spark, host_info, db_type, schema):
    """
    Profiles all tables from a given database host and saves stats to a Delta Table.
    
    Args:
    - spark (SparkSession): Active Spark session.
    - host_info (dict): Dictionary containing project name, JDBC URL, username, and password.
    - db_type (str): Database type ("mssql" or "oracle").
    - schema (str): Schema name.
    """
    project_name = host_info["project"]
    jdbc_url = host_info["jdbc_url"]

    # ✅ Define JDBC properties separately for MSSQL & Oracle
    if db_type == "mssql":
        jdbc_properties = {
            "user": host_info["user"],
            "password": host_info["password"],
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "encrypt": "true",
            "trustServerCertificate": "true",
            "loginTimeout": "30"
        }
    elif db_type == "oracle":
        jdbc_properties = {
            "user": host_info["user"],
            "password": host_info["password"],
            "driver": "oracle.jdbc.OracleDriver",
            "oracle.jdbc.timezoneAsRegion": "false"
        }
    else:
        raise ValueError(f"❌ Unsupported database type: {db_type}")

    print(f"🔍 Profiling host: {jdbc_url} | Project: {project_name} | DB Type: {db_type}")

    tables = get_tables_from_schema(spark, jdbc_url, jdbc_properties, schema)

    for table in tables:
        try:
            row_count = get_row_count(spark, jdbc_url, jdbc_properties, schema, table)
            column_stats = get_column_stats(spark, jdbc_url, jdbc_properties, schema, table)

            table_profile = {
                "project": project_name,
                "host": jdbc_url,
                "database_type": db_type,
                "schema": schema,
                "table_name": table,
                "row_count": row_count,
                "column_count": column_stats["column_count"],
                "schema_details": str(column_stats["schema_details"]),
                "profiling_date": datetime.now()
            }

            df = spark.createDataFrame([table_profile])
            df.write.format("delta").mode("append").saveAsTable(delta_table)

            print(f"✅ Profiled {table} from {jdbc_url} (Project: {project_name}) and saved to Delta")

        except Exception as e:
            print(f"❌ Failed to profile {table} from {jdbc_url} (Project: {project_name}): {str(e)}")

```

---

## **✅ Step 4: Run Profiling for All Hosts**
```python
# ✅ Profile MSSQL Hosts
for host in mssql_hosts:
    profile_host(spark, host["project"], host["jdbc_url"], mssql_properties, "mssql", "dbo")

# ✅ Profile Oracle Hosts
for host in oracle_hosts:
    profile_host(spark, host["project"], host["jdbc_url"], oracle_properties, "oracle", "HR")
```

---

## **📌 Why This Fix Works**
| **Issue Before** | **Fix Applied** |
|-----------------|----------------|
| ❌ Credentials were inside each host entry | ✅ Now stored **separately in `mssql_properties` & `oracle_properties`** |
| ❌ Hardcoded properties for MSSQL & Oracle | ✅ Automatically picks the correct JDBC driver |
| ❌ Repeating authentication details per host | ✅ Now **all hosts share common credentials per database type** |

---

## **🚀 Done! Now Credentials Are Stored Separately for MSSQL & Oracle.**
This makes the framework **cleaner and easier to manage**. Let me know if you need refinements! 🚀🔥
