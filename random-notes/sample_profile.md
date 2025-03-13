### **📌 Databricks Notebook Code for Profiling MSSQL & Oracle Hosts**
This code will:  
✅ **Loop through 10 MSSQL & 4 Oracle hosts**.  
✅ **Collect table stats (row count, column count, schema details)**.  
✅ **Save results into a Delta Table (`table_profile_stats`)**.  
✅ **Run one by one for each host to avoid overload.**  

---

## **✅ Step 1: Define Database Hosts**
Modify these **with your actual connection details**:
```python
# ✅ MSSQL Hosts
mssql_hosts = [
    "jdbc:sqlserver://mssql-host-1:1433;databaseName=db1",
    "jdbc:sqlserver://mssql-host-2:1433;databaseName=db2",
    "jdbc:sqlserver://mssql-host-3:1433;databaseName=db3"
    # Add remaining MSSQL hosts
]

# ✅ Oracle Hosts
oracle_hosts = [
    "jdbc:oracle:thin:@oracle-host-1:1521:orcl",
    "jdbc:oracle:thin:@oracle-host-2:1521:orcl"
    # Add remaining Oracle hosts
]

# ✅ Common JDBC properties
mssql_properties = {"user": "mssql_user", "password": "mssql_password", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
oracle_properties = {"user": "oracle_user", "password": "oracle_password", "driver": "oracle.jdbc.OracleDriver"}

# ✅ Target Delta Table
delta_table = "dasp_system.na_etl_dev.table_profile_stats"
```

---

## **✅ Step 2: Define Function to Collect & Save Stats**
```python
from pyspark.sql import SparkSession
from modules.utils import get_tables_from_schema, get_row_count, get_column_stats, clean_jdbc_properties

def profile_host(spark, jdbc_url, jdbc_properties, db_type, schema):
    """
    Profiles all tables from a given database host and saves stats to a Delta Table.
    """
    print(f"🔍 Profiling host: {jdbc_url} | DB Type: {db_type}")

    # ✅ Fetch tables from schema
    tables = get_tables_from_schema(
        spark=spark,
        jdbc_url=jdbc_url,
        jdbc_properties=jdbc_properties,
        schema=schema,
    )

    for table in tables:
        try:
            # ✅ Get row count & column stats
            row_count = get_row_count(spark, jdbc_url, jdbc_properties, schema, table, db_type)
            column_stats = get_column_stats(spark, jdbc_url, jdbc_properties, schema, table, db_type)

            # ✅ Prepare data for writing
            table_profile = {
                "host": jdbc_url,
                "database_type": db_type,
                "schema": schema,
                "table_name": table,
                "row_count": row_count,
                "column_count": column_stats["column_count"],
                "schema_details": str(column_stats["schema_details"]),
                "profiling_date": datetime.now()
            }

            # ✅ Convert to DataFrame & Append to Delta Table
            df = spark.createDataFrame([table_profile])
            df.write.format("delta").mode("append").saveAsTable(delta_table)

            print(f"✅ Profiled {table} from {jdbc_url} and saved to Delta")

        except Exception as e:
            print(f"❌ Failed to profile {table} from {jdbc_url}: {str(e)}")
```

---

## **✅ Step 3: Run Profiling for All Hosts**
```python
spark = SparkSession.builder.appName("DBProfiling").enableHiveSupport().getOrCreate()

# ✅ Profile MSSQL Hosts
for host in mssql_hosts:
    profile_host(spark, host, mssql_properties, "mssql", "dbo")  # Change schema if needed

# ✅ Profile Oracle Hosts
for host in oracle_hosts:
    profile_host(spark, host, oracle_properties, "oracle", "HR")  # Change schema if needed
```

---

## **📌 Expected Delta Table Schema (`table_profile_stats`)**
| host | database_type | schema | table_name | row_count | column_count | schema_details | profiling_date |
|------|--------------|--------|------------|-----------|--------------|----------------|----------------|
| `mssql-host-1` | `mssql` | `dbo` | `employees` | 100000 | 12 | `{"id": "INT", "name": "VARCHAR"}` | `2025-03-13 10:00:00` |
| `oracle-host-2` | `oracle` | `HR` | `payroll` | 50000 | 8 | `{"emp_id": "INT", "salary": "DECIMAL"}` | `2025-03-13 10:05:00` |

---

## **📌 Final Steps: Run in Databricks**
Paste the code **into a Databricks notebook cell** and run it:
```python
# Run profiling job
```

---

## **🚀 Done! Now All Hosts Are Profiled & Stored in Delta.**
Let me know if you need refinements! 🔥🚀
