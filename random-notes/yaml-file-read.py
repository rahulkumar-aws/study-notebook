# naanalytics_dataloader/tables_list.py
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils  # provided on clusters
import argparse, yaml, os, json

def _read_text(path: str) -> str:
    # Support both Workspace Files (/Workspace/...) and DBFS (dbfs:/...)
    if path.startswith("dbfs:/"):
        local = "/dbfs/" + path[len("dbfs:/"):]
        with open(local, "r") as f:
            return f.read()
    else:
        with open(path, "r") as f:
            return f.read()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml", required=True, help="Path to YAML with list: tables: [..]")
    parser.add_argument("--key", default="tables", help="YAML key holding the table list")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    text = _read_text(args.yaml)
    data = yaml.safe_load(text) or {}
    tables = data.get(args.key, []) or []

    # keep only non-empty strings
    tables = [t.strip() for t in tables if isinstance(t, str) and t.strip()]

    # optional: guard against huge payloads (~48KiB limit)
    payload = json.dumps(tables)
    if len(payload.encode("utf-8")) > 48000:
        raise ValueError(f"tables list too large ({len(payload)} bytes)")

    # publish to downstream tasks
    dbutils.jobs.taskValues.set(key="tables", value=tables)
    print(f"[tables_list] published {len(tables)} tables")

if __name__ == "__main__":
    main()
