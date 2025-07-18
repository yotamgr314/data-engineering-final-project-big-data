"""
Simple schema validation for Bronze tables.
Exits with code !=0 on failure (so Airflow marks task as failed).
"""
import sys
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("validate_bronze")
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    .config("spark.sql.catalog.minio.warehouse", "s3a://bronze")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

expected = {
    "bronze_airports_raw" : {"iata","city","country"},
    "bronze_aircrafts_raw": {"aircraft_id","model","capacity"},
    "bronze_flights_raw"  : {"flight_id","origin","destination","flight_date","departure_time"},
}

errors = []
for tbl, cols in expected.items():
    df = spark.table(f"minio.{tbl}")
    missing = cols.difference(set(df.columns))
    if missing:
        errors.append(f"{tbl}: missing {missing}")

if errors:
    for e in errors: print("❌", e)
    sys.exit(1)
print("✔ Bronze schema validation passed.")
