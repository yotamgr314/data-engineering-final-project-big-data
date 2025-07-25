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

expected_tables = [
    "bronze_boarding_events_raw",
    "bronze_booked_tickets_raw_streaming",
    "bronze_flight_events_raw",
    "bronze_flight_weather_raw_api",
    "bronze_flights_streaming.source",
    "bronze_registered_customers_streaming",
    "bronze_route_weather_points_static",
    "bronze_static_routes_raw",
    "bronze_ticket_events_raw_streaming",
    "bronze_ticket_prices"
]

print("✔ Bronze schema validation passed.")
