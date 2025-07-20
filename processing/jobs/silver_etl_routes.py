import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
        .appName("silver_etl_routes")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")‑
        .getOrCreate()
)

# Load the Bronze data
bronze_routes_df = spark.read.format("iceberg").load("s3a://bronze/bronze_static_routes_raw")

# Perform transformations
silver_routes_df = bronze_routes_df.select(
    col("route_id").alias("routes_id"),
    col("airport_origin"),
    col("airport_destination"),
    col("latitude_origin"),
    col("longitude_origin"),
    col("latitude_destination"),
    col("longitude_destination"),
    col("distance_km"),
    col("ingestion_time")
)

# Write to Silver layer
silver_routes_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_ROUTES")

print("Silver Routes table created successfully!")
