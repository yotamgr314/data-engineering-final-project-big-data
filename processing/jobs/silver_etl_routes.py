import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# הגדרת Spark עם חיבור ל‑MinIO
spark = (
    SparkSession.builder
        .appName("silver_etl_routes")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")  # הגדרת המפתח
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")  # הגדרת הסוד
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # הגדרת ה‑endpoint של MinIO
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # הגדרת שימוש ב‑path style
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
