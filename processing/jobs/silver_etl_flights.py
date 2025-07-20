import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# הגדרת Spark עם חיבור ל‑MinIO
spark = (
    SparkSession.builder
        .appName("silver_etl_flights")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")  # הגדרת המפתח
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")  # הגדרת הסוד
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # הגדרת ה‑endpoint של MinIO
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # הגדרת שימוש ב‑path style
        .getOrCreate()
)

# Load the Bronze data
bronze_flights_df = spark.read.format("iceberg").load("s3a://bronze/bronze_flights_streaming_source")

# Perform transformations
silver_flights_df = bronze_flights_df.select(
    col("flight_id"),
    col("route_id").alias("routes_id"),
    col("max_passenger_capacity").alias("possible_passenger_amount"),
    col("schedualed_departure").alias("scheduled_departure"),
    col("schedualed_arrival").alias("scheduled_arrival"),
    col("max_lagguge_weight_capacity").alias("possible_luggage_weight_kg"),
    col("ingestion_time"),
)

# Write to Silver layer
silver_flights_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_FLIGHTS")

print("Silver Flights table created successfully!")
