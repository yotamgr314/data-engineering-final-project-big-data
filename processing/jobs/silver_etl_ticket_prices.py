import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# הגדרת Spark עם חיבור ל‑MinIO
spark = (
    SparkSession.builder
        .appName("silver_etl_ticket_prices")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")  # הגדרת המפתח
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")  # הגדרת הסוד
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # הגדרת ה‑endpoint של MinIO
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # הגדרת שימוש ב‑path style
        .getOrCreate()
)

# Load the Bronze data
bronze_ticket_prices_df = spark.read.format("iceberg").load("s3a://bronze/bronze_ticket_prices")

# Perform transformations
silver_ticket_prices_df = bronze_ticket_prices_df.select(
    col("price_id"),
    col("flight_id").alias("silver_flight_id"),
    col("class"),
    col("price"),
    col("luggage_fee"),
    col("start_date"),
    col("end_date"),
    col("actual"),
    col("ingestion_time")
)

# Write to Silver layer
silver_ticket_prices_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_TICKET_PRICES")

print("Silver Ticket Prices table created successfully!")
