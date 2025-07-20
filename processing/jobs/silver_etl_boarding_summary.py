import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

# הגדרת Spark עם חיבור ל‑MinIO
spark = (
    SparkSession.builder
        .appName("silver_etl_boarding_summary")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")  # הגדרת המפתח
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")  # הגדרת הסוד
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # הגדרת ה‑endpoint של MinIO
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # הגדרת שימוש ב‑path style
        .getOrCreate()
)

# Load the Bronze data
bronze_boarding_events_df = spark.read.format("iceberg").load("s3a://bronze/bronze_boarding_events_raw")

# Perform transformations
boarding_summary_df = bronze_boarding_events_df.filter(col("event_type") == "passenger_scanned") \
    .groupBy("flight_id").agg(
        count("event_type").alias("passenger_count"),
        sum("baggage_weight").alias("total_baggage_weight")
    )

boarding_summary_df = boarding_summary_df.withColumn(
    "avg_baggage_per_passenger", col("total_baggage_weight") / col("passenger_count")
)

# Write to Silver layer
boarding_summary_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_BOARDING_SUMMARY")

print("Silver Boarding Summary table created successfully!")
