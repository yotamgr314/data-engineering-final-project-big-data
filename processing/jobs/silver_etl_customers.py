import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# הגדרת Spark עם חיבור ל‑MinIO
spark = (
    SparkSession.builder
        .appName("silver_etl_customers")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")  # הגדרת המפתח
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")  # הגדרת הסוד
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # הגדרת ה‑endpoint של MinIO
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # הגדרת שימוש ב‑path style
        .getOrCreate()
)

# Load the Bronze data
bronze_customers_df = spark.read.format("iceberg").load("s3a://bronze/bronze_registered_customeres_streaming")

# Perform transformations
silver_customers_df = bronze_customers_df.select(
    col("customer_passport_id"),
    col("customer_first_name"),
    col("customer_last_name"),
    col("customer_date_of_birth"),
    col("passenger_nationality").alias("customer_nationality"),
    col("passenger_email").alias("customer_email"),
    col("customer_membership_tier").alias("customer_membership"),
    col("ingestion_time")
)

# Write to Silver layer
silver_customers_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_CUSTOMERS")

print("Silver Customers table created successfully!")
