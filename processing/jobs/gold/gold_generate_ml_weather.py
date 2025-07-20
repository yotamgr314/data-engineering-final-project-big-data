from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, count, avg, lit, udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_DIM_ML_WEATHER") \
    .config("spark.sql.catalog.silver_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.silver_cat.type", "hadoop") \
    .config("spark.sql.catalog.silver_cat.warehouse", "s3a://silver/") \
    .config("spark.sql.catalog.gold_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gold_cat.type", "hadoop") \
    .config("spark.sql.catalog.gold_cat.warehouse", "s3a://gold/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

weather_summary = spark.read.format("iceberg").load("silver_cat.weather_summary")

weather_counts = weather_summary.groupBy("majority_weather_condition") \
    .agg(
        count("*").alias("number_of_most_common_condition"),
        avg("average_temperature").alias("average_temperature"),
        avg("average_humidity").alias("average_humidity"),
        avg("average_wind_speed").alias("average_wind_speed"),
        avg("average_wind_direction").alias("average_wind_direction")
    )

result = weather_counts.withColumn("weather_id", generate_uuid()) \
                       .withColumn("average_height", lit(None).cast("double")) \
                       .withColumn("ingestion_time", current_timestamp())

result.select(
    "weather_id", "majority_weather_condition", "number_of_most_common_condition",
    "average_height", "average_temperature", "average_humidity",
    "average_wind_speed", "average_wind_direction", "ingestion_time"
).write.format("iceberg").mode("overwrite").saveAsTable("gold_cat.dim_ml_weather")

print("[SUCCESS] GOLD_DIM_MONTHLY_DELAYS generated successfuly!")
spark.stop()