from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, max, udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_DIM_ML_BOARDING_DATA") \
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

flights = spark.read.format("iceberg").load("silver_cat.flights")

agg = flights.agg(
    max("possible_passenger_amount").alias("max_passenger_capacity"),
    max("possible_luggage_weight").alias("max_luggage_weight_capacity")
)

result = agg.withColumn("boarding_data_id", generate_uuid()) \
            .withColumn("ingestion_time", current_timestamp())

result.select(
    "boarding_data_id", "max_passenger_capacity", "max_luggage_weight_capacity", "ingestion_time"
).write.format("iceberg").mode("overwrite").saveAsTable("gold_cat.dim_ml_boarding_data")

print("[SUCCESS] GOLD_DIM_ML_BOARDING_DATA generated successfuly!")
spark.stop()