from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import year, month, col, count, sum as spark_sum, avg
import uuid

spark = SparkSession.builder \
    .appName("Generate GOLD_DIM_MONTHLY_FLIGHT_INFO") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "s3a://gold/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

flights_df = spark.read.format("iceberg").load("s3a://silver/")
boarding_df = spark.read.format("iceberg").load("hadoop_cat.silver.boarding_summary")

joined_df = flights_df.join(boarding_df, on="flight_id", how="inner")
joined_df = joined_df.withColumn("year", year("flight_date")).withColumn("month", month("flight_date"))

monthly_stats = joined_df.groupBy("year", "month") \
    .agg(
        count("flight_id").alias("total_flights"),
        spark_sum("passenger_count").alias("total_passengers_number"),
        avg("passenger_count").alias("average_passengers_number_on_flight")
    ) \
    .withColumn("flights_info_id", lit(None).cast("string")) \
    .withColumn("ingestion_time", current_timestamp())

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())
monthly_stats = monthly_stats.withColumn("flights_info_id", generate_uuid())

monthly_stats.select(
    "flights_info_id", "total_flights", "total_passengers_number",
    "average_passengers_number_on_flight", "ingestion_time", "year", "month"
).writeTo("hadoop_cat.gold.dim_monthly_flight_info").createOrReplace()

print("[SUCCESS] GOLD_DIM_MONTHLY_FLIGHT_INFO generated successfuly!")
spark.stop()