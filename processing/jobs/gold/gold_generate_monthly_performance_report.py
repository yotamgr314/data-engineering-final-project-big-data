from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_FACT_MONTHLY_PERFORMANCE_REPORT") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "s3a://gold/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

flights_info = spark.read.format("iceberg").load("hadoop_cat.gold.dim_monthly_flight_info")
revenue = spark.read.format("iceberg").load("hadoop_cat.gold.dim_monthly_revenue")
delays = spark.read.format("iceberg").load("hadoop_cat.gold.dim_monthly_delays")
popular = spark.read.format("iceberg").load("hadoop_cat.gold.dim_monthly_most_popular")

joined = flights_info.join(revenue, ["year", "month"]) \
    .join(delays, ["year", "month"]) \
    .join(popular, ["year", "month"])

fact_df = joined.select(
    col("year"), col("month"),
    col("flights_info_id"),
    col("revenue_id"),
    col("delays_id"),
    col("metrics_id")
).withColumn("report_id", generate_uuid()) \
 .withColumn("ingestion_time", current_timestamp())

fact_df.select(
    "report_id", "revenue_id", "flights_info_id", "metrics_id", "delays_id",
    "year", "month", "ingestion_time"
).writeTo("hadoop_cat.gold.fact_monthly_performance_report").createOrReplace()

print("[SUCCESS] GOLD_FACT_MONTHLY_PERFORMANCE_REPORT generated successfuly!")
spark.stop()