from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_MONTHLY_AGGREGATES") \
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

agg = joined.select(
    col("year"), col("month"),
    col("total_flights"),
    col("total_revenue"),
    col("average_delay"),
    col("most_popular_destination"),
    col("most_popular_order_method")
).withColumn("aggregate_id", generate_uuid()) \
 .withColumn("ingestion_time", current_timestamp())

agg.select(
    "aggregate_id", "year", "month", "total_flights", "total_revenue",
    "average_delay", "most_popular_destination", "most_popular_order_method",
    "ingestion_time"
).writeTo("hadoop_cat.gold.monthly_aggregates").createOrReplace()

print("[SUCCESS] GOLD_MONTHLY_AGGREGATES generated successfuly!")
spark.stop()