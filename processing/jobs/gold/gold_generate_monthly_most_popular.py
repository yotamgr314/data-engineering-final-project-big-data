from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, month, current_timestamp
from pyspark.sql.functions import row_number, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_DIM_MONTHLY_MOST_POPULAR") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "s3a://gold/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

booked = spark.read.format("iceberg").load("hadoop_cat.silver.booked_tickets")
flights = spark.read.format("iceberg").load("hadoop_cat.silver.flights")
routes = spark.read.format("iceberg").load("hadoop_cat.silver.routes")

joined = booked.join(flights, booked.booked_ticket_id == flights.flight_id, "inner") \
    .join(routes, flights.routes_id == routes.routes_id, "inner") \
    .withColumn("year", year("booking_date")) \
    .withColumn("month", month("booking_date"))

origin_counts = joined.groupBy("year", "month", "airport_origin") \
    .agg(count("*").alias("cnt"))
origin_window = Window.partitionBy("year", "month").orderBy(col("cnt").desc())
most_origin = origin_counts.withColumn("row", row_number().over(origin_window)) \
    .filter("row = 1").select("year", "month", col("airport_origin").alias("most_popular_origin"))

dest_counts = joined.groupBy("year", "month", "airport_destination") \
    .agg(count("*").alias("cnt"))
dest_window = Window.partitionBy("year", "month").orderBy(col("cnt").desc())
most_dest = dest_counts.withColumn("row", row_number().over(dest_window)) \
    .filter("row = 1").select("year", "month", col("airport_destination").alias("most_popular_destination"))

most_method = joined.withColumn("method", lit("online")) \
    .groupBy("year", "month", "method").agg(count("*").alias("cnt"))
method_window = Window.partitionBy("year", "month").orderBy(col("cnt").desc())
most_method = most_method.withColumn("row", row_number().over(method_window)) \
    .filter("row = 1").select("year", "month", col("method").alias("most_popular_order_method"))

result = most_origin \
    .join(most_dest, ["year", "month"]) \
    .join(most_method, ["year", "month"]) \
    .withColumn("metrics_id", generate_uuid()) \
    .withColumn("ingestion_time", current_timestamp())

result.select(
    "metrics_id", "most_popular_origin", "most_popular_destination",
    "most_popular_order_method", "ingestion_time", "year", "month"
).writeTo("hadoop_cat.gold.dim_monthly_most_popular").createOrReplace()

print("[SUCCESS] GOLD_DIM_MONTHLY_MOST_POPULAR generated successfuly!")
spark.stop()