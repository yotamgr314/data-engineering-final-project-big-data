from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, avg, min, max, expr, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_DIM_MONTHLY_DELAYS") \
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_cat.warehouse", "s3a://gold/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

flights = spark.read.format("iceberg").load("hadoop_cat.silver.flights")
events = spark.read.format("iceberg").load("hadoop_cat.silver.flights_events")
reasons = spark.read.format("iceberg").load("hadoop_cat.silver.agg_frequent_delay_reasons")

actual_departures = events.filter(col("event_type") == "actual_departure")

joined = flights.join(actual_departures, on="flight_id", how="inner") \
    .withColumn("delay_minutes", expr("CAST((event_time - scheduled_departure) AS long) / 60")) \
    .withColumn("year", year("flight_date")) \
    .withColumn("month", month("flight_date"))

delay_stats = joined.groupBy("year", "month").agg(
    avg("delay_minutes").alias("average_delay"),
    min("delay_minutes").alias("minimal_delay"),
    max("delay_minutes").alias("maximal_delay")
)

top_delay_reason = reasons \
    .join(events, reasons.silver_flight_event == events.flight_event_id, "inner") \
    .join(flights, events.flight_id == flights.flight_id, "inner") \
    .withColumn("year", year("flight_date")) \
    .withColumn("month", month("flight_date"))

windowSpec = Window.partitionBy("year", "month").orderBy(col("total_occurences").desc())
top_delay_reason = top_delay_reason.withColumn("row", expr("row_number() over (partition by year, month order by total_occurences desc)")) \
    .filter("row = 1") \
    .select("year", "month", col("delay_reason").alias("most_frequent_delay"))

final = delay_stats.join(top_delay_reason, ["year", "month"], "left") \
    .withColumn("delays_id", generate_uuid()) \
    .withColumn("ingestion_time", current_timestamp())

final.select(
    "delays_id", "most_frequent_delay", "average_delay",
    "minimal_delay", "maximal_delay", "ingestion_time", "year", "month"
).writeTo("hadoop_cat.gold.dim_monthly_delays").createOrReplace()

print("[SUCCESS] GOLD_DIM_MONTHLY_DELAYS generated successfuly!")
spark.stop()