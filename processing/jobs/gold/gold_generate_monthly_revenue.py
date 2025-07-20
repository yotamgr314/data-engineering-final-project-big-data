from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import year, month, col, sum as spark_sum, avg, count, lit, when, expr
from pyspark.sql.functions import udf
from pyspark.sql.functions import row_number
import uuid

spark = SparkSession.builder
    .appName("Generate GOLD_DIM_MONTHLY_REVENUE")
    .config("spark.sql.catalog.hadoop_cat", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hadoop_cat.type", "hadoop")
    .config("spark.sql.catalog.hadoop_cat.warehouse", "s3a://gold/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()

ticket_prices = spark.read.format("iceberg").load("hadoop_cat.silver.ticket_prices")
booked_tickets = spark.read.format("iceberg").load("hadoop_cat.silver.booked_tickets")
agg_cancel_rate = spark.read.format("iceberg").load("hadoop_cat.silver.agg_cancelation_rate")

joined = ticket_prices.join(booked_tickets, ticket_prices.silver_flight_id == booked_tickets.booked_ticket_id, "inner")
joined = joined.withColumn("year", year("start_date")).withColumn("month", month("start_date"))

revenue_by_class = joined.groupBy("year", "month", "ticket_class").agg(spark_sum(col("ticket_price") + col("luggage_fee")).alias("class_revenue"))

w = Window.partitionBy("year", "month").orderBy(col("class_revenue").desc())
max_class = revenue_by_class.withColumn("row_num", row_number().over(w)).filter(col("row_num") == 1)

main_metrics = joined.groupBy("year", "month").agg(spark_sum(col("ticket_price") + col("luggage_fee")).alias("total_revenue"),avg("ticket_price").alias("average_ticket_price"))

main = main_metrics.join(max_class.select("year", "month", "ticket_class"), ["year", "month"], "left")
    .withColumnRenamed("ticket_class", "max_revenue_class")
    .join(agg_cancel_rate, ["year", "month"], "left")
    .withColumn("revenue_per_passenger", col("total_revenue") / lit(1))

uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
main = main.withColumn("revenue_id", uuid_udf())
main = main.withColumn("ingestion_time", expr("current_timestamp()"))

main.select("revenue_id", "max_revenue_class", "total_revenue", "revenue_per_passenger", "average_ticket_price", "cancelation_rate", "ingestion_time", "year", "month").writeTo("hadoop_cat.gold.dim_monthly_revenue").createOrReplace()

print("[SUCCESS] GOLD_DIM_MONTHLY_REVENUE generated successfuly!")
spark.stop()