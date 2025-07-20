from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, avg, current_timestamp, udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_DIM_ML_ROUTES") \
    .config("spark.sql.catalog.silver_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.silver_cat.type", "hadoop") \
    .config("spark.sql.catalog.silver_cat.warehouse", "s3a://gold/") \
    .config("spark.sql.catalog.gold_cat", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gold_cat.type", "hadoop") \
    .config("spark.sql.catalog.gold_cat.warehouse", "s3a://gold/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

routes = spark.read.format("iceberg").load("silver_cat.routes")
flights = spark.read.format("iceberg").load("silver_cat.flights")

joined = routes.join(flights, "routes_id", "inner") \
    .withColumn("time_of_flight", expr("(unix_timestamp(scheduled_arrival) - unix_timestamp(scheduled_departure)) / 60.0")) \
    .groupBy("routes_id", "airport_origin", "airport_destination", "distance_km") \
    .agg(avg("time_of_flight").alias("time_of_flight"))

result = joined.withColumn("route_id", generate_uuid()) \
               .withColumn("ingestion_time", current_timestamp())

result.select("route_id", "airport_origin", "airport_destination",
              "distance_km", "time_of_flight", "ingestion_time") \
      .write.format("iceberg").mode("overwrite").saveAsTable("gold_cat.dim_ml_routes")

print("[SUCCESS] GOLD_DIM_ML_ROUTES generated successfuly!")
spark.stop()