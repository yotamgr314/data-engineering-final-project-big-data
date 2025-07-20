from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, udf
from pyspark.sql.types import StringType
import uuid

generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

spark = SparkSession.builder \
    .appName("Generate GOLD_FACT_ML_TRAINING") \
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

flights = spark.read.format("iceberg").load("silver_cat.flights")
events = spark.read.format("iceberg").load("silver_cat.flights_events")
routes_dim = spark.read.format("iceberg").load("gold_cat.dim_ml_routes")
weather_dim = spark.read.format("iceberg").load("gold_cat.dim_ml_weather")
boarding_dim = spark.read.format("iceberg").load("gold_cat.dim_ml_boarding_data")

departures = events.filter(col("event_type") == "actual_departure")

joined = flights.join(departures, "flight_id", "inner") \
    .withColumn("delay_in_minutes", expr("(unix_timestamp(event_time) - unix_timestamp(scheduled_departure)) / 60.0"))

joined = joined.join(routes_dim, "routes_id", "left")

weather_row = weather_dim.limit(1).select("weather_id").collect()
weather_id_value = weather_row[0]["weather_id"] if weather_row else None
joined = joined.withColumn("weather_id", lit(weather_id_value))

boarding_row = boarding_dim.limit(1).select("boarding_data_id").collect()
boarding_id_value = boarding_row[0]["boarding_data_id"] if boarding_row else None
joined = joined.withColumn("boarding_data_id", lit(boarding_id_value))

result = joined.withColumn("training_set_id", generate_uuid()) \
               .withColumn("ingestion_time", current_timestamp())

result.select(
    "training_set_id", "weather_id", "route_id", "boarding_data_id",
    "delay_in_minutes", "ingestion_time"
).write.format("iceberg").mode("overwrite").saveAsTable("gold_cat.fact_ml_training")

print("[SUCCESS] GOLD_DIM_MONTHLY_FLIGHT_INFO generated successfuly!")
spark.stop()