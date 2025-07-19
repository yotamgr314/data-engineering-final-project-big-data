from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("silver_etl_flight_events").getOrCreate()

# Load the Bronze data
bronze_flight_events_df = spark.read.format("iceberg").load("s3a://bronze/bronze_flight_events_raw")

# Perform transformations
silver_flight_events_df = bronze_flight_events_df.select(
    col("event_id").alias("flight_event_id"),
    col("flight_id"),
    col("event_type"),
    col("event_time"),
    col("delay_reason"),
    col("ingestion_time")
)

# Write to Silver layer
silver_flight_events_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_FLIGHTS_EVENTS")

print("Silver Flight Events table created successfully!")
