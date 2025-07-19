from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("silver_etl_flights").getOrCreate()

# Load the Bronze data
bronze_flights_df = spark.read.format("iceberg").load("s3a://bronze/bronze_flights_streaming_source")

# Perform transformations
silver_flights_df = bronze_flights_df.select(
    col("flight_id"),
    col("route_id").alias("routes_id"),
    col("max_passenger_capacity").alias("possible_passenger_amount"),
    col("schedualed_departure").alias("scheduled_departure"),
    col("schedualed_arrival").alias("scheduled_arrival"),
    col("max_lagguge_weight_capacity").alias("possible_luggage_weight_kg"),
    col("ingestion_time"),
)

# Write to Silver layer
silver_flights_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_FLIGHTS")

print("Silver Flights table created successfully!")
