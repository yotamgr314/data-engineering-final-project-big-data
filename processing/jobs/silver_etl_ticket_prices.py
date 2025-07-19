from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("silver_etl_ticket_prices").getOrCreate()

# Load the Bronze data
bronze_ticket_prices_df = spark.read.format("iceberg").load("s3a://bronze/bronze_ticket_prices")

# Perform transformations
silver_ticket_prices_df = bronze_ticket_prices_df.select(
    col("price_id"),
    col("flight_id").alias("silver_flight_id"),
    col("class"),
    col("price"),
    col("luggage_fee"),
    col("start_date"),
    col("end_date"),
    col("actual"),
    col("ingestion_time")
)

# Write to Silver layer
silver_ticket_prices_df.write.format("iceberg").mode("overwrite").save("s3a://silver/SILVER_TICKET_PRICES")

print("Silver Ticket Prices table created successfully!")
