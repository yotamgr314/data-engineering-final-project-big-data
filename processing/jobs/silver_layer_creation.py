from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, abs, round, when, sum, countDistinct, avg, count, row_number
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Silver Layer Transformation") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
    .getOrCreate()

# Define Bronze table paths (Iceberg tables stored in MinIO)
bronze_routes_raw = "spark_catalog.bronze.BRONZE_ROUTES_RAW"
bronze_flights = "spark_catalog.bronze.BRONZE_FLIGHTS"
bronze_ticket_prices = "spark_catalog.bronze.BRONZE_TICKET_PRICES"
bronze_booked_tickets = "spark_catalog.bronze.BRONZE_BOOKED_TICKETS_RAW"
bronze_flight_weather = "spark_catalog.bronze.BRONZE_FLIGHT_WEATHER_RAW_API"
bronze_registered_customers = "spark_catalog.bronze.BRONZE_REGISTERED_CUSTOMERS"
bronze_route_weather_points = "spark_catalog.bronze.BRONZE_ROUTE_WEATHER_POINTS"
bronze_boarding_events = "spark_catalog.bronze.BRONZE_BOARDING_EVENTS_RAW"
bronze_flight_events = "spark_catalog.bronze.BRONZE_FLIGHT_EVENTS_RAW"
bronze_ticket_events = "spark_catalog.bronze.BRONZE_TICKET_EVENTS_RAW"

# Load Bronze tables into DataFrames
df_routes_raw = spark.read.format("iceberg").load(bronze_routes_raw)
df_flights = spark.read.format("iceberg").load(bronze_flights)
df_ticket_prices = spark.read.format("iceberg").load(bronze_ticket_prices)
df_booked_tickets = spark.read.format("iceberg").load(bronze_booked_tickets)
df_flight_weather = spark.read.format("iceberg").load(bronze_flight_weather)
df_registered_customers = spark.read.format("iceberg").load(bronze_registered_customers)
df_route_weather_points = spark.read.format("iceberg").load(bronze_route_weather_points)
df_boarding_events = spark.read.format("iceberg").load(bronze_boarding_events)
df_flight_events = spark.read.format("iceberg").load(bronze_flight_events)
df_ticket_events = spark.read.format("iceberg").load(bronze_ticket_events)

print("✅ Bronze tables loaded successfully. Ready for transformation.")
# ==============================================
# Silver transforms
# ==============================================

# Create silver_routes by copying all fields and adding ingestion_time
silver_routes = df_routes_raw.withColumn("ingestion_time", current_timestamp())

# Write to silver layer
silver_routes.write.format("iceberg").mode("overwrite").save("spark_catalog.silver.SILVER_ROUTES")

print("✅ silver_routes created.")

from pyspark.sql.functions import col, to_date, current_timestamp


# Transform BRONZE_FLIGHTS to SILVER_FLIGHTS
silver_flights = df_flights.select(
    col("flight_id"),
    col("routes_id"),
    col("max_passenger_capacity").alias("possible_passenger_amount"),
    col("max_luggage_weight_capacity").alias("possible_luggage_weight"),
    to_date("scheduled_departure").alias("flight_date"),
    col("scheduled_departure"),
    col("scheduled_arrival"),
    current_timestamp().alias("ingestion_time")
)

# Save to Iceberg silver table
silver_flights.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_FLIGHTS")

print("✅ SILVER_FLIGHTS created successfully.")


from pyspark.sql.functions import col, current_timestamp

# Transform BRONZE_ROUTE_WEATHER_POINTS → SILVER_WAYPOINT_SAMPLING_WEATHER_POINTS
silver_waypoint_weather = df_route_weather.select(
    col("route_id"),
    col("way_point_number"),
    col("latitude").alias("point_latitude"),
    col("longitude").alias("point_longitude"),
    col("height").alias("point_height"),
    current_timestamp().alias("ingestion_time")
)

# Save to Silver table in Iceberg
silver_waypoint_weather.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_WAYPOINT_SAMPLING_WEATHER_POINTS")

print("✅ Created: SILVER_WAYPOINT_SAMPLING_WEATHER_POINTS successfully.")


# Transform BRONZE_TICKET_PRICES → SILVER_TICKET_PRICES
silver_ticket_prices = df_ticket_prices.select(
    col("price_id"),
    col("flight_id").alias("silver_flight_id"),
    col("price").alias("ticket_price"),
    col("luggage_fee"),
    col("ticket_class"),
    col("start_date"),
    col("end_date"),
    col("actual"),
    current_timestamp().alias("ingestion_time")
)

# Save to Iceberg Silver table
silver_ticket_prices.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_TICKET_PRICES")

print("✅ Created: SILVER_TICKET_PRICES successfully.")


# Transform BRONZE_REGISTERED_CUSTOMERS → SILVER_CUSTOMERS
silver_customers = df_registered_customers.select(
    "customer_passport_id",
    "customer_first_name",
    "customer_last_name",
    "customer_nationality",
    "customer_email",
    col("customer_membership_tier").cast("string").alias("customer_membership_tier"),
    "customer_date_of_birth",
    current_timestamp().alias("ingestion_time")
)

# Save to Iceberg Silver table
silver_customers.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_CUSTOMERS")

print("✅ Created: SILVER_CUSTOMERS successfully.")

# Transform BRONZE_FLIGHT_WEATHER_RAW_API → SILVER_FLIGHT_WEATHER
# Step 1: Join weather with route waypoints based on proximity
joined_weather_waypoints = df_flight_weather.crossJoin(df_route_weather) \
    .withColumn("distance", 
        abs(col("latitude") - col("latitude")) + abs(col("longitude") - col("longitude"))
    )

# Step 2: Keep closest waypoint per weather sample
window_spec = Window.partitionBy("weather_sample_id").orderBy("distance")
closest_matches = joined_weather_waypoints.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(col("rank") == 1)

# Step 3: Join with flights to get silver_flight_id (based on route_id match)
weather_with_flight = closest_matches.join(
    df_flights.select("flight_id", "routes_id"),
    closest_matches.route_id == df_flights.routes_id,
    how="inner"
)

# Step 4: Build final SILVER_FLIGHT_WEATHER DataFrame
silver_flight_weather = weather_with_flight.select(
    col("weather_sample_id"),
    col("flight_id").alias("silver_flight_id"),
    col("way_point_number").alias("waypoint_sampling_number"),
    col("current_weather_condition").alias("weather_condition"),
    "temperature",
    "humidity",
    "wind_speed",
    "wind_direction",
    F.to_date("sample_for_date").alias("sample_for_date"),
    current_timestamp().alias("ingestion_time")
)

# Step 5: Write to Iceberg
silver_flight_weather.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_FLIGHT_WEATHER")

print("✅ Created: SILVER_FLIGHT_WEATHER successfully.")


# Transform BRONZE_BOOKED_TICKETS_RAW → SILVER_BOOKED_TICKETS
silver_booked_tickets = df_booked_tickets.select(
    col("booked_ticket_id"),
    col("passenger_passport_id").alias("customer_passport_id"),
    col("ticket_class"),
    col("booking_date"),
    current_timestamp().alias("ingestion_time")
)

# Save to Iceberg Silver table
silver_booked_tickets.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_BOOKED_TICKETS")

print("✅ Created: SILVER_BOOKED_TICKETS successfully.")

# Transform BRONZE_FLIGHT_EVENTS_RAW → SILVER_FLIGHTS_EVENTS
silver_flight_events = df_flight_events.select(
    col("event_id").alias("flight_event_id"),
    col("flight_id"),
    col("event_type"),
    col("delay_reason"),
    col("event_time"),
    current_timestamp().alias("ingestion_time")
)

# Save to Iceberg Silver table
silver_flight_events.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_FLIGHTS_EVENTS")

print("✅ Created: SILVER_FLIGHTS_EVENTS successfully.")

# create SILVER_WEATHER_SUMMARY table
# Load silver flight weather if needed (only if not in memory)
df_silver_flight_weather = spark.read.format("iceberg").load("spark_catalog.silver.SILVER_FLIGHT_WEATHER")

# Average metrics per flight
weather_avg = df_silver_flight_weather.groupBy("silver_flight_id").agg(
    avg("temperature").alias("average_temperature"),
    avg("humidity").alias("average_humidity"),
    avg("wind_speed").alias("average_wind_speed"),
    avg("wind_direction").alias("average_wind_direction")
)

# Majority weather condition per flight
weather_condition_counts = df_silver_flight_weather.groupBy(
    "silver_flight_id", "weather_condition"
).agg(
    count("*").alias("condition_count")
)
window_spec = Window.partitionBy("silver_flight_id").orderBy(F.desc("condition_count"))
majority_condition = weather_condition_counts.withColumn(
    "rank", row_number().over(window_spec)
).filter(F.col("rank") == 1).drop("rank", "condition_count")

# Join averages and majority condition
weather_summary = weather_avg.join(
    majority_condition,
    on="silver_flight_id",
    how="inner"
)

# Final formatting and ingestion_time
silver_weather_summary = weather_summary.select(
    F.col("silver_flight_id").alias("flight_id"),
    F.round("average_temperature", 2).alias("average_temperature"),
    F.round("average_humidity", 2).alias("average_humidity"),
    F.round("average_wind_speed", 2).alias("average_wind_speed"),
    F.round("average_wind_direction", 2).alias("average_wind_direction"),
    F.col("weather_condition").alias("majority_weather_condition"),
    current_timestamp().alias("ingestion_time")
)

# Write to Iceberg
silver_weather_summary.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_WEATHER_SUMMARY")

print("✅ Created: SILVER_WEATHER_SUMMARY successfully.")


# Transform BRONZE_TICKET_EVENTS_RAW → SILVER_TICKET_EVENTS
silver_ticket_events = df_ticket_events.select(
    col("event_id"),
    col("booked_ticket_id"),
    col("event_type"),
    col("event_time"),
    current_timestamp().alias("ingestion_time")
)

# Save to Iceberg Silver table
silver_ticket_events.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_TICKET_EVENTS")

print("✅ Created: SILVER_TICKET_EVENTS successfully.")


# Create SILVER_AGG_CANCELATION_RATE table
# Count total booking and cancelation events from BRONZE_TICKET_EVENTS_RAW
agg_cancelation_df = df_ticket_events.groupBy().agg(
    F.sum(when(col("event_type") == "booked", 1).otherwise(0)).alias("total_booking"),
    F.sum(when(col("event_type") == "canceled", 1).otherwise(0)).alias("total_cancelation")
)

# Calculate cancelation rate (handle division by zero)
agg_cancelation_df = agg_cancelation_df.withColumn(
    "cancelation_rate",
    when(col("total_booking") != 0, col("total_cancelation") / col("total_booking")).otherwise(0.0)
).withColumn("ingestion_time", current_timestamp())

# Write to Silver table
agg_cancelation_df.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_AGG_CANCELATION_RATE")

print("✅ Created: SILVER_AGG_CANCELATION_RATE successfully.")


# create SILVER_BOARDING_SUMMARY table
# Filter only 'departure' events
departure_events = df_boarding_events.filter(col("event_type") == "departure")

# Aggregate boarding summary
silver_boarding_summary = departure_events.groupBy("flight_id").agg(
    .count("*").alias("passenger_count"),
    .sum(when(col("baggage_weight") > 0, 1).otherwise(0)).alias("baggage_count"),
    .sum(when(col("baggage_weight") > 0, col("baggage_weight")).otherwise(0)).alias("total_baggage_weight"),
    to_date(F.min("event_time")).alias("boarding_date"),
    current_timestamp().alias("ingestion_time")
).withColumn(
    "avg_baggage_per_passenger",
    (col("baggage_count") / col("passenger_count")).cast("float")
).withColumn(
    "avg_baggage_weight",
    (col("total_baggage_weight") / col("baggage_count")).cast("float")
)

# Write to Iceberg table
silver_boarding_summary.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_BOARDING_SUMMARY")

print("✅ Created: SILVER_BOARDING_SUMMARY successfully.")


# Create SILVER_AGG_FREQUENT_DELAY_REASONS table
# Filter only delay events with valid delay_reason
delays_only = df_flight_events.filter(
    (col("event_type") == "delayed") & (col("delay_reason").isNotNull())
)

# Group by flight and reason
agg_delay_reasons = delays_only.groupBy("flight_id", "delay_reason").agg(
    count("*").alias("total_occurences")
)

# Create unique delay_id and get first matching event_id for FK reference
delay_summary = agg_delay_reasons.join(
    df_flight_events,
    on=["flight_id", "delay_reason"],
    how="left"
).withColumn(
    "delay_id", F.concat_ws("_", col("flight_id"), col("delay_reason"))
).select(
    col("delay_id"),
    col("event_id").alias("silver_flight_event"),
    col("total_occurences"),
    current_timestamp().alias("ingestion_time")
).dropDuplicates(["delay_id"])  # just in case of multiple matches

# Save to Silver layer
delay_summary.write.format("iceberg") \
    .mode("overwrite") \
    .save("spark_catalog.silver.SILVER_AGG_FREQUENT_DELAY_REASONS")

print("✅ Created: SILVER_AGG_FREQUENT_DELAY_REASONS successfully.")
