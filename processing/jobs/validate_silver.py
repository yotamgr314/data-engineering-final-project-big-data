import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, expr, lit, abs

# === Init Spark ===
spark = (
    SparkSession.builder
    .appName("validate_silver_comprehensive")
    .config("spark.sql.catalog.minio_silver", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio_silver.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    .config("spark.sql.catalog.minio_silver.warehouse", "s3a://silver")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

errors = []

# Utility to abort on errors
def abort_if_errors():
    if errors:
        for e in errors:
            print(e)
        sys.exit(1)

# 1. SILVER_ROUTES
df = spark.table("minio_silver.silver_routes")
# Schema & non-null
required = ["routes_id","airport_origin","airport_destination",
            "latitude_origin","longitude_origin",
            "latitude_destination","longitude_destination",
            "distance_km","ingestion_time"]
missing = set(required) - set(df.columns)
if missing:
    errors.append(f" silver_routes: missing columns {missing}")
# Nulls
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0:
        errors.append(f" silver_routes: {cnt} NULLs in {c}")
# Ranges
cnt = df.filter((col("latitude_origin")<-90)|(col("latitude_origin")>90)).count()
if cnt>0: errors.append(f" silver_routes: {cnt} invalid latitude_origin")
cnt = df.filter((col("longitude_origin")<-180)|(col("longitude_origin")>180)).count()
if cnt>0: errors.append(f" silver_routes: {cnt} invalid longitude_origin")
cnt = df.filter(col("distance_km")<=0).count()
if cnt>0: errors.append(f" silver_routes: {cnt} non-positive distance_km")
print("✔ silver_routes checks done")

# 2. SILVER_FLIGHTS
df = spark.table("minio_silver.silver_flights")
required = ["flight_id","routes_id","possible_passenger_amount",
            "flight_date","scheduled_departure","scheduled_arrival",
            "ingestion_time","possible_luggage_weight_kg"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" silver_flights: missing {missing}")
for c in ["flight_id","routes_id","scheduled_departure","scheduled_arrival"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" silver_flights: {cnt} NULLs in {c}")
# PK uniqueness
dup = df.groupBy("flight_id").count().filter("count>1").count()
if dup>0: errors.append(f" silver_flights: {dup} duplicate flight_id")
# Foreign key
fk_missing = df.join(spark.table("minio_silver.silver_routes"),
                     df.routes_id==col("routes_id"),"left_anti").count()
if fk_missing>0: errors.append(f" silver_flights: {fk_missing} routes_id with no parent")
# Values
cnt = df.filter(col("possible_passenger_amount")<0).count()
if cnt>0: errors.append(f" silver_flights: {cnt} negative passenger_amount")
cnt = df.filter(col("possible_luggage_weight_kg")<0).count()
if cnt>0: errors.append(f" silver_flights: {cnt} negative luggage_weight")
# Time logic
cnt = df.filter(col("scheduled_departure")>=col("scheduled_arrival")).count()
if cnt>0: errors.append(f" silver_flights: {cnt} departures not before arrivals")
print("✔ silver_flights checks done")

# 3. SILVER_WAYPOINT_SAMPLING_WEATHER_POINTS
df = spark.table("minio_silver.silver_waypoint_sampling_weather_points")
required = ["route_id","way_point_number","latitude","longitude","height","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" silver_waypoint_sampling_weather_points: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" waypoint: {cnt} NULLs in {c}")
# range lat/lon
cnt = df.filter((col("latitude")<-90)|(col("latitude")>90)).count()
if cnt>0: errors.append(f" waypoint: {cnt} invalid latitude")
cnt = df.filter((col("longitude")<-180)|(col("longitude")>180)).count()
if cnt>0: errors.append(f" waypoint: {cnt} invalid longitude")
print("✔ waypoint_sampling_weather_points checks done")

# 4. SILVER_TICKET_PRICES
df = spark.table("minio_silver.silver_ticket_prices")
required = ["price_id","silver_flight_id","class","price","luggage_fee",
            "start_date","end_date","actual","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" ticket_prices: missing {missing}")
# Nulls & PK
for c in ["price_id","silver_flight_id","class","price"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" ticket_prices: {cnt} NULLs in {c}")
dup = df.groupBy("price_id").count().filter("count>1").count()
if dup>0: errors.append(f" ticket_prices: {dup} duplicate price_id")
# Enum class
valid = {"first","business","economy"}
bad = df.filter(~col("class").isin(valid)).count()
if bad>0: errors.append(f" ticket_prices: {bad} invalid class values")
# Numeric & date logic
cnt = df.filter(col("price")<0).count()
if cnt>0: errors.append(f" ticket_prices: {cnt} negative prices")
cnt = df.filter(col("luggage_fee")<0).count()
if cnt>0: errors.append(f" ticket_prices: {cnt} negative luggage_fee")
cnt = df.filter(col("start_date")>col("end_date")).count()
if cnt>0: errors.append(f" ticket_prices: {cnt} start_date after end_date")
print("✔ silver_ticket_prices checks done")

# 5. SILVER_CUSTOMERS
df = spark.table("minio_silver.silver_customers")
required = ["customer_passport_id","customer_first_name","customer_last_name",
            "customer_date_of_birth","customer_nationality","customer_email",
            "customer_membership","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" customers: missing {missing}")
# Nulls & PK
for c in ["customer_passport_id","customer_email"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" customers: {cnt} NULLs in {c}")
dup = df.groupBy("customer_passport_id").count().filter("count>1").count()
if dup>0: errors.append(f" customers: {dup} duplicate passport_id")
print("✔ silver_customers checks done")

# 6. SILVER_FLIGHTS_EVENTS
df = spark.table("minio_silver.silver_flights_events")
required = ["flight_event_id","flight_id","event_type","event_time","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" flight_events: missing {missing}")
# Nulls & PK
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" flight_events: {cnt} NULLs in {c}")
dup = df.groupBy("flight_event_id").count().filter("count>1").count()
if dup>0: errors.append(f" flight_events: {dup} duplicate event_id")
# FK flight_id
fk = df.join(spark.table("minio_silver.silver_flights"),
             "flight_id","left_anti").count()
if fk>0: errors.append(f" flight_events: {fk} flight_id missing in silver_flights")
print("✔ silver_flights_events checks done")

# 7. SILVER_BOARDING_SUMMARY
df = spark.table("minio_silver.silver_boarding_summary")
required = ["flight_id","boarding_date","passenger_count","baggage_count",
            "total_baggage_weight","ingestion_time","avg_baggage_per_passenger","avg_baggage_weight"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" boarding_summary: missing {missing}")
# Nulls
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" boarding_summary: {cnt} NULLs in {c}")
# PK + FK
dup = df.groupBy("flight_id").count().filter("count>1").count()
if dup>0: errors.append(f" boarding_summary: {dup} duplicate flight_id")
fk = df.join(spark.table("minio_silver.silver_flights"),"flight_id","left_anti").count()
if fk>0: errors.append(f" boarding_summary: {fk} flight_id fk violation")
# Derived fields
calc1 = df.withColumn("calc_avg1", col("total_baggage_weight")/col("baggage_count")) \
          .filter(abs(col("calc_avg1")-col("avg_baggage_weight"))>1e-3).count()
if calc1>0: errors.append(f" boarding_summary: {calc1} avg_baggage_weight mismatch")
calc2 = df.withColumn("calc_avg2", col("total_baggage_weight")/col("passenger_count")) \
          .filter(abs(col("calc_avg2")-col("avg_baggage_per_passenger"))>1e-3).count()
if calc2>0: errors.append(f" boarding_summary: {calc2} avg_baggage_per_passenger mismatch")
print("✔ silver_boarding_summary checks done")

# 8. SILVER_AGG_FREQUENT_DELAY_REASONS
df = spark.table("minio_silver.silver_agg_frequent_delay_reasons")
required = ["delay_id","silver_flight_event","total_occurrences","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" agg_frequent_delay_reasons: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" agg_frequent_delay_reasons: {cnt} NULLs in {c}")
dup = df.groupBy("delay_id").count().filter("count>1").count()
if dup>0: errors.append(f" agg_frequent_delay_reasons: {dup} duplicate delay_id")
print("✔ silver_agg_frequent_delay_reasons checks done")

# 9. SILVER_FLIGHT_WEATHER
df = spark.table("minio_silver.silver_flight_weather")
required = ["weather_sample_id","silver_flight_id","waypoint_sampling_number",
            "temperature","humidity","wind_speed","wind_direction",
            "weather_condition","sample_for_date","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" flight_weather: missing {missing}")
for c in ["weather_sample_id","silver_flight_id","weather_condition"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" flight_weather: {cnt} NULLs in {c}")
# Ranges
cnt = df.filter((col("temperature") < -100) | (col("temperature") > 60)).count()
if cnt>0: errors.append(f" flight_weather: {cnt} invalid temperature")
cnt = df.filter((col("humidity") < 0) | (col("humidity") > 100)).count()
if cnt>0: errors.append(f" flight_weather: {cnt} invalid humidity")
cnt = df.filter(col("wind_speed")<0).count()
if cnt>0: errors.append(f" flight_weather: {cnt} negative wind_speed")
cnt = df.filter((col("wind_direction")<0)|(col("wind_direction")>360)).count()
if cnt>0: errors.append(f" flight_weather: {cnt} invalid wind_direction")
print("✔ silver_flight_weather checks done")

# 10. SILVER_WEATHER_SUMMARY
df = spark.table("minio_silver.silver_weather_summary")
required = ["flight_id","average_temperature","average_humidity",
            "average_wind_speed","average_wind_direction",
            "majority_weather_condition","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" weather_summary: missing {missing}")
for c in ["flight_id","majority_weather_condition"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" weather_summary: {cnt} NULLs in {c}")
# Range
cnt = df.filter((col("average_humidity")<0)|(col("average_humidity")>100)).count()
if cnt>0: errors.append(f" weather_summary: {cnt} invalid avg_humidity")
print("✔ silver_weather_summary checks done")

# 11. SILVER_AGG_CANCELLATION_RATE
df = spark.table("minio_silver.silver_agg_cancellation_rate")
required = ["total_booking","total_cancellation","cancellation_rate","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" agg_cancellation_rate: missing {missing}")
for c in ["total_booking","total_cancellation","cancellation_rate"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" agg_cancellation_rate: {cnt} NULLs in {c}")
# logical
cnt = df.filter(col("total_cancellation")>col("total_booking")).count()
if cnt>0: errors.append(f" agg_cancellation_rate: {cnt} cancellations > bookings")
cnt = df.filter((col("cancellation_rate")<0)|(col("cancellation_rate")>1)).count()
if cnt>0: errors.append(f" agg_cancellation_rate: {cnt} invalid cancellation_rate")
print("✔ silver_agg_cancellation_rate checks done")

# 12. SILVER_TICKET_EVENTS
df = spark.table("minio_silver.silver_ticket_events")
required = ["event_id","booked_ticket_id","event_type","event_time","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" ticket_events: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" ticket_events: {cnt} NULLs in {c}")
dup = df.groupBy("event_id").count().filter("count>1").count()
if dup>0: errors.append(f" ticket_events: {dup} duplicate event_id")
valid_types = {"canceled","class upgrade","added luggage","request to cancel"}
bad = df.filter(~col("event_type").isin(valid_types)).count()
if bad>0: errors.append(f" ticket_events: {bad} invalid event_type")
print("✔ silver_ticket_events checks done")

# 13. SILVER_BOOKED_TICKETS
df = spark.table("minio_silver.silver_booked_tickets")
required = ["booked_ticket_id","class","customer_passport_id","order_method","booking_date","ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" booked_tickets: missing {missing}")
for c in ["booked_ticket_id","class","customer_passport_id"]:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" booked_tickets: {cnt} NULLs in {c}")
dup = df.groupBy("booked_ticket_id").count().filter("count>1").count()
if dup>0: errors.append(f" booked_tickets: {dup} duplicate booked_ticket_id")
valid_classes = {"first","business","economy"}
bad = df.filter(~col("class").isin(valid_classes)).count()
if bad>0: errors.append(f" booked_tickets: {bad} invalid class")
# FK customer
fk = df.join(spark.table("minio_silver.silver_customers"),
             df.customer_passport_id==col("customer_passport_id"),"left_anti").count()
if fk>0: errors.append(f" booked_tickets: {fk} customer_passport_id fk violation")
print("✔ silver_booked_tickets checks done")

# Finalize
abort_if_errors()
print("\\n✅ All SILVER layer data quality checks PASSED.")
spark.stop()
