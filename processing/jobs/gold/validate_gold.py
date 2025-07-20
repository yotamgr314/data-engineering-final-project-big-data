import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, expr, lit, abs

# === Init Spark ===
spark = (
    SparkSession.builder
    .appName("validate_gold_comprehensive")
    .config("spark.sql.catalog.minio_gold", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio_gold.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    .config("spark.sql.catalog.minio_gold.warehouse", "s3a://gold")
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

# 1. GOLD_DIM_MONTHLY_DELAYS
df = spark.table("minio_gold.gold_dim_monthly_delays")
required = ["delays_id","most_frequent_delay","average_delay","minimal_delay",
            "maximal_delay","ingestion_time","year","month"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" gold_dim_monthly_delays: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" gold_dim_monthly_delays: {cnt} NULLs in {c}")
# Logical checks
cnt = df.filter(col("average_delay") < 0).count()
if cnt > 0: errors.append(f" gold_dim_monthly_delays: {cnt} negative average_delay")
cnt = df.filter(col("maximal_delay") < col("average_delay")).count()
if cnt > 0: errors.append(f" gold_dim_monthly_delays: {cnt} maximal_delay < average_delay")
print("✔ gold_dim_monthly_delays checks done")

# 2. GOLD_DIM_MONTHLY_FLIGHT_INFO
df = spark.table("minio_gold.gold_dim_monthly_flight_info")
required = ["flights_info_id", "total_flights", "total_passengers_number",
            "average_passengers_number_on_flight", "ingestion_time", "year", "month"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" gold_dim_monthly_flight_info: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" gold_dim_monthly_flight_info: {cnt} NULLs in {c}")
# Logical checks
cnt = df.filter(col("total_passengers_number") < 0).count()
if cnt > 0: errors.append(f" gold_dim_monthly_flight_info: {cnt} negative total_passengers_number")
cnt = df.filter(col("average_passengers_number_on_flight") < 0).count()
if cnt > 0: errors.append(f" gold_dim_monthly_flight_info: {cnt} negative average_passengers_number_on_flight")
print("✔ gold_dim_monthly_flight_info checks done")

# 3. GOLD_DIM_MONTHLY_REVENUE
df = spark.table("minio_gold.gold_dim_monthly_revenue")
required = ["revenue_id", "max_revenue_class", "total_revenue", "revenue_per_passenger",
            "average_ticket_price", "cancelation_rate", "ingestion_time", "year", "month"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" gold_dim_monthly_revenue: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" gold_dim_monthly_revenue: {cnt} NULLs in {c}")
# Logical checks
cnt = df.filter(col("total_revenue") < 0).count()
if cnt > 0: errors.append(f" gold_dim_monthly_revenue: {cnt} negative total_revenue")
cnt = df.filter(col("revenue_per_passenger") < 0).count()
if cnt > 0: errors.append(f" gold_dim_monthly_revenue: {cnt} negative revenue_per_passenger")
cnt = df.filter(col("cancelation_rate") < 0).count()
if cnt > 0: errors.append(f" gold_dim_monthly_revenue: {cnt} negative cancelation_rate")
cnt = df.filter(col("cancelation_rate") > 1).count()
if cnt > 0: errors.append(f" gold_dim_monthly_revenue: {cnt} invalid cancelation_rate (>1)")
print("✔ gold_dim_monthly_revenue checks done")

# 4. GOLD_FACT_ML_TRAINING
df = spark.table("minio_gold.gold_fact_ml_training")
required = ["training_set_id", "weather_id", "route_id", "boarding_data_id", "delay_in_minutes",
            "ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" gold_fact_ml_training: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" gold_fact_ml_training: {cnt} NULLs in {c}")
# Logical checks
cnt = df.filter(col("delay_in_minutes") < 0).count()
if cnt > 0: errors.append(f" gold_fact_ml_training: {cnt} negative delay_in_minutes")
print("✔ gold_fact_ml_training checks done")

# 5. GOLD_DIM_ML_ROUTES
df = spark.table("minio_gold.gold_dim_ml_routes")
required = ["route_id", "airport_origin", "airport_destination", "time_of_flight", "distance_km", "ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" gold_dim_ml_routes: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" gold_dim_ml_routes: {cnt} NULLs in {c}")
# Logical checks
cnt = df.filter(col("time_of_flight") < 0).count()
if cnt > 0: errors.append(f" gold_dim_ml_routes: {cnt} negative time_of_flight")
cnt = df.filter(col("distance_km") <= 0).count()
if cnt > 0: errors.append(f" gold_dim_ml_routes: {cnt} non-positive distance_km")
print("✔ gold_dim_ml_routes checks done")

# 6. GOLD_DIM_ML_WEATHER
df = spark.table("minio_gold.gold_dim_ml_weather")
required = ["weather_id", "majority_weather_condition", "number_of_most_common_condition",
            "average_height", "average_temperature", "average_humidity", "average_wind_speed",
            "average_wind_direction", "ingestion_time"]
missing = set(required)-set(df.columns)
if missing: errors.append(f" gold_dim_ml_weather: missing {missing}")
for c in required:
    cnt = df.filter(col(c).isNull()).count()
    if cnt>0: errors.append(f" gold_dim_ml_weather: {cnt} NULLs in {c}")
# Logical checks
cnt = df.filter(col("average_temperature") < -100).count()
if cnt > 0: errors.append(f" gold_dim_ml_weather: {cnt} invalid average_temperature")
cnt = df.filter(col("average_humidity") < 0).count()
if cnt > 0: errors.append(f" gold_dim_ml_weather: {cnt} invalid average_humidity")
print("✔ gold_dim_ml_weather checks done")

# Finalize
abort_if_errors()
print("\n✅ All GOLD layer data quality checks PASSED.")
spark.stop()
