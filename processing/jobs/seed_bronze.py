"""
Seed Bronze tables (batch oriented) – FIXED:
* Drop existing Iceberg table (if any)
* Write dataframe with createOrReplace()
"""
import uuid, random
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, DecimalType
)
fake = Faker()

spark = (
    SparkSession.builder.appName("seed_bronze")
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    .config("spark.sql.catalog.minio.warehouse", "s3a://bronze")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

def create_table(name: str, schema: StructType, rows=None):
    """Drop & create Iceberg table in ‘minio’ catalog"""
    spark.sql(f"DROP TABLE IF EXISTS minio.{name}")
    df = spark.createDataFrame(rows or [], schema)
    (df.writeTo(f"minio.{name}")
       .tableProperty("format-version", "2")
       .createOrReplace())
    print(f"✅ created {name}")

# ---------------------------------------------------------------------
# 1. טבלאות Static / Raw
# ---------------------------------------------------------------------
create_table(
    "bronze_static_routes_raw",
    StructType([
        StructField("route_id",               StringType(), False),
        StructField("airport_origin",         StringType(), False),
        StructField("airport_destination",    StringType(), False),
        StructField("latitude_origin",        DoubleType(), False),
        StructField("longitude_origin",       DoubleType(), False),
        StructField("latitude_destination",   DoubleType(), False),
        StructField("longitude_destination",  DoubleType(), False),
        StructField("distance_km",            FloatType(),  False),
    ])
)

create_table(
    "bronze_route_weather_points_static",
    StructType([
        StructField("route_id",        StringType(), False),
        StructField("way_point_number",IntegerType(), False),
        StructField("latitude",        DoubleType(), False),
        StructField("longitude",       DoubleType(), False),
        StructField("height",          FloatType(),  False),
    ])
)

# ---------------------------------------------------------------------
# 2. מקורות Streaming / API
# ---------------------------------------------------------------------
create_table(
    "bronze_flights_streaming_source",
    StructType([
        StructField("flight_id",                StringType(),  False),
        StructField("route_id",                 StringType(),  False),
        StructField("max_passenger_capacity",   IntegerType(), False),
        StructField("schedualed_arrival",       TimestampType(), True),
        StructField("schedualed_departure",     TimestampType(), True),
        StructField("max_lagguge_weight_capacity", FloatType(), True),   # ק"ג
    ])
)

create_table(
    "bronze_booked_tickets_raw_streaming",
    StructType([
        StructField("booked_ticket_id",       StringType(), False),
        StructField("ticket_price",           FloatType(),  False),
        StructField("passenger_passport_id",  StringType(), False),
        StructField("passenger_first_name",   StringType(), False),
        StructField("passenger_last_name",    StringType(), False),
        StructField("order_method",           StringType(), False),
        StructField("booking_date",           DateType(),   False),
        StructField("ticket_class",           StringType(), False),
        StructField("luggage_class",          StringType(), False),
        StructField("passenger_nationality",  StringType(), False),
        StructField("passenger_email",        StringType(), False),
        StructField("passenger_date_of_birth",DateType(),   False),
    ])
)

create_table(
    "bronze_registered_customeres_streaming",
    StructType([
        StructField("customer_passport_id",   StringType(), False),
        StructField("customer_first_name",    StringType(), False),
        StructField("customer_last_name",     StringType(), False),
        StructField("customer_date_of_birth", DateType(),   False),
        StructField("passenger_nationality",  StringType(), False),
        StructField("passenger_email",        StringType(), False),
        StructField("customer_membership_tier",StringType(),False),
    ])
)

create_table(
    "bronze_flight_weather_raw_api",
    StructType([
        StructField("weather_sample_id",      StringType(), False),
        StructField("temperature",            DoubleType(), False),
        StructField("humidity",               DoubleType(), False),
        StructField("wind_speed",             DoubleType(), False),
        StructField("wind_direction",         StringType(), False),
        StructField("current_weather_condition", StringType(), False),
        StructField("sample_for_date",        TimestampType(), False),
    ])
)

# ---------------------------------------------------------------------
# 3. טבלאות מחירים / אירועים
# ---------------------------------------------------------------------
create_table(
    "bronze_ticket_prices",
    StructType([
        StructField("price_id",      StringType(), False),
        StructField("flight_id",     StringType(), False),
        StructField("class",         StringType(), False),  # first / business / economy
        StructField("price",         FloatType(),  False),
        StructField("luggage_fee",   FloatType(),  False),
        StructField("start_date",    DateType(),   False),
        StructField("end_date",      DateType(),   False),
        StructField("actual",        BooleanType(),False),
    ])
)

create_table(
    "bronze_boarding_events_raw",
    StructType([
        StructField("event_id",          StringType(),  False),
        StructField("flight_id",         StringType(),  False),
        StructField("event_type",        StringType(),  False),
        StructField("event_time",        TimestampType(), False),
        StructField("passenger_id",      StringType(),  True),
        StructField("baggage_weight",    DecimalType(10,2), True),  # kg
        StructField("ingestion_time",    TimestampType(), False),
    ])
)

create_table(
    "bronze_flight_events_raw",
    StructType([
        StructField("event_id",     StringType(), False),
        StructField("flight_id",    StringType(), False),
        StructField("event_type",   StringType(), False),
        StructField("delay_reason", StringType(), True),
        StructField("event_time",   TimestampType(), False),
    ])
)

create_table(
    "bronze_ticket_events_raw_streaming",
    StructType([
        StructField("event_id",        StringType(), False),
        StructField("booked_ticket_id",StringType(), False),
        StructField("event_type",      StringType(), False),
        StructField("event_time",      TimestampType(), False),
    ])
)

print("🎉  All Bronze tables created successfully!")
