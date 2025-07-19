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
    """Drop & create Iceberg table in ‘minio’ catalog"""
    try:
        spark.sql(f"DROP TABLE IF EXISTS minio.{name}")
        df = spark.createDataFrame(rows or [], schema)
        df.writeTo(f"minio.{name}") \
            .tableProperty("format-version", "2") \
            .createOrReplace()
        print(f"✅ created {name}")
    except Exception as e:
        print(f"❌ Failed to create {name}: {e}")

def generate_rows_for_table(schema: StructType, num_rows=10):
    """Generates fake data rows based on schema"""
    rows = []
    for _ in range(num_rows):
        row = []
        for field in schema.fields:
            dtype = field.dataType
            if isinstance(dtype, StringType):
                row.append(str(uuid.uuid4()))
            elif isinstance(dtype, IntegerType):
                row.append(random.randint(1, 100))
            elif isinstance(dtype, DoubleType):
                row.append(round(random.uniform(1.0, 100.0), 2))
            elif isinstance(dtype, FloatType):
                row.append(round(random.uniform(1.0, 100.0), 2))
            elif isinstance(dtype, BooleanType):
                row.append(random.choice([True, False]))
            elif isinstance(dtype, DateType):
                row.append(fake.date_between(start_date='-1y', end_date='today'))
            elif isinstance(dtype, TimestampType):
                row.append(fake.date_time_this_year())
            elif isinstance(dtype, DecimalType):
                row.append(round(random.uniform(1.0, 100.0), 2))
            else:
                row.append(None)
        rows.append(tuple(row))
    return rows

# הגדרת הטבלאות
tables = [
    {
        "name": "bronze_static_routes_raw",
        "schema": StructType([
            StructField("route_id", StringType(), False),
            StructField("airport_origin", StringType(), False),
            StructField("airport_destination", StringType(), False),
            StructField("latitude_origin", DoubleType(), False),
            StructField("longitude_origin", DoubleType(), False),
            StructField("latitude_destination", DoubleType(), False),
            StructField("longitude_destination", DoubleType(), False),
            StructField("distance_km", FloatType(), False),
        ])
    },
    {
        "name": "bronze_route_weather_points_static",
        "schema": StructType([
            StructField("route_id", StringType(), False),
            StructField("way_point_number", IntegerType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("height", FloatType(), False),
        ])
    },
    {
        "name": "bronze_flights_streaming_source",
        "schema": StructType([
            StructField("flight_id", StringType(), False),
            StructField("route_id", StringType(), False),
            StructField("max_passenger_capacity", IntegerType(), False),
            StructField("schedualed_arrival", TimestampType(), True),
            StructField("schedualed_departure", TimestampType(), True),
            StructField("max_lagguge_weight_capacity", FloatType(), True),
        ])
    },
    {
        "name": "bronze_booked_tickets_raw_streaming",
        "schema": StructType([
            StructField("booked_ticket_id", StringType(), False),
            StructField("ticket_price", FloatType(), False),
            StructField("passenger_passport_id", StringType(), False),
            StructField("passenger_first_name", StringType(), False),
            StructField("passenger_last_name", StringType(), False),
            StructField("order_method", StringType(), False),
            StructField("booking_date", DateType(), False),
            StructField("ticket_class", StringType(), False),
            StructField("luggage_class", StringType(), False),
            StructField("passenger_nationality", StringType(), False),
            StructField("passenger_email", StringType(), False),
            StructField("passenger_date_of_birth", DateType(), False),
        ])
    },
    {
        "name": "bronze_registered_customeres_streaming",
        "schema": StructType([
            StructField("customer_passport_id", StringType(), False),
            StructField("customer_first_name", StringType(), False),
            StructField("customer_last_name", StringType(), False),
            StructField("customer_date_of_birth", DateType(), False),
            StructField("passenger_nationality", StringType(), False),
            StructField("passenger_email", StringType(), False),
            StructField("customer_membership_tier", StringType(), False),
        ])
    },
    {
        "name": "bronze_flight_weather_raw_api",
        "schema": StructType([
            StructField("weather_sample_id", StringType(), False),
            StructField("temperature", DoubleType(), False),
            StructField("humidity", DoubleType(), False),
            StructField("wind_speed", DoubleType(), False),
            StructField("wind_direction", StringType(), False),
            StructField("current_weather_condition", StringType(), False),
            StructField("sample_for_date", TimestampType(), False),
        ])
    },
    {
        "name": "bronze_ticket_prices",
        "schema": StructType([
            StructField("price_id", StringType(), False),
            StructField("flight_id", StringType(), False),
            StructField("class", StringType(), False),
            StructField("price", FloatType(), False),
            StructField("luggage_fee", FloatType(), False),
            StructField("start_date", DateType(), False),
            StructField("end_date", DateType(), False),
            StructField("actual", BooleanType(), False),
        ])
    },
    {
        "name": "bronze_boarding_events_raw",
        "schema": StructType([
            StructField("event_id", StringType(), False),
            StructField("flight_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("passenger_id", StringType(), True),
            StructField("baggage_weight", DecimalType(10,2), True),
            StructField("ingestion_time", TimestampType(), False),
        ])
    },
    {
        "name": "bronze_flight_events_raw",
        "schema": StructType([
            StructField("event_id", StringType(), False),
            StructField("flight_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("delay_reason", StringType(), True),
            StructField("event_time", TimestampType(), False),
        ])
    },
    {
        "name": "bronze_ticket_events_raw_streaming",
        "schema": StructType([
            StructField("event_id", StringType(), False),
            StructField("booked_ticket_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_time", TimestampType(), False),
        ])
    }
]

# ריצה ליצירת כל הטבלאות עם נתונים דמיוניים (10 שורות כל טבלה)
for table in tables:
    schema = table["schema"]
    name = table["name"]
    rows = generate_rows_for_table(schema, num_rows=10)  # הפחתתי את מספר השורות ל-10
    create_table(name, schema, rows)

print("🎉  כל הטבלאות נוצרו והוגדרו בהצלחה עם נתונים דמויות!")
