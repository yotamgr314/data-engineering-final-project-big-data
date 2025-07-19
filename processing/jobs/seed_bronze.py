import uuid, random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType


# יצירת סשן של Spark
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

# פונקציה ליצירת טבלה
def create_table(name: str, schema: StructType, rows=None):
    """Drop & create Iceberg table in ‘minio’ catalog"""
    spark.sql(f"DROP TABLE IF EXISTS minio.{name}")
    df = spark.createDataFrame(rows or [], schema)
    
    # תיקון: המרת כל שדה שהוא float ל-DecimalType עם 2 ספרות אחרי הנקודה
    for field in df.schema.fields:
        if isinstance(field.dataType, FloatType) or isinstance(field.dataType, DoubleType):
            df = df.withColumn(field.name, col(field.name).cast(DecimalType(10, 2)))
    
    # כתיבת הנתונים ל-Iceberg
    (df.writeTo(f"minio.{name}")
       .tableProperty("format-version", "2")
       .createOrReplace())
    print(f"✅ created {name}")

# פונקציה ליצירת נתונים מבוקרים
def generate_data_manually(schema, num_rows):
    rows = []
    for _ in range(num_rows):
        row = []
        for field in schema.fields:
            if isinstance(field.dataType, StringType):
                # יצירת מילים מותאמות אישית
                row.append("airport_code")
            elif isinstance(field.dataType, IntegerType):
                # יצירת מספרים אקראיים בתחום
                row.append(random.randint(1, 100))
            elif isinstance(field.dataType, DoubleType):
                # יצירת מספרים עשרוניים
                row.append(random.uniform(1, 100))
            elif isinstance(field.dataType, FloatType):
                # יצירת מספרים עשרוניים
                row.append(random.uniform(1, 100))
            elif isinstance(field.dataType, TimestampType):
                # יצירת תאריך ושעה אקראיים
                row.append(datetime.now() - timedelta(days=random.randint(0, 10), hours=random.randint(0, 23)))
            elif isinstance(field.dataType, DateType):
                # יצירת תאריך אקראי
                row.append(datetime.now().date())
            elif isinstance(field.dataType, BooleanType):
                # יצירת ערכים אקראיים של True/False
                row.append(random.choice([True, False]))
            elif isinstance(field.dataType, DecimalType):
                # יצירת מספרים עשרוניים
                row.append(round(random.uniform(1, 100), 2))
            else:
                row.append(None)
        rows.append(row)
    return rows

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
    ]),
    generate_data_manually(
        StructType([
            StructField("route_id",               StringType(), False),
            StructField("airport_origin",         StringType(), False),
            StructField("airport_destination",    StringType(), False),
            StructField("latitude_origin",        DoubleType(), False),
            StructField("longitude_origin",       DoubleType(), False),
            StructField("latitude_destination",   DoubleType(), False),
            StructField("longitude_destination",  DoubleType(), False),
            StructField("distance_km",            FloatType(),  False),
        ]),
        100
    )
)

create_table(
    "bronze_route_weather_points_static",
    StructType([
        StructField("route_id",        StringType(), False),
        StructField("way_point_number",IntegerType(), False),
        StructField("latitude",        DoubleType(), False),
        StructField("longitude",       DoubleType(), False),
        StructField("height",          FloatType(),  False),
    ]),
    generate_data_manually(
        StructType([
            StructField("route_id",        StringType(), False),
            StructField("way_point_number",IntegerType(), False),
            StructField("latitude",        DoubleType(), False),
            StructField("longitude",       DoubleType(), False),
            StructField("height",          FloatType(),  False),
        ]),
        100
    )
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
        StructField("scheduled_arrival",        TimestampType(), True),
        StructField("scheduled_departure",      TimestampType(), True),
        StructField("max_luggage_weight_capacity", FloatType(), True),   # ק"ג
    ]),
    generate_data_manually(
        StructType([
            StructField("flight_id",                StringType(),  False),
            StructField("route_id",                 StringType(),  False),
            StructField("max_passenger_capacity",   IntegerType(), False),
            StructField("scheduled_arrival",        TimestampType(), True),
            StructField("scheduled_departure",      TimestampType(), True),
            StructField("max_luggage_weight_capacity", FloatType(), True),
        ]),
        100
    )
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
    ]),
    generate_data_manually(
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
        ]),
        100
    )
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
    ]),
    generate_data_manually(
        StructType([
            StructField("price_id",      StringType(), False),
            StructField("flight_id",     StringType(), False),
            StructField("class",         StringType(), False),
            StructField("price",         FloatType(),  False),
            StructField("luggage_fee",   FloatType(),  False),
            StructField("start_date",    DateType(),   False),
            StructField("end_date",      DateType(),   False),
            StructField("actual",        BooleanType(),False),
        ]),
        100
    )
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
    ]),
    generate_data_manually(
        StructType([
            StructField("event_id",          StringType(),  False),
            StructField("flight_id",         StringType(),  False),
            StructField("event_type",        StringType(),  False),
            StructField("event_time",        TimestampType(), False),
            StructField("passenger_id",      StringType(),  True),
            StructField("baggage_weight",    DecimalType(10,2), True),
            StructField("ingestion_time",    TimestampType(), False),
        ]),
        100
    )
)

create_table(
    "bronze_flight_events_raw",
    StructType([
        StructField("event_id",     StringType(), False),
        StructField("flight_id",    StringType(), False),
        StructField("event_type",   StringType(), False),
        StructField("delay_reason", StringType(), True),
        StructField("event_time",   TimestampType(), False),
    ]),
    generate_data_manually(
        StructType([
            StructField("event_id",     StringType(), False),
            StructField("flight_id",    StringType(), False),
            StructField("event_type",   StringType(), False),
            StructField("delay_reason", StringType(), True),
            StructField("event_time",   TimestampType(), False),
        ]),
        100
    )
)

create_table(
    "bronze_ticket_events_raw_streaming",
    StructType([
        StructField("event_id",        StringType(), False),
        StructField("booked_ticket_id",StringType(), False),
        StructField("event_type",      StringType(), False),
        StructField("event_time",      TimestampType(), False),
    ]),
    generate_data_manually(
        StructType([
            StructField("event_id",        StringType(), False),
            StructField("booked_ticket_id",StringType(), False),
            StructField("event_type",      StringType(), False),
            StructField("event_time",      TimestampType(), False),
        ]),
        100
    )
)

print("🎉  All Bronze tables created successfully!")
