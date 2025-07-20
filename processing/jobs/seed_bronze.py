import uuid, random
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, DecimalType
)
from decimal import Decimal


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
                row.append(Decimal(round(random.uniform(1.0, 100.0), 2)))  # המרת float ל-Decimal
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
    # טבלאות נוספות...
]

# ריצה ליצירת כל הטבלאות עם נתונים דמיוניים (10 שורות כל טבלה)
for table in tables:
    schema = table["schema"]
    name = table["name"]
    rows = generate_rows_for_table(schema, num_rows=10)  # הפחתתי את מספר השורות ל-10
    create_table(name, schema, rows)

print("🎉  כל הטבלאות נוצרו והוגדרו בהצלחה עם נתונים דמיוניים!")
