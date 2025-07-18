"""
Seed Bronze tables (batch oriented) – FIXED:
* Drop existing Iceberg table (if any)
* Write dataframe with createOrReplace()
"""
import uuid, random
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import *

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

# ---------- Generators ----------------------------------------------------
def gen_airports(n=200):
    rows = [(fake.unique.bothify("???").upper(),
             fake.city(), fake.country_code()) for _ in range(n)]
    return spark.createDataFrame(rows,
        "iata string, city string, country string")

def gen_aircrafts(n=100):
    rows = [(uuid.uuid4().hex,
             fake.pystr(min_chars=3, max_chars=10).upper(),
             random.randint(100, 350)) for _ in range(n)]
    return spark.createDataFrame(rows,
        "aircraft_id string, model string, capacity int")

def gen_flights(n=5_000):
    rows = []
    for _ in range(n):
        dep, arr = fake.unique.bothify("???").upper(), fake.unique.bothify("???").upper()
        while arr == dep:
            arr = fake.unique.bothify("???").upper()
        rows.append((uuid.uuid4().hex, dep, arr,
                     fake.date_time_this_year(),          # datetime object
                     fake.time(pattern="%H:%M:%S")))
    return spark.createDataFrame(rows,
        "flight_id string, origin string, destination string, "
        "flight_date timestamp, departure_time string")

tables = {
    "bronze_airports_raw" : gen_airports(),
    "bronze_aircrafts_raw": gen_aircrafts(),
    "bronze_flights_raw"  : gen_flights(),
}

# ---------- Write ---------------------------------------------------------
for tbl, df in tables.items():
    target = f"minio.{tbl}"
    spark.sql(f"DROP TABLE IF EXISTS {target}")      # מסיר סכמה ישנה
    print(f"Seeding {target} ...")
    df.writeTo(target).createOrReplace()             # יוצר או מחליף
    print(f" -> wrote {df.count()} rows")

print("✔ Bronze seed completed.")
