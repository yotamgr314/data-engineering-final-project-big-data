"""
Reads Iceberg tables in Bronze layer, performs light transformation
and writes to Silver.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("bronze_to_silver_batch")
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    .config("spark.sql.catalog.minio.warehouse", "s3a://bronze")  # הגדרת מחסן עבור ה‑Bronze
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # הגדרת ה‑endpoint של MinIO
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")  # הגדרת המפתח
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")  # הגדרת הסוד
    .config("spark.hadoop.fs.s3a.path.style.access", "true")  # הגדרת שימוש ב‑path style
    .getOrCreate()
)

bronze = "minio.bronze_flights_raw"
silver = "minio_silver.flights_clean"

spark.sql("CREATE NAMESPACE IF NOT EXISTS minio_silver")

df = (
    spark.table(bronze)
         .where("origin != destination")
         .dropDuplicates(["flight_id"])
)

df.writeTo(silver).createOrReplace()
print("✔ Bronze→Silver done.")
