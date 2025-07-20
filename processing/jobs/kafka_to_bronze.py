import os, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# === config ===============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.getenv("KAFKA_TOPIC", "raw_users")
BRONZE_PATH     = "s3a://bronze/events_raw"

schema = StructType([
    StructField("event_ts", StringType()),
    StructField("user_id",  StringType()),
    StructField("name",     StringType()),
    StructField("country",  StringType()),
])

spark = (
    SparkSession.builder.appName("kafka_to_bronze")
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    .config("spark.sql.catalog.minio.warehouse", "s3a://bronze")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# === stream ==============================================================
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json("json_str", schema).alias("d"))
      .select("d.*")
      .withColumn("event_ts", to_timestamp(col("event_ts")))
)

qry = (
    parsed.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://bronze/_checkpoints/kafka_raw")
    .option("path", BRONZE_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

qry.awaitTermination()
