from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Initialize SparkSession with Iceberg catalog
spark = (
    SparkSession.builder
        .appName('load_static_bronze')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
        .config('spark.sql.catalog.spark_catalog.type', 'hive')
        .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate()
)

# Load static routes CSV and write to Iceberg table
routes_df = (
    spark.read
        .option('header', True)
        .csv('s3a://bronze/static/routes.csv')
)

# Add ingestion_time and write to Iceberg
routes_df_with_ts = routes_df.withColumn('ingestion_time', current_timestamp())

routes_df_with_ts.writeTo('bronze.bronze_static_routes_raw') \
    .createOrReplace()

SparkSession.builder.getOrCreate().stop()