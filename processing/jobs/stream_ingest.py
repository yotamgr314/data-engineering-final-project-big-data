from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Schema for flight events
schema = StructType([  
    StructField('flight_id', StringType()),
    StructField('route_id', StringType()),
    StructField('scheduled_departure', StringType()),
    StructField('scheduled_arrival', StringType()),
    StructField('max_passenger_capacity', IntegerType())
])

spark = (
    SparkSession.builder
        .appName('bronze_stream_ingest')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog')
        .config('spark.sql.catalog.spark_catalog.type', 'hive')
        .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate()
)

# Read from Kafka
df = (
    spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', 'kafka:9092')
        .option('subscribe', 'flights')
        .option('startingOffsets', 'latest')
        .load()
)

# Parse JSON and select fields
parsed = (
    df.select(from_json(col('value').cast('string'), schema).alias('data'))
      .select('data.*')
)

# Write to Iceberg table with checkpointing
query = (
    parsed.writeStream
      .format('iceberg')
      .option('checkpointLocation', 's3a://bronze/checkpoints/flights')
      .outputMode('append')
      .toTable('bronze.bronze_flights_streaming_source')
)

query.awaitTermination()