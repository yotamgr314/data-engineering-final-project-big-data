from datetime import timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

from pyspark.sql import SparkSession

# הגדרת Spark עם חיבור ל‑MinIO
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# בתוך קונטיינר ה‑Airflow יש לנו:
#   - את הסקריפטים ב־/opt/bitnami/spark/jobs
#   - את ה‑docker.sock ב־/var/run/docker.sock
HOST_JOB_DIR = "/opt/bitnami/spark/jobs"
DOCKER_SOCK   = "/var/run/docker.sock"

# MinIO credentials (ברירת מחדל של bitnami/minio)
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT   = "http://minio:9000"

default_args = {
    "owner": "data-eng",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="silver_etl",
    default_args=default_args,
    description="ETL Jobs for Silver Layer",
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    # מיפוי תיקיית הסקריפטים ו‑docker.sock
    mounts = [
        Mount(source=HOST_JOB_DIR, target="/opt/bitnami/spark/jobs", type="bind"),
        Mount(source=DOCKER_SOCK,   target=DOCKER_SOCK,           type="bind"),
    ]

    # env vars to let Spark / Hadoop fs.s3a connect to MinIO
    env_vars = {
        "AWS_ACCESS_KEY_ID":     MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        # many Spark/Hadoop libs also respect AWS_ENDPOINT_URL
        "AWS_ENDPOINT_URL":      MINIO_ENDPOINT,
        # לשימוש ב‑path style מול MinIO
        "S3A_USE_PATH_STYLE_ENDPOINT": "true",
    }

    def make_job(task_id, script_name):
        return DockerOperator(
            task_id=task_id,
            image="spark-custom:3.3",
            command=f"python /opt/bitnami/spark/jobs/{script_name}.py",
            environment=env_vars,
            mounts=mounts,
            docker_url="unix:///var/run/docker.sock",
            network_mode="bigdata-net",
            auto_remove=True,
        )

    # הגדרת משימות ETL
    etl_silver_routes            = make_job("etl_silver_routes",            "silver_etl_routes")
    etl_silver_flights           = make_job("etl_silver_flights",           "silver_etl_flights")
    etl_silver_ticket_prices     = make_job("etl_silver_ticket_prices",     "silver_etl_ticket_prices")
    etl_silver_customers         = make_job("etl_silver_customers",         "silver_etl_customers")
    etl_silver_flight_events     = make_job("etl_silver_flight_events",     "silver_etl_flight_events")
    etl_silver_boarding_summary  = make_job("etl_silver_boarding_summary",  "silver_etl_boarding_summary")

    # הגדרת התלותות ביניהם
    (
        etl_silver_routes
        >> etl_silver_flights
        >> etl_silver_ticket_prices
        >> etl_silver_customers
        >> etl_silver_flight_events
        >> etl_silver_boarding_summary
    )
