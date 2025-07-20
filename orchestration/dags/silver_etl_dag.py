import os
from datetime import timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount

RELATIVE_JOB_PATH = "../processing/jobs"
HOST_JOB_DIR = "/mnt/c/Miri/data-engineering-final-project-big-data/processing/jobs"

DOCKER_SOCK = "/var/run/docker.sock"

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

    mounts = [
        Mount(source=HOST_JOB_DIR, target="/opt/bitnami/spark/jobs", type="bind"),
        Mount(source=DOCKER_SOCK, target=DOCKER_SOCK, type="bind"),
    ]

    env_vars = {
        "AWS_ACCESS_KEY_ID":     MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL":      MINIO_ENDPOINT,
        "S3A_USE_PATH_STYLE_ENDPOINT": "true",
    }


    def make_job(task_id, script_name):
        return DockerOperator(
            task_id=task_id,
            image="spark-custom:3.3",
            command=f"python /opt/bitnami/spark/jobs/{script_name}.py",
            working_dir="/opt/bitnami/spark/jobs",
            environment=env_vars,
            mounts=mounts,
            docker_url="unix:///var/run/docker.sock",
            network_mode="bigdata-net",
            auto_remove=True,
        )

    etl_silver_routes            = make_job("etl_silver_routes",            "silver_etl_routes")
    etl_silver_flights           = make_job("etl_silver_flights",           "silver_etl_flights")
    etl_silver_ticket_prices     = make_job("etl_silver_ticket_prices",     "silver_etl_ticket_prices")
    etl_silver_customers         = make_job("etl_silver_customers",         "silver_etl_customers")
    etl_silver_flight_events     = make_job("etl_silver_flight_events",     "silver_etl_flight_events")
    etl_silver_boarding_summary  = make_job("etl_silver_boarding_summary",  "silver_etl_boarding_summary")

    (
        etl_silver_routes
        >> etl_silver_flights
        >> etl_silver_ticket_prices
        >> etl_silver_customers
        >> etl_silver_flight_events
        >> etl_silver_boarding_summary
    )
