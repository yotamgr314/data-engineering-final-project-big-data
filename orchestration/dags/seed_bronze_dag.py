from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from docker.types import Mount 
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
HOST_JOBS_PATH = os.path.join(BASE_DIR, "processing", "jobs")

with DAG(
    dag_id="seed_and_validate_bronze",
    description="seed bronze + schema validation",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    seed = DockerOperator(
        task_id="seed_bronze",
        image="spark-custom:3.3",
        auto_remove=True,
        command=(
            "bash -c '[[ \"{{ dag_run.conf.get(\"seed\", false) }}\" == \"True\" ]] "
            "&& spark-submit --master spark://spark-master:7077 "
            "/opt/bitnami/spark/jobs/seed_bronze.py || echo Skip'"),
        mounts=[Mount(source=HOST_JOBS_PATH, target="/opt/bitnami/spark/jobs", type="bind")],
        network_mode="bigdata-net",
    )

    validate = DockerOperator(
        task_id="validate_bronze",
        image="spark-custom:3.3",
        auto_remove=True,
        command=("spark-submit --master spark://spark-master:7077 "
                 "/opt/bitnami/spark/jobs/validate_bronze.py"),
        mounts=[Mount(source=HOST_JOBS_PATH, target="/opt/bitnami/spark/jobs", type="bind")],
        network_mode="bigdata-net",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    seed >> validate
