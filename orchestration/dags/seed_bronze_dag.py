from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

MOUNT = "/home/yotamgr314/data-engineering-final-project-big-data/processing/jobs:/opt/bitnami/spark/jobs"

default_args = {"owner": "data-eng", "retries": 1,
                "retry_delay": timedelta(minutes=3)}

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
        mounts=[MOUNT],
        network_mode="bigdata-net",
    )

    validate = DockerOperator(
        task_id="validate_bronze",
        image="spark-custom:3.3",
        auto_remove=True,
        command=("spark-submit --master spark://spark-master:7077 "
                 "/opt/bitnami/spark/jobs/validate_bronze.py"),
        mounts=[MOUNT],
        network_mode="bigdata-net",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    seed >> validate
