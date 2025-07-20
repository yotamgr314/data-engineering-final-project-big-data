from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

with DAG(
    dag_id="ml_block_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="ETL for Machine Learning Tables of GOLD layer",
    tags=["ml", "gold", "iceberg"]
) as dag:

    generate_routes = BashOperator(
        task_id="gold_generate_ml_routes_dim",
        bash_command="spark-submit /opt/bitnami/spark/processing/jobs/gold_generate_ml_routes.py"
    )

    generate_weather = BashOperator(
        task_id="gold_generate_ml_weather_dim",
        bash_command="spark-submit /opt/bitnami/spark/processing/jobs/gold_generate_ml_weather.py"
    )

    generate_boarding = BashOperator(
        task_id="gold_generate_ml_boarding_data_dim",
        bash_command="spark-submit /opt/bitnami/spark/processing/jobs/gold_generate_ml_boarding_data.py"
    )

    generate_fact = BashOperator(
        task_id="gold_generate_ml_training_fact",
        bash_command="spark-submit /opt/bitnami/spark/processing/jobs/gold_generate_ml_training_fact.py"
    )

    [generate_routes, generate_weather, generate_boarding] >> generate_fact