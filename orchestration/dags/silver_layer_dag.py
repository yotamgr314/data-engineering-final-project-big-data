from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='silver_layer_creation_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['silver'],
) as dag:

    run_silver_layer = BashOperator(
        task_id='run_silver_layer_job',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/jobs/silver_layer_creation.py'
    )

    run_silver_layer
