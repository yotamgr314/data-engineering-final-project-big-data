from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'data-eng',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': days_ago(1),
    'catchup': False
}

with DAG(
    'silver_etl',
    default_args=default_args,
    description='ETL Jobs for Silver Layer',
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    etl_silver_routes = DockerOperator(
        task_id="etl_silver_routes",
        image="your_docker_image_for_silver_etl",
        command="python /opt/bitnami/spark/jobs/silver_etl_routes.py",
        mounts=["/opt/airflow/dags/jobs:/opt/bitnami/spark/jobs"],
        network_mode="bridge",
        dag=dag
    )



    # Task 2: ETL for Silver Flights (joins bronze_flights_streaming_source)
    etl_silver_flights = DockerOperator(
        task_id="etl_silver_flights",
        image="your_docker_image_for_silver_etl",
        command="python /opt/bitnami/spark/jobs/silver_etl_flights.py",
        mounts=["/path/to/local:/opt/bitnami/spark/jobs"],
        network_mode="bridge",
        dag=dag
    )

    # Task 3: ETL for Silver Ticket Prices (joins bronze_ticket_prices)
    etl_silver_ticket_prices = DockerOperator(
        task_id="etl_silver_ticket_prices",
        image="your_docker_image_for_silver_etl",
        command="python /opt/bitnami/spark/jobs/silver_etl_ticket_prices.py",
        mounts=["/path/to/local:/opt/bitnami/spark/jobs"],
        network_mode="bridge",
        dag=dag
    )

    # Task 4: ETL for Silver Customers (joins bronze_registered_customeres_streaming)
    etl_silver_customers = DockerOperator(
        task_id="etl_silver_customers",
        image="your_docker_image_for_silver_etl",
        command="python /opt/bitnami/spark/jobs/silver_etl_customers.py",
        mounts=["/path/to/local:/opt/bitnami/spark/jobs"],
        network_mode="bridge",
        dag=dag
    )

    # Task 5: ETL for Silver Flight Events (joins bronze_flight_events_raw)
    etl_silver_flight_events = DockerOperator(
        task_id="etl_silver_flight_events",
        image="your_docker_image_for_silver_etl",
        command="python /opt/bitnami/spark/jobs/silver_etl_flight_events.py",
        mounts=["/path/to/local:/opt/bitnami/spark/jobs"],
        network_mode="bridge",
        dag=dag
    )

    # Task 6: ETL for Silver Boarding Summary (joins bronze_boarding_events_raw)
    etl_silver_boarding_summary = DockerOperator(
        task_id="etl_silver_boarding_summary",
        image="your_docker_image_for_silver_etl",
        command="python /opt/bitnami/spark/jobs/silver_etl_boarding_summary.py",
        mounts=["/path/to/local:/opt/bitnami/spark/jobs"],
        network_mode="bridge",
        dag=dag
    )

    # Define task dependencies
    etl_silver_routes >> etl_silver_flights >> etl_silver_ticket_prices >> etl_silver_customers >> etl_silver_flight_events >> etl_silver_boarding_summary
