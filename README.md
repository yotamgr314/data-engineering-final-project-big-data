# Data Engineering Final Project - Big Data Pipeline

## Overview

This project delivers a complete end-to-end **data engineering pipeline** using modern big data tools. It simulates a real-time airline management system by orchestrating ingestion, processing, transformation, and storage of flight, ticket, customer, weather, and event data across **bronze**, **silver**, and **gold** Iceberg layers — all within a Dockerized environment.

---

##  Tech Stack

| Component        | Tool / Framework                     |
| ---------------- | ------------------------------------ |
| Table Format     | Apache Iceberg                       |
| Storage          | MinIO (S3-compatible)                |
| Processing       | Apache Spark (batch & streaming)     |
| Streaming        | Apache Kafka                         |
| Orchestration    | Apache Airflow (with CeleryExecutor) |
| Data Generation  | Faker                                |
| Containerization | Docker Compose                       |

---

##  Project Structure

```bash
.
├── processing/
│   ├── docker-compose.yml       # Spark + MinIO
│   └── jobs/                    # All PySpark ETL scripts
├── orchestration/
│   ├── dags/                    # Airflow DAGs
│   ├── docker-compose.yml       # Airflow Webserver, Scheduler, Workers
│   └── jobs/                    # Mapped into Airflow container
├── streaming/
│   ├── docker-compose.yml       # Kafka + Producer
│   └── producer/                # Python Kafka producer
├── docs/                        # Architecture & Mermaid diagrams
├── start_project.sh             # Script to start the entire stack
├── stop_project.sh              # Shutdown all containers
└── README.md
```

---

##  Architecture

| Layer      | Description                              |
| ---------- | ---------------------------------------- |
| **Bronze** | Raw ingested data (Kafka, seed)          |
| **Silver** | Cleaned, validated, lightly enriched     |
| **Gold**   | Aggregated analytics, ML training tables |

> Architecture diagrams & Mermaid code available in `docs/project_architecture`.

---

##  Setup Instructions

###  Start the Environment

```bash
./start_project.sh
```

Services will be available at:

*  **Spark UI**: [http://localhost:8080](http://localhost:8080)
*  **Airflow**: [http://localhost:8083](http://localhost:8083)
*  **MinIO**: [http://localhost:9001](http://localhost:9001)

---

###  First-Time Setup (Airflow)

```bash
docker exec -it orchestration-airflow-webserver-1 airflow users create \
  --username user \
  --firstname userfirstname \
  --lastname userlastname \
  --role Admin \
  --email user@example.com \
  --password user_password
```

Then log into Airflow UI using these credentials.

---

##  Workflow Execution Guide

### 1.  Seed Bronze Layer

Use Airflow DAG: `seed_and_validate_bronze`
→ Seeds raw Iceberg tables (via `seed_bronze.py`)
→ Validates schema conformity (via `validate_bronze.py`)

### 2.  Silver Layer ETL

Use Airflow DAG: `silver_etl`
Transforms and cleans:

* `routes`
* `flights`
* `ticket_prices`
* `customers`
* `flight_events`
* `boarding_summary`

### 3.  Gold Layer Aggregation + ML

Use Airflow DAG: `ml_block_etl`
Generates:

* Gold ML dimensions (`dim_ml_routes`, `dim_ml_weather`, etc.)
* Gold fact table (`fact_ml_training`)
* Monthly reports (`dim_monthly_*`, `monthly_aggregates`, etc.)

---

## MinIO Access

| UI     | [http://localhost:9001](http://localhost:9001) |
| ------ | ---------------------------------------------- |
| Bucket | `bronze/`, `silver/`, `gold/`                  |
| User   | `minioadmin`                                   |
| Pass   | `minioadmin`                                   |

---

##  Shut Everything Down

```bash
./stop_project.sh
```

---

##  Entity Relationship Diagrams (ERD)

All table models are defined using Mermaid.js and included in:

```
docs/project_architecture/mermaid_code/
```

Rendered PNGs are located alongside.
Tables cover all 3 layers: `bronze`, `silver`, `gold`.

---

##  Notes

* Supports **streaming ingestion** from Kafka into Bronze using `kafka_to_bronze.py`.
* All PySpark jobs are run via `spark-submit` inside Docker containers.
* ETL jobs are parameterized and orchestrated with Airflow.
* Reproducible and isolated via Docker Compose (`version: 3.8`).

---

##  Developer Notes

* All components run inside Docker containers using shared network: `bigdata-net`.
* Iceberg runtime is pre-installed via custom Dockerfiles.
* The project supports **local WSL paths** using relative mount mapping.
* Use `seed_bronze.py` only once per full reinitialization.

---

##  Troubleshooting Tips

* Use `docker-compose logs -f` under each directory to inspect issues.
* For Spark script debugging:

  ```bash
  docker exec -it processing-spark-master-1 bash
  spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/jobs/YOUR_JOB.py
  ```

---

##  Contributors

* **Yotam Greenstein** - Developer & Architect
* Special thanks to the **Big Data Engineering Course Team**

---

##  License

MIT License — see `LICENSE` file.
