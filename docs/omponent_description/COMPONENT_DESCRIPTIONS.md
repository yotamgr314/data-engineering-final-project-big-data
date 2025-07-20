# Component Descriptions

Below is a detailed description of each component (service) in the project, its purpose, key files, and how it fits into the overall architecture.

---

## 1. Orchestration (`/orchestration`)

**Purpose:**  
Coordinates and schedules all ETL jobs (batch & streaming), handles retries, dependencies, and monitoring via Apache Airflow.

**Key Files & Directories:**

- **`docker-compose.yml`**  
  Defines the Airflow webserver, scheduler, worker, Flower, Redis broker, and Postgres metadata database.
- **`dags/`**
  - `seed_and_validate_bronze.py` — seeds Bronze tables and validates their schemas
  - `silver_etl.py` — orchestrates all Silver‐layer transformations
  - `ml_block_etl.py` — generates Gold ML dimensions & fact tables
- **`jobs/`**  
  Mapped into the Airflow container; contains the same PySpark scripts used by the Processing component.
- **Volumes & Network:**
  - Mounts the PySpark job folder (`../processing/jobs`)
  - Joins the `bigdata-net` to communicate with Spark & MinIO

**How It Works:**

1. **Seed Bronze** → runs `seed_bronze.py`, then `validate_bronze.py`.
2. **Silver ETL** → triggers six DockerOperators to run each Silver‐layer script inside a Spark container.
3. **Gold ML ETL** → runs four BashOperators submitting Spark jobs for ML tables.
4. Retries, logging, and failure notifications are managed automatically by Airflow.

---

## 2. Processing (`/processing`)

**Purpose:**  
Hosts all Apache Spark–based batch and streaming applications, including Bronze→Silver and Gold transformations.

**Key Files & Directories:**

- **`docker-compose.yml`**  
  Brings up Spark master, Spark workers, and MinIO instance.
- **`jobs/`**
  - **Batch jobs:**
    - `bronze_to_silver_batch.py`
    - `silver_etl_*.py` (one per Silver table)
    - `gold_generate_*.py` (fact & dimension scripts)
  - **Streaming jobs:**
    - `kafka_to_bronze.py` — reads Kafka, writes to Iceberg Bronze
- **Custom Spark Dockerfile**  
  Installs `pyspark`, Iceberg runtime JAR, Faker, and other dependencies.

**How It Works:**

- **Batch:** Spark jobs are submitted via `spark-submit --master spark://spark-master:7077 ...`
- **Streaming:** Spark Structured Streaming reads from Kafka topic `raw_users`, writes to Iceberg Bronze with checkpointing.

---

## 3. Streaming (`/streaming`)

**Purpose:**  
Sets up Apache Kafka (and Zookeeper) plus a Python-based producer to simulate real-time event streams.

**Key Files & Directories:**

- **`docker-compose.yml`**
  - `zookeeper` service
  - `kafka` broker
  - `producer` application
- **`producer/`**
  - `Dockerfile` — Python image with `kafka-python`
  - `app.py` — continuously sends fake JSON events (`user_id`, `name`, `country`, `event_ts`) to topic `raw_users`
  - `requirements.txt`

**How It Works:**

1. **Start Kafka & Zookeeper** via Docker Compose
2. **Run Producer** → produces one message every ~0.3 seconds
3. **Spark Streaming** (in Processing) consumes from `raw_users` topic and writes to Bronze layer

---

## 4. Documentation (`/docs`)

**Purpose:**  
Holds all project documentation, diagrams, and data model definitions.

**Key Files & Directories:**

- **`project_architecture/`**
  - **PNGs** (`bronze_layer_architecture.png`, `silver_layer_architecture.png`, `gold_layer_architecture.png`)
  - **Mermaid code** (`*.mmd`) for each layer, showing ER diagrams
- **`README.md`** (root) links here for deeper dive into data models and architecture

**How It Works:**

- Mermaid files can be rendered automatically (e.g., via VSCode or Mermaid Live Editor)
- Diagrams illustrate table relationships, data flow between layers, and component interactions

---

## 5. Root‐Level Scripts

### `start_project.sh`

- Creates (if needed) Docker network `bigdata-net`
- Boots services in order:
  1. Streaming (Kafka & Producer)
  2. Processing (Spark & MinIO)
  3. Orchestration (Airflow stack)
- Prints service URLs for easy access

### `stop_project.sh`

- Tears down in reverse order:
  1. Orchestration
  2. Processing
  3. Streaming

---

## 6. MinIO Access

Although MinIO is brought up under `/processing`, it serves all layers:

- **Buckets:** `bronze/`, `silver/`, `gold/`
- **Credentials:**
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin`
- **UI:** [http://localhost:9001](http://localhost:9001)
- Used by both Spark jobs (`s3a://…`) and by you for manual inspection

---

## How Components Communicate

- All containers join the **`bigdata-net`** Docker network
- **Airflow** triggers Spark jobs over the Docker socket or via `spark-submit` inside containers
- **Spark** reads/writes Iceberg tables directly in MinIO using S3A
- **Kafka** broker is reachable from both producer and Spark streaming jobs

---
