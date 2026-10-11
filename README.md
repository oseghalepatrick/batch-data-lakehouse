# OpenLakehouse: End-to-End Data Engineering, Quality & Analytics Platform

## Overview

OpenLakehouse is a containerised open data lakehouse platform that demonstrates an end-to-end modern data engineering workflow using open-source technologies.

The platform supports data ingestion, orchestration, object storage, Iceberg table management, SQL analytics, transformation, data quality monitoring, and interactive analytics dashboards.

The solution is designed as a reproducible local development and demonstration environment that can also serve as a reference architecture for larger lakehouse implementations.

## Core Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow | Workflow scheduling, dependency management, pipeline execution |
| Task Queue | Redis | Celery message broker for Airflow |
| Metadata Database | PostgreSQL | Airflow metadata and supporting state |
| Object Storage | MinIO | S3-compatible storage for lakehouse data |
| Table Format | Apache Iceberg | Open table format for Bronze, Silver, and Gold datasets |
| Catalog | Project Nessie | Iceberg catalog and versioned metadata management |
| Query Engine | Trino | Distributed SQL query engine over Iceberg tables |
| Transformation | dbt | SQL-based transformation and modelling |
| Data Quality | Soda Core | Automated data quality checks across data layers |
| Application Layer | Streamlit | Sales analytics and data quality dashboards |
| Containerisation | Docker Compose | Local service orchestration and reproducible deployment |

## Architecture

```text
                           +------------------+
                           |    Streamlit     |
                           | Analytics & DQ   |
                           +--------+---------+
                                    |
                                    v
                           +------------------+
                           |      Trino       |
                           |   SQL Engine     |
                           +--------+---------+
                                    |
                                    v
                     +--------------+--------------+
                     |                             |
                     v                             v
              +-------------+              +-------------+
              |   Iceberg   |              |    Nessie   |
              |   Tables    |<------------>|   Catalog   |
              +------+------+              +------+------+
                     |                            |
                     v                            |
              +-------------+                    |
              |    MinIO    |<-------------------+
              | S3 Storage  |
              +-------------+

        +-----------------------------------------------+
        |                 Apache Airflow                |
        | Ingestion -> Bronze DQ -> Silver -> dbt ->   |
        | Silver DQ -> Gold -> Gold DQ                 |
        +----------------------+------------------------+
                               |
                  +------------+-------------+
                  |                          |
                  v                          v
             +---------+                +---------+
             |  Redis  |                |Postgres |
             | Celery  |                |Metadata |
             +---------+                +---------+
```

## Data Flow

The implementation follows a medallion-style lakehouse architecture:

1. **Bronze Layer**
   - Raw source data is generated or ingested.
   - Parquet files are stored in MinIO.
   - Soda quality checks validate incoming datasets before downstream processing.

2. **Silver Layer**
   - Bronze data is cleaned, standardised, enriched, and written as Apache Iceberg tables.
   - PyIceberg and Nessie manage table metadata and commits.
   - Soda validates transformed Silver datasets.

3. **Gold Layer**
   - dbt transforms Silver datasets into analytics-ready fact and aggregate models.
   - Gold models are queried through Trino.
   - Soda performs final data quality validation.

4. **Analytics Layer**
   - Streamlit queries Gold and Silver datasets through Trino.
   - The Sales Analytics dashboard presents commercial metrics.
   - The Data Quality dashboard provides quality KPIs, trends, failures, warnings, schema diagnostics, and check-level drill-down.

## Docker Services

The Docker Compose environment contains the following runtime services:

| Service | Role | Host Port |
|---|---|---:|
| PostgreSQL | Airflow metadata and supporting database | Internal |
| Redis | Airflow Celery broker | Internal |
| Airflow API Server | Airflow web/API interface | 8080 |
| Airflow Scheduler | DAG scheduling | Internal |
| Airflow DAG Processor | DAG parsing | Internal |
| Airflow Worker | Celery task execution | Internal |
| Airflow Triggerer | Deferred task execution | Internal |
| Airflow Init | Database migration and initial setup | One-time |
| Airflow CLI | Debug/administrative CLI | Profile-based |
| MinIO | S3-compatible object storage | 9000 |
| MinIO Console | Object-storage administration UI | 9001 |
| MinIO Client | Bucket bootstrap and administration | Internal |
| Project Nessie | Iceberg REST catalog | 19120 |
| Trino | Distributed SQL query engine | 8089 |
| Streamlit | Analytics application | 8501 |

All services communicate through the shared Docker network:

```text
trino-net
```

## Persistent Storage

The Docker deployment defines persistent volumes for:

```text
nessie-pg-data
minio_data
postgres-db-volume
```

Airflow also mounts local project directories for:

```text
airflow/dags
airflow/logs
airflow/config
airflow/plugins
airflow/src
airflow/dbt
airflow/data_quality
```

The Streamlit container mounts:

```text
streamlit/app.py
streamlit/pages
streamlit/.streamlit
```

## Resource Requirements

### Minimum requirements declared by the Airflow bootstrap

The supplied environment checks for at least:

| Resource | Minimum |
|---|---:|
| CPU | 2 cores |
| Memory | 4 GB RAM |
| Available disk | 10 GB |

These values are the minimum checks implemented by the Airflow initialisation process.

### Recommended resources for the complete lakehouse stack

Because the environment runs Airflow, PostgreSQL, Redis, MinIO, Nessie, Trino, and Streamlit concurrently, the following allocation is recommended for a smoother development experience:

| Resource | Development Minimum | Recommended |
|---|---:|---:|
| CPU | 4 cores | 6-8 cores |
| Memory | 8 GB RAM | 12-16 GB RAM |
| Free disk | 20 GB | 30-50+ GB |
| Docker Engine | Required | Latest stable version |
| Docker Compose | Required | Compose v2 |
| Architecture | x86_64 or ARM64 where images support it | x86_64/ARM64 |

Actual disk consumption will depend on the volume of Parquet and Iceberg data retained in MinIO.

## Network Ports

Ensure the following host ports are available before starting the platform:

```text
8080   Airflow
8089   Trino
8501   Streamlit
9000   MinIO API
9001   MinIO Console
19120  Nessie
```

PostgreSQL and Redis are used internally by the Docker network and do not require host exposure for the current implementation.

## Prerequisites

Install the following before running the project:

- Docker Engine or Docker Desktop
- Docker Compose v2
- Git
- At least the minimum CPU, memory, and disk resources listed above

For Docker Desktop, allocate sufficient CPU and memory to the Docker virtual machine rather than relying only on the physical host capacity.

## Project Structure

A typical project structure is:

```text
.
├── airflow/
│   ├── dags/
│   ├── logs/
│   ├── config/
│   ├── plugins/
│   ├── src/
│   ├── dbt/
│   └── data_quality/
├── initdb/
├── streamlit/
│   ├── app.py
│   ├── pages/
│   └── .streamlit/
├── trino/
│   ├── catalog/
│   └── coordinator/
├── docker-compose.yml
├── Dockerfile
├── .env
└── README.md
```

## Implementation Steps

### 1. Clone the repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Create the environment file

Create a `.env` file at the project root.

At minimum, define the values required by your Airflow and project configuration.

For Linux hosts, set the Airflow user ID:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

Avoid committing credentials or production secrets to source control.

### 3. Verify the required project directories

Ensure that the mounted directories exist:

```bash
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/config
mkdir -p airflow/plugins
mkdir -p airflow/src
mkdir -p airflow/dbt
mkdir -p airflow/data_quality
mkdir -p trino/catalog
mkdir -p trino/coordinator
mkdir -p streamlit/pages
mkdir -p streamlit/.streamlit
mkdir -p initdb
```

### 4. Configure Trino

Configure the Trino coordinator under:

```text
trino/coordinator/config.properties
```

Configure the Iceberg catalog under:

```text
trino/catalog/
```

The catalog should connect Trino to the Iceberg/Nessie catalog and MinIO object storage.

The Docker environment exposes Trino on:

```text
http://localhost:8089
```

### 5. Initialise PostgreSQL

The PostgreSQL service loads bootstrap SQL from:

```text
./initdb
```

Place database/user initialisation scripts in this directory when required.

### 6. Start the infrastructure services

Build and start the environment:

```bash
docker compose up -d --build
```

Docker Compose will start the supporting services and the Airflow initialisation container.

Check container status:

```bash
docker compose ps
```

### 7. Validate PostgreSQL and Redis

Confirm that the Airflow dependencies are healthy:

```bash
docker compose ps postgres redis
```

Both should report a healthy state before the Airflow runtime is considered ready.

### 8. Validate Airflow

Open:

```text
http://localhost:8080
```

The environment uses the CeleryExecutor, with Redis as the broker and PostgreSQL as the result/backend database.

Confirm that the following Airflow components are running:

```text
airflow-apiserver
airflow-scheduler
airflow-dag-processor
airflow-worker
airflow-triggerer
```

### 9. Validate MinIO

Open the MinIO console:

```text
http://localhost:9001
```

The MinIO bootstrap client creates the lakehouse bucket:

```text
warehouse
```

The S3-compatible API is available at:

```text
http://localhost:9000
```

### 10. Validate Nessie

Project Nessie is available at:

```text
http://localhost:19120
```

The configured default warehouse is:

```text
s3://warehouse/
```

Confirm that Nessie can access the MinIO-backed warehouse.

### 11. Validate Trino

Connect using the Trino CLI or another SQL client:

```bash
trino --server http://localhost:8089
```

Confirm that the configured catalog is visible:

```sql
SHOW CATALOGS;
```

Then inspect schemas:

```sql
SHOW SCHEMAS FROM iceberg;
```

Typical lakehouse schemas include:

```text
bronze
silver
gold
```

### 12. Configure the Airflow DAG

Place the pipeline DAG under:

```text
airflow/dags/
```

The orchestration flow should follow the project dependency sequence:

```text
Generate / Ingest Data
        |
        v
Bronze Data
        |
        v
Bronze Data Quality
        |
        v
Silver Transformation
        |
        v
Silver Data Quality
        |
        v
dbt Gold Transformation
        |
        v
Gold Data Quality
```

### 13. Configure Bronze ingestion

Raw or generated source data should be persisted to the Bronze area of the MinIO warehouse.

Dimension datasets follow the project convention:

```text
dimensions/<table>/<table>.parquet
```

Other Bronze datasets follow:

```text
<table>/<table>.parquet
```

### 14. Configure Silver processing

Transform Bronze datasets into standardised Iceberg tables.

Use PyIceberg/Nessie for table creation and append operations.

Silver tables should be registered in the Iceberg catalog and queryable through Trino.

### 15. Configure dbt transformations

Store the dbt project under:

```text
airflow/dbt/
```

The dbt layer creates Gold analytics models such as fact and aggregate tables.

Typical execution flow:

```bash
dbt debug
dbt run
dbt test
```

Within Airflow, dbt execution can be orchestrated as part of the DAG rather than run manually.

### 16. Configure Soda data quality

Store Soda configuration and check definitions under:

```text
airflow/data_quality/
```

Quality checks should be executed at the appropriate Bronze, Silver, and Gold stages.

The project persists transformed Soda scan results into:

```text
silver.data_quality_results
```

This allows historical quality results to be queried through Trino and consumed by the Streamlit Data Quality dashboard.

### 17. Configure Streamlit

The Streamlit service is available at:

```text
http://localhost:8501
```

It connects to Trino using environment variables provided by Docker Compose.

The application contains two principal analytical experiences:

```text
Sales Analytics
Data Quality Monitoring
```

The Sales Analytics dashboard consumes Gold aggregate tables.

The Data Quality dashboard consumes the persisted Soda scan results.

### 18. Run the pipeline

Trigger the Airflow DAG from the Airflow interface or CLI.

Monitor task execution to confirm the complete flow:

```text
Bronze -> Bronze DQ -> Silver -> Silver DQ -> Gold -> Gold DQ
```

### 19. Validate the data layers

Use Trino to inspect the resulting schemas and tables.

Example:

```sql
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;
```

Validate records:

```sql
SELECT *
FROM iceberg.gold.agg_daily_sales
LIMIT 10;
```

Validate quality results:

```sql
SELECT *
FROM iceberg.silver.data_quality_results
ORDER BY scan_start_ts DESC
LIMIT 100;
```

### 20. Validate the dashboards

Open:

```text
http://localhost:8501
```

Confirm that:

- filters return the expected data;
- Sales Analytics reads Gold aggregates;
- Data Quality Monitoring reads `silver.data_quality_results`;
- failed and warning checks appear under Quality Diagnostics;
- schema diagnostics display expected and actual types;
- historical quality trends render correctly.

## Service Health Checks

Useful operational commands include:

```bash
docker compose ps
```

```bash
docker compose logs -f airflow-scheduler
```

```bash
docker compose logs -f airflow-worker
```

```bash
docker compose logs -f trino
```

```bash
docker compose logs -f nessie
```

```bash
docker compose logs -f minio
```

```bash
docker compose logs -f streamlit
```

## Restarting the Platform

Stop services:

```bash
docker compose down
```

Start services again:

```bash
docker compose up -d
```

Rebuild after dependency or Dockerfile changes:

```bash
docker compose up -d --build
```

## Cleaning the Environment

To stop containers while preserving persistent volumes:

```bash
docker compose down
```

To remove containers and persistent Docker volumes:

```bash
docker compose down -v
```

Use the second command carefully because lakehouse data and service metadata stored in Docker volumes may be deleted.

## Security Considerations

The current Docker configuration is suitable primarily for local development and demonstration.

Before adapting the platform for production:

- move credentials to a secret-management solution;
- do not hard-code object-storage credentials;
- configure authentication and TLS for Trino, MinIO, Nessie, Airflow, and Streamlit;
- restrict service exposure using network and firewall controls;
- replace development passwords;
- configure persistent database-backed Nessie storage where required;
- configure backup and recovery for PostgreSQL and MinIO;
- implement role-based access controls;
- pin container image versions instead of relying on `latest`;
- define explicit CPU and memory limits for containers.

## Implementation Checklist

- [ ] Docker and Docker Compose installed
- [ ] Required host ports available
- [ ] `.env` configured
- [ ] Airflow directories created
- [ ] Trino catalog configured
- [ ] PostgreSQL initialisation scripts available
- [ ] Docker images built
- [ ] PostgreSQL healthy
- [ ] Redis healthy
- [ ] Airflow initialised
- [ ] MinIO warehouse bucket created
- [ ] Nessie available
- [ ] Trino can query Iceberg
- [ ] Bronze ingestion operational
- [ ] Bronze quality checks operational
- [ ] Silver transformations operational
- [ ] Silver quality checks operational
- [ ] dbt Gold models operational
- [ ] Gold quality checks operational
- [ ] Soda results persisted to Silver
- [ ] Sales Analytics dashboard operational
- [ ] Data Quality dashboard operational

## Project Outcome

This project demonstrates a complete modern open-source lakehouse implementation with:

- distributed workflow orchestration;
- S3-compatible object storage;
- Apache Iceberg open table storage;
- versioned catalog management;
- federated SQL analytics;
- automated SQL transformation;
- multi-layer data quality validation;
- historical quality observability;
- interactive business analytics;
- containerised deployment.

It provides a strong reference implementation for building maintainable, observable, and analytics-ready data platforms without dependency on a single proprietary warehouse technology.
