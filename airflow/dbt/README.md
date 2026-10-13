# dbt Transformation Layer

This directory contains the dbt project used to transform **Silver-layer Apache Iceberg tables into Gold-layer analytics models** for OpenLakehouse.

dbt runs as part of the Airflow batch pipeline and uses Trino as the query engine for model execution.

## Purpose

The dbt layer is responsible for:

- transforming cleaned Silver datasets into analytics-ready models;
- applying reusable business logic;
- building fact and aggregate tables;
- managing model dependencies;
- validating transformation logic with dbt tests;
- preparing Gold data for Streamlit analytics.

## Data Flow

```text
Silver Iceberg Tables
        |
        v
      Trino
        |
        v
       dbt
        |
        v
Gold Analytics Models
        |
        v
Streamlit Dashboard
```

## Gold Models

The project includes analytics models such as:

```text
fact_sales
agg_daily_sales
agg_product_performance
agg_customer_lifetime_value
```

### `fact_sales`

Provides transaction-level sales data enriched with customer, product, and store attributes.

### `agg_daily_sales`

Provides daily sales aggregates for dashboard KPIs, revenue trends, geographic analysis, store performance, payment methods, and order metrics.

### `agg_product_performance`

Provides product and category-level sales performance metrics.

### `agg_customer_lifetime_value`

Provides customer-level value metrics and supports customer historical analysis.

## Project Location

```text
airflow/dbt/
```

## dbt Profile Configuration

The Airflow Docker image already exposes the dbt profile and project locations through environment variables:

```dockerfile
ENV DBT_PROFILES_DIR=/opt/airflow/dbt/profiles
ENV DBT_PROJECT_DIR=/opt/airflow/dbt/iceberg_datalake
```

Create the dbt profile at:

```text
airflow/dbt/profiles/profiles.yml
```

Example profile:

```yaml
iceberg_datalake:
  outputs:
    dev:
      type: trino
      method: none
      user: airflow
      host: trino
      port: 8089
      catalog: iceberg
      schema: silver
      threads: 1

  target: dev
```

The `profiles` directory is intentionally excluded through `.gitignore` because it may contain environment-specific connection settings or credentials.

> Do not commit `profiles.yml` or sensitive credentials to the repository.

## Running dbt

Because `DBT_PROFILES_DIR` and `DBT_PROJECT_DIR` are already configured in the Airflow Docker image, dbt can be run without passing `--profiles-dir` or `--project-dir` explicitly.


Run the following commands from the dbt project directory.

Validate the connection and configuration:

```bash
dbt debug
```

Build the models:

```bash
dbt run
```

Run tests:

```bash
dbt test
```

Build and test together:

```bash
dbt build
```

In normal operation, dbt is executed through Apache Airflow as part of the OpenLakehouse batch pipeline.

## Model Flow

```text
Silver
  |
  v
fact_sales
  |
  +-----------------------------+
  |              |              |
  v              v              v
agg_daily_sales  agg_product_   agg_customer_
                 performance    lifetime_value
```

## Technology

- [dbt](https://www.getdbt.com/)
- [Trino](https://trino.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Project Nessie](https://projectnessie.org/)
- [MinIO](https://min.io/)
- [Apache Airflow](https://airflow.apache.org/)

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt Community](https://community.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Events](https://events.getdbt.com/)
- [dbt Blog](https://blog.getdbt.com/)
