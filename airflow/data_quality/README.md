# Soda Data Quality

This directory contains the **Soda Core data quality framework used to validate Bronze, Silver, and Gold datasets** in the OpenLakehouse pipeline.

Data quality checks are executed through Apache Airflow and the results are transformed and persisted for monitoring in the Streamlit Data Quality dashboard.

## Purpose

The Soda layer is responsible for:

- validating dataset completeness;
- detecting duplicate records;
- identifying missing values;
- validating column types and schema;
- checking data validity;
- monitoring freshness and timeliness;
- recording warnings and failures;
- persisting quality results for historical monitoring.

## Data Quality Flow

```text
Bronze
  |
  v
Bronze Data Quality
  |
  v
Silver
  |
  v
Silver Data Quality
  |
  v
Gold
  |
  v
Gold Data Quality
```

## Project Location

```text
airflow/data_quality/
```

## Quality Results

Processed Soda results are persisted into:

```text
iceberg.silver.data_quality_results
```

The results are queried through Trino and displayed in the Streamlit Data Quality Monitoring dashboard.

## Example Checks

```yaml
checks for ${TABLE}:
  - row_count > 0

  - duplicate_count(customer_id) = 0

  - missing_count(customer_id) = 0

  - schema:
      fail:
        when required column missing:
          [customer_id, country]
```

## Quality Dimensions

The monitoring layer covers checks such as:

- Completeness
- Uniqueness
- Validity
- Schema
- Timeliness
- Distribution

## Technology

- [Soda Core](https://docs.soda.io/soda-core/overview.html)
- [Apache Airflow](https://airflow.apache.org/)
- [Trino](https://trino.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Streamlit](https://streamlit.io/)
