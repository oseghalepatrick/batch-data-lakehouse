# Streamlit Analytics Dashboard

This directory contains the Streamlit application used to visualise the Gold analytics models and persisted data quality results in OpenLakehouse.

The application connects to Trino and provides two main views:

- **Sales Analytics**
- **Data Quality Monitoring**

## Data Sources

### Sales Analytics

The Sales Analytics dashboard primarily queries the Gold dbt models:

```text
iceberg.gold.agg_daily_sales
iceberg.gold.agg_product_performance
iceberg.gold.agg_customer_lifetime_value
iceberg.gold.fact_sales
```

The dashboard uses the aggregate models wherever their grain supports the required visual or KPI.

### Data Quality Monitoring

The Data Quality dashboard queries:

```text
iceberg.silver.data_quality_results
```

This table stores processed Soda check results from Bronze, Silver, and Gold quality scans.

## Run Locally

Install dependencies:

```bash
pip install -r requirements.txt
```

If Trino is running in Docker and Streamlit is running on the host:

```bash
export TRINO_HOST=localhost
export TRINO_PORT=8089
export TRINO_USER=airflow
export TRINO_CATALOG=iceberg
export TRINO_SCHEMA=gold

streamlit run app.py
```

If Streamlit runs in the same Docker network as Trino:

```bash
export TRINO_HOST=trino
export TRINO_PORT=8089
export TRINO_USER=airflow
export TRINO_CATALOG=iceberg
export TRINO_SCHEMA=gold

streamlit run app.py
```

## Sales Analytics

The Sales Analytics dashboard provides:

1. Revenue, orders, units sold, average order value, customers, and products
2. Revenue trends
3. Orders by country
4. Region and country filtering
5. Product performance
6. Category performance
7. Store performance
8. Payment-method analysis
9. Customer lifetime value
10. Customer historical city value
11. Customer SCD Type II detail with pagination

The dashboard supports interactive filters including date range, region, country, store, category, product, payment method, and sales status.

Region and country filters are cascading, so the available countries are restricted by the selected region or regions.

## Data Quality Monitoring

The Data Quality dashboard provides:

1. Pass, warning, and failure KPIs
2. Data quality score by layer
3. Quality score by dimension
4. Quality trends over time
5. Check status history
6. Failing-table analysis
7. Failure-type analysis
8. Schema diagnostics
9. Scan duration monitoring
10. Paginated check-level results

The monitoring layer includes dimensions such as:

```text
Completeness
Uniqueness
Validity
Schema
Timeliness
Distribution
```

## Mart Usage

The dashboard uses the dbt marts according to their grain:

- `agg_daily_sales` for daily sales analysis, store performance, geography, payment method, and dashboard-level filtering
- `agg_product_performance` for product and category performance
- `agg_customer_lifetime_value` for customer value and SCD Type II city history
- `fact_sales` only where transaction-level detail is required

The dashboard does not sum non-additive metrics such as `unique_customers` across aggregate rows. Derived metrics such as average order value are recalculated from revenue and transaction totals where required.

## Query Caching

Streamlit query results are cached for five minutes to reduce repeated queries against Trino.

Use the **Refresh data** control in the sidebar to clear the cache and reload the latest results.

## Default Connection

The application expects the following default configuration:

```text
TRINO_HOST=trino
TRINO_PORT=8089
TRINO_USER=airflow
TRINO_CATALOG=iceberg
TRINO_SCHEMA=gold
```

These values can be overridden through environment variables.
