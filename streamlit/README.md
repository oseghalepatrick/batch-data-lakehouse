# Streamlit Sales Dashboard

This dashboard reads the dbt marts from Trino:

- fact_sales
- agg_product_performance
- agg_daily_sales
- agg_customer_lifetime_value

## Run locally

Install dependencies:

```bash
pip install -r requirements.txt
```

If Trino is running in Docker but Streamlit is running on the host:

```bash
export TRINO_HOST=localhost
export TRINO_PORT=8089
export TRINO_CATALOG=iceberg
export TRINO_SCHEMA=gold

streamlit run streamlit_app.py
```

If Streamlit runs in the same Docker network as Trino:

```bash
export TRINO_HOST=trino-coordinator
export TRINO_PORT=8089
export TRINO_CATALOG=iceberg
export TRINO_SCHEMA=gold

streamlit run streamlit_app.py
```

If the marts currently exist in `silver_gold`, temporarily use:

```bash
export TRINO_SCHEMA=silver_gold
```

## Dashboard sections

1. Revenue, orders, units, AOV, customers and products
2. Daily revenue and order trend
3. Top products
4. Category performance
5. Store performance
6. Payment-method revenue mix
7. Customer lifetime value
8. Customer historical city value
9. Customer SCD Type II detail

The dashboard caches query results for five minutes.

## Mart usage

The dashboard intentionally uses the dbt marts where their grain supports the visual:

- `agg_daily_sales` for daily trends and store performance
- `agg_product_performance` for product and category performance
- `agg_customer_lifetime_value` for customer value and SCD Type II city history
- `fact_sales` for transaction-level metrics and payment-method analysis

`agg_product_performance` is an aggregate over completed sales and therefore represents the available aggregate period rather than the sidebar date range.
