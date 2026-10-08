{{ config(
    materialized = 'table',
    table_type = 'iceberg'
) }}
select s.product_id,
    s.product_name,
    s.category,
    count(distinct transaction_id) as total_transactions,
    count(distinct customer_id) as unique_customers,
    sum(quantity) as units_sold,
    sum(total_amount) as total_revenue,
    avg(unit_price) as average_selling_price
from {{ ref('fact_sales') }} s
where status = 'completed'
group by product_id,
    product_name,
    category