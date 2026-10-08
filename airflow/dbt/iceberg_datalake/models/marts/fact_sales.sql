{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id'
    )
}}
with sales as (
    SELECT *
    FROM {{ ref('stg_sales') }}
    {% if is_incremental() %}

        WHERE transaction_date >= (SELECT max(transaction_date) FROM {{ this }})

    {% endif %}
)
, transactions as (
    select s.transaction_id,
        s.transaction_timestamp,
        s.transaction_date,
        s.customer_id,
        s.product_id,
        p.product_name,
        p.category,
        s.store_id,
        st.store_name,
        st.city as store_city,
        st.country as store_country,
        s.quantity,
        -- historical product price
        p.price as unit_price,
        -- calculated sales value
        s.quantity * p.price as total_amount,
        s.payment_method,
        s.status
    from  sales s
        left join {{ ref('stg_stores') }} st on s.store_id = st.store_id
        left join {{ ref('stg_products') }} p on s.product_id = p.product_id
        and s.transaction_date >= p.effective_start
        and (
            s.transaction_date < p.effective_end
            or p.effective_end is null
        )
)
select *
from transactions
where unit_price is not null
    