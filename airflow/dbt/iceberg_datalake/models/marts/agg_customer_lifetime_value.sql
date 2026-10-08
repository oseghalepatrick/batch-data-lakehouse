with sales_customer_history as (
    select s.transaction_id,
        s.customer_id,
        s.transaction_date,
        s.transaction_timestamp,
        s.status,
        s.total_amount,
        s.quantity,
        c.customer_name,
        c.email,
        c.phone,
        c.city,
        c.country,
        c.segment,
        c.effective_start,
        c.effective_end
    from {{ ref('fact_sales') }} s
        inner join {{ ref('stg_customers') }} c on s.customer_id = c.customer_id
        and s.transaction_date >= c.effective_start
        and (
            s.transaction_date < c.effective_end
            or c.effective_end is null
        )
)
    select customer_id,
        customer_name,
        city,
        country,
        segment,
        effective_start,
        effective_end,
        count(distinct transaction_id) as total_orders,
        sum(total_amount) as lifetime_value,
        avg(total_amount) as average_order_value,
        sum(quantity) as total_items_purchased,
        min(transaction_date) as first_purchase_date,
        max(transaction_date) as last_purchase_date
    from sales_customer_history
    where status = 'completed'
    group by customer_id,
        customer_name,
        city,
        country,
        segment,
        effective_start,
        effective_end