select transaction_id,
    customer_id,
    product_id,
    store_id,
    quantity,
    transaction_timestamp,
    cast(transaction_timestamp as date) as transaction_date,
    payment_method,
    "status"
from {{ source('silver', 'sales') }}