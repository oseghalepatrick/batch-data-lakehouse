select customer_id,
    customer_name,
    email,
    phone,
    city,
    country,
    segment,
    signup_date,
    effective_start,
    effective_end,
    is_current
from {{ source('silver', 'customers') }}