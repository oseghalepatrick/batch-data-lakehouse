select store_id,
    store_name,
    city,
    country,
    region
from {{ source('silver', 'dim_stores') }}