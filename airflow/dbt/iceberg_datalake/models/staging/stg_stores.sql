select store_id,
    store_name,
    city,
    country
from {{ source('silver', 'stores') }}