select product_id,
    product_name,
    category,
    price,
    effective_start,
    effective_end,
    is_current
from {{ source('silver', 'products') }}