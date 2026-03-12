select
    product_id,
    product_code,
    trim(product_description) as product_description,
    business_unit,
    product_category,
    cas_registry
from {{ ref('products') }}
