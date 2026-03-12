select
    product_substance_id,
    product_id,
    substance_id,
    cast(concentration_ppm as numeric) as concentration_ppm,
    cast(last_verified_date as date) as last_verified_date,
    data_source
from {{ ref('product_substances') }}
