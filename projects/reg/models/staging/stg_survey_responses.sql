select
    response_id,
    survey_id,
    product_id,
    substance_id,
    compliance_status,
    cast(declared_concentration_ppm as numeric) as declared_concentration_ppm,
    cast(nullif(response_date::text, '') as date) as response_date,
    analyst
from {{ ref('survey_responses') }}
