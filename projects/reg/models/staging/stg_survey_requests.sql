select
    survey_id,
    customer_id,
    upper(region) as region,
    survey_type,
    complexity,
    cast(product_count as integer) as product_count,
    status,
    cast(requested_date as date) as requested_date,
    cast(due_date as date) as due_date,
    cast(nullif(completed_date::text, '') as date) as completed_date,
    business_unit
from {{ ref('survey_requests') }}
