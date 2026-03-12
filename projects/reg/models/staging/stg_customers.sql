select
    customer_id,
    trim(customer_name) as customer_name,
    upper(region) as region,
    industry,
    lower(contact_email) as contact_email,
    cast(active_since as date) as active_since_date
from {{ ref('customers') }}
