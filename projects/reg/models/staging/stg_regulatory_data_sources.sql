select
    source_id,
    trim(source_name) as source_name,
    source_type,
    upper(region) as region,
    url,
    update_frequency,
    cast(last_checked as date) as last_checked
from {{ ref('regulatory_data_sources') }}
