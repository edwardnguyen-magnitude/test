select
    list_id,
    list_code,
    list_name,
    upper(region) as region,
    authority,
    cast(last_updated as date) as last_updated,
    cast(substance_count as integer) as substance_count
from {{ ref('regulatory_lists') }}
