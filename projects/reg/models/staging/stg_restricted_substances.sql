select
    substance_id,
    trim(substance_name) as substance_name,
    cas_number,
    list_id,
    cast(threshold_ppm as numeric) as threshold_ppm,
    restriction_type,
    hazard_class
from {{ ref('restricted_substances') }}
