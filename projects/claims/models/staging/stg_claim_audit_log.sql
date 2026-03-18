select
    audit_id,
    claim_id,
    trim(action) as action,
    cast(action_date as timestamp) as action_date,
    trim(performed_by) as performed_by,
    trim(role) as role,
    nullif(trim(previous_status), '') as previous_status,
    trim(new_status) as new_status,
    trim(notes) as notes,
    nullif(trim(routing_rule), '') as routing_rule

from {{ source('raw', 'fact_claim_audit_log') }}
