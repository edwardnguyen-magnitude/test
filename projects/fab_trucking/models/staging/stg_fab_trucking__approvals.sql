{{ config(materialized='view') }}

with source as (
    select * from {{ ref('approvals') }}
),

staged as (
    select
        -- Primary key (canonical: approval_event_id)
        approval_event_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        offline_case_id,
        vin,

        -- Approval attributes (matching canonical schema)
        approval_step_type,
        requested_ts::timestamp as requested_ts,
        -- Handle empty approved_ts/rejected_ts (empty string may load as different types)
        -- Cast to text first, check for empty or '0', then cast non-empty to timestamp
        case
            when approved_ts::text = '' or approved_ts::text = '0' or approved_ts is null then null
            else approved_ts::text::timestamp
        end as approved_ts,
        case
            when rejected_ts::text = '' or rejected_ts::text = '0' or rejected_ts is null then null
            else rejected_ts::text::timestamp
        end as rejected_ts,
        approver_role,
        approver_employee_id,
        decision,
        comments,
        source_system,

        -- Timestamps
        created_at

    from source
)

select * from staged
