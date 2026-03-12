{{ config(materialized='table') }}

with approval_timeline as (
    select * from {{ ref('int_fab_trucking__approval_timeline') }}
),

offline_cases as (
    select * from {{ ref('stg_fab_trucking__offline_cases') }}
),

trucks as (
    select * from {{ ref('stg_fab_trucking__trucks') }}
),

plants as (
    select * from {{ ref('dim_fab_trucking__plants') }}
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['at.approval_event_id']) }} as approval_key,
        at.approval_event_id,
        at.offline_case_id,
        oc.vin,
        t.truck_build_id,
        at.tenant_id,

        -- Plant info
        p.plant_key,
        t.plant_id,
        p.plant_code,
        p.plant_name,

        -- Approval details (canonical column names)
        at.approval_step_type,
        at.approver_employee_id,
        at.approver_role,
        at.requested_ts,
        at.approved_ts,
        at.rejected_ts,
        at.requested_ts::date as requested_date,

        -- Status
        at.decision,
        at.is_pending,
        at.is_approved,
        at.is_rejected,

        -- Wait time analysis
        at.wait_hours,
        at.wait_band,

        -- SLA tracking
        case
            when at.approval_step_type = 'QA_SIGNOFF' then 4
            when at.approval_step_type = 'ENGINEERING_REVIEW' then 8
            when at.approval_step_type = 'MATERIALS_CONFIRMATION' then 2
            when at.approval_step_type = 'BUSINESS_RELEASE' then 24
            else 8
        end as sla_hours,

        case
            when at.wait_hours is not null then
                at.wait_hours > case
                    when at.approval_step_type = 'QA_SIGNOFF' then 4
                    when at.approval_step_type = 'ENGINEERING_REVIEW' then 8
                    when at.approval_step_type = 'MATERIALS_CONFIRMATION' then 2
                    when at.approval_step_type = 'BUSINESS_RELEASE' then 24
                    else 8
                end
            else null
        end as sla_breached,

        -- Comments
        at.comments,

        -- Source
        at.source_system,

        -- Timestamps
        at.created_at

    from approval_timeline at
    left join offline_cases oc on at.offline_case_id = oc.offline_case_id
    left join trucks t on oc.vin = t.vin
    left join plants p on t.plant_id = p.plant_id
)

select * from final
