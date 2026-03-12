{{ config(materialized='view') }}

with approvals as (
    select * from {{ ref('stg_fab_trucking__approvals') }}
),

offline_cases as (
    select * from {{ ref('stg_fab_trucking__offline_cases') }}
),

approval_metrics as (
    select
        a.approval_event_id,
        a.tenant_id,
        a.offline_case_id,
        a.vin,
        a.approval_step_type,
        a.requested_ts,
        a.approved_ts,
        a.rejected_ts,
        a.approver_role,
        a.approver_employee_id,
        a.decision,
        a.comments,
        a.source_system,

        -- Related offline case info
        oc.offline_reason_primary,
        oc.current_offline_status,
        oc.priority_band,

        -- Calculate wait time in hours
        case
            when a.approved_ts is not null
            then extract(epoch from (a.approved_ts::timestamp - a.requested_ts::timestamp)) / 3600
            when a.rejected_ts is not null
            then extract(epoch from (a.rejected_ts::timestamp - a.requested_ts::timestamp)) / 3600
            else extract(epoch from (current_timestamp - a.requested_ts::timestamp)) / 3600
        end as wait_hours,

        -- Is still pending
        case
            when a.decision = 'PENDING' then true
            else false
        end as is_pending,

        -- Is approved
        case when a.decision = 'APPROVED' then true else false end as is_approved,

        -- Is rejected
        case when a.decision = 'REJECTED' then true else false end as is_rejected,

        -- Wait time band
        case
            when a.approved_ts is not null or a.rejected_ts is not null then
                case
                    when extract(epoch from (coalesce(a.approved_ts::timestamp, a.rejected_ts::timestamp) - a.requested_ts::timestamp)) / 3600 <= 4 then 'GREEN'
                    when extract(epoch from (coalesce(a.approved_ts::timestamp, a.rejected_ts::timestamp) - a.requested_ts::timestamp)) / 3600 <= 24 then 'YELLOW'
                    else 'RED'
                end
            else
                case
                    when extract(epoch from (current_timestamp - a.requested_ts::timestamp)) / 3600 <= 4 then 'GREEN'
                    when extract(epoch from (current_timestamp - a.requested_ts::timestamp)) / 3600 <= 24 then 'YELLOW'
                    else 'RED'
                end
        end as wait_band,

        a.created_at

    from approvals a
    left join offline_cases oc on a.offline_case_id = oc.offline_case_id
)

select * from approval_metrics
