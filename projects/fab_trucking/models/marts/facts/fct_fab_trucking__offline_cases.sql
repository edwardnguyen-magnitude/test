{{ config(materialized='table') }}

with offline_enriched as (
    select * from {{ ref('int_fab_trucking__offline_cases_enriched') }}
),

quality_status as (
    select * from {{ ref('int_fab_trucking__quality_status') }}
),

-- Aggregate shortages per offline case
shortage_agg as (
    select
        truck_build_id,
        count(*) as shortage_count,
        sum(case when is_open_shortage then 1 else 0 end) as open_shortage_count,
        max(shortage_age_days) as max_shortage_age_days,
        max(severity_score) as max_shortage_severity_score,
        array_agg(distinct part_id) filter (where part_id is not null) as shortage_part_ids,
        array_agg(distinct supplier_id) filter (where supplier_id is not null) as shortage_supplier_ids
    from {{ ref('int_fab_trucking__shortage_impact') }}
    group by truck_build_id
),

-- Aggregate approvals per offline case
approval_agg as (
    select
        offline_case_id,
        count(*) as total_approvals,
        sum(case when is_pending then 1 else 0 end) as pending_approvals,
        max(wait_hours) as max_approval_wait_hours,
        sum(wait_hours) as total_approval_wait_hours,
        array_agg(distinct approval_step_type) filter (where is_pending) as pending_approval_steps
    from {{ ref('int_fab_trucking__approval_timeline') }}
    group by offline_case_id
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['oc.offline_case_id']) }} as offline_case_key,
        oc.offline_case_id,
        oc.vin,
        oc.truck_build_id,
        oc.tenant_id,
        oc.plant_id,
        oc.configuration_id,
        oc.truck_model_id,
        oc.customer_id,

        -- Identifiers
        oc.plant_code,
        oc.plant_name,
        oc.model_code,
        oc.model_name,
        oc.configuration_code,
        oc.customer_code,
        oc.customer_name,
        oc.customer_segment,

        -- Offline timing
        oc.opened_ts,
        oc.closed_ts,
        oc.offline_age_days,
        oc.is_open,

        -- Classification
        oc.offline_reason_primary,
        oc.offline_reason_secondary,
        oc.current_offline_status,
        oc.responsible_function,

        -- Priority
        oc.priority_score,
        oc.priority_band,

        -- Derived age band based on thresholds
        case
            when oc.offline_age_days <= 1 then 'GREEN'
            when oc.offline_age_days <= 5 then 'YELLOW'
            when oc.offline_age_days <= 14 then 'RED'
            else 'BLACK'
        end as offline_age_band,

        -- Shortage info
        coalesce(sa.shortage_count, 0) as shortage_count,
        coalesce(sa.open_shortage_count, 0) as open_shortage_count,
        sa.max_shortage_age_days,
        sa.shortage_part_ids,
        sa.shortage_supplier_ids,
        sa.open_shortage_count > 0 as has_open_shortage,

        -- Quality info
        qs.qa_status,
        qs.test_status,
        qs.critical_open_defects,
        qs.major_open_defects,
        qs.open_defects as total_open_defects,

        -- Approval info
        coalesce(aa.total_approvals, 0) as total_approvals,
        coalesce(aa.pending_approvals, 0) as pending_approvals,
        aa.max_approval_wait_hours,
        aa.total_approval_wait_hours,
        aa.pending_approval_steps,

        -- Value at risk
        oc.total_truck_value,
        case
            when oc.is_open then oc.total_truck_value
            else 0
        end as revenue_at_risk,

        -- SLA tracking
        oc.offline_age_days > 1 as sla_breach_flag,
        oc.ship_delay_days,

        -- Source
        oc.source_system,

        -- Timestamps
        oc.created_at,
        oc.updated_at

    from offline_enriched oc
    left join shortage_agg sa on oc.truck_build_id = sa.truck_build_id
    left join approval_agg aa on oc.offline_case_id = aa.offline_case_id
    left join quality_status qs on oc.vin = qs.vin
)

select * from final
