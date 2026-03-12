{{ config(materialized='table') }}

{#
  Aggregated backlog dashboard for offline truck management.
  Provides plant-level and age-band summaries for executive reporting.
#}

with offline_cases as (
    select * from {{ ref('fct_fab_trucking__offline_cases') }}
    where is_open = true
),

-- Plant-level summary
plant_summary as (
    select
        plant_id,
        plant_code,
        plant_name,

        count(*) as total_open_cases,

        -- Age band breakdown
        sum(case when offline_age_band = 'GREEN' then 1 else 0 end) as green_count,
        sum(case when offline_age_band = 'YELLOW' then 1 else 0 end) as yellow_count,
        sum(case when offline_age_band = 'RED' then 1 else 0 end) as red_count,
        sum(case when offline_age_band = 'BLACK' then 1 else 0 end) as black_count,

        -- Priority breakdown
        sum(case when priority_band = 'CRITICAL' then 1 else 0 end) as critical_priority_count,
        sum(case when priority_band = 'HIGH' then 1 else 0 end) as high_priority_count,
        sum(case when priority_band = 'MEDIUM' then 1 else 0 end) as medium_priority_count,
        sum(case when priority_band = 'LOW' then 1 else 0 end) as low_priority_count,

        -- Issue breakdown
        sum(case when has_open_shortage then 1 else 0 end) as shortage_blocked_count,
        sum(case when pending_approvals > 0 then 1 else 0 end) as approval_blocked_count,
        sum(case when critical_open_defects > 0 then 1 else 0 end) as critical_defect_count,
        sum(case when sla_breach_flag then 1 else 0 end) as sla_breach_count,

        -- Metrics
        avg(offline_age_days) as avg_offline_days,
        max(offline_age_days) as max_offline_days,
        sum(revenue_at_risk) as total_revenue_at_risk,

        -- Reason breakdown
        sum(case when offline_reason_primary = 'SHORTAGE' then 1 else 0 end) as shortage_reason_count,
        sum(case when offline_reason_primary = 'QUALITY' then 1 else 0 end) as quality_reason_count,
        sum(case when offline_reason_primary = 'ENGINEERING' then 1 else 0 end) as engineering_reason_count,
        sum(case when offline_reason_primary = 'CUSTOMER' then 1 else 0 end) as customer_reason_count

    from offline_cases
    group by plant_id, plant_code, plant_name
),

-- Overall summary
overall_summary as (
    select
        'ALL' as plant_id,
        'ALL' as plant_code,
        'All Plants' as plant_name,

        count(*) as total_open_cases,

        sum(case when offline_age_band = 'GREEN' then 1 else 0 end) as green_count,
        sum(case when offline_age_band = 'YELLOW' then 1 else 0 end) as yellow_count,
        sum(case when offline_age_band = 'RED' then 1 else 0 end) as red_count,
        sum(case when offline_age_band = 'BLACK' then 1 else 0 end) as black_count,

        sum(case when priority_band = 'CRITICAL' then 1 else 0 end) as critical_priority_count,
        sum(case when priority_band = 'HIGH' then 1 else 0 end) as high_priority_count,
        sum(case when priority_band = 'MEDIUM' then 1 else 0 end) as medium_priority_count,
        sum(case when priority_band = 'LOW' then 1 else 0 end) as low_priority_count,

        sum(case when has_open_shortage then 1 else 0 end) as shortage_blocked_count,
        sum(case when pending_approvals > 0 then 1 else 0 end) as approval_blocked_count,
        sum(case when critical_open_defects > 0 then 1 else 0 end) as critical_defect_count,
        sum(case when sla_breach_flag then 1 else 0 end) as sla_breach_count,

        avg(offline_age_days) as avg_offline_days,
        max(offline_age_days) as max_offline_days,
        sum(revenue_at_risk) as total_revenue_at_risk,

        sum(case when offline_reason_primary = 'SHORTAGE' then 1 else 0 end) as shortage_reason_count,
        sum(case when offline_reason_primary = 'QUALITY' then 1 else 0 end) as quality_reason_count,
        sum(case when offline_reason_primary = 'ENGINEERING' then 1 else 0 end) as engineering_reason_count,
        sum(case when offline_reason_primary = 'CUSTOMER' then 1 else 0 end) as customer_reason_count

    from offline_cases
),

final as (
    select * from plant_summary
    union all
    select * from overall_summary
)

select * from final
