{{ config(materialized='table') }}

{#
  Plant-level performance metrics for offline management.
  Provides KPIs for plant operations and quality teams.
#}

with truck_builds as (
    select * from {{ ref('fct_fab_trucking__truck_builds') }}
),

offline_cases as (
    select * from {{ ref('fct_fab_trucking__offline_cases') }}
),

inspections as (
    select * from {{ ref('fct_fab_trucking__inspections') }}
),

defects as (
    select * from {{ ref('fct_fab_trucking__defects') }}
),

plants as (
    select * from {{ ref('dim_fab_trucking__plants') }}
),

-- Build metrics by plant
build_metrics as (
    select
        plant_id,
        count(*) as total_builds,
        sum(case when is_offline then 1 else 0 end) as offline_builds,
        sum(case when is_delivered then 1 else 0 end) as delivered_builds,
        sum(case when is_shipped then 1 else 0 end) as shipped_builds,
        avg(build_duration_hours) as avg_build_duration_hours,
        avg(build_delay_days) filter (where build_delay_days > 0) as avg_build_delay_days,
        sum(truck_value) as total_truck_value,
        sum(case when is_offline then truck_value else 0 end) as offline_truck_value
    from truck_builds
    group by plant_id
),

-- Offline metrics by plant
offline_metrics as (
    select
        plant_id,
        count(*) as total_offline_cases,
        sum(case when is_open then 1 else 0 end) as open_offline_cases,
        avg(offline_age_days) filter (where is_open) as avg_open_case_age_days,
        max(offline_age_days) filter (where is_open) as max_open_case_age_days,
        sum(case when sla_breach_flag and is_open then 1 else 0 end) as sla_breaches,
        sum(case when is_open then revenue_at_risk else 0 end) as total_revenue_at_risk,

        -- Reason breakdown
        sum(case when offline_reason_primary = 'SHORTAGE' and is_open then 1 else 0 end) as open_shortage_cases,
        sum(case when offline_reason_primary = 'QUALITY' and is_open then 1 else 0 end) as open_quality_cases,
        sum(case when offline_reason_primary = 'ENGINEERING' and is_open then 1 else 0 end) as open_engineering_cases

    from offline_cases
    group by plant_id
),

-- Inspection metrics by plant
inspection_metrics as (
    select
        plant_id,
        count(*) as total_inspections,
        sum(case when is_passed then 1 else 0 end) as passed_inspections,
        sum(case when is_failed then 1 else 0 end) as failed_inspections,
        round(100.0 * sum(case when is_passed then 1 else 0 end) / nullif(count(*), 0), 2) as first_pass_rate,
        sum(defect_count) as total_defects_found,
        avg(defect_count) filter (where defect_count > 0) as avg_defects_per_failed_inspection
    from inspections
    group by plant_id
),

-- Defect metrics by plant
defect_metrics as (
    select
        plant_id,
        count(*) as total_defects,
        sum(case when is_open then 1 else 0 end) as open_defects,
        sum(case when is_critical and is_open then 1 else 0 end) as open_critical_defects,
        sum(case when is_major and is_open then 1 else 0 end) as open_major_defects,
        avg(repair_duration_hours) filter (where repair_duration_hours is not null) as avg_repair_hours
    from defects
    group by plant_id
),

final as (
    select
        p.plant_key,
        p.plant_id,
        p.plant_code,
        p.plant_name,
        p.location_country as country,
        p.location_region as region,
        p.plant_type,
        p.is_active,

        -- Build KPIs
        coalesce(bm.total_builds, 0) as total_builds,
        coalesce(bm.offline_builds, 0) as offline_builds,
        coalesce(bm.delivered_builds, 0) as delivered_builds,
        round(100.0 * coalesce(bm.offline_builds, 0) / nullif(bm.total_builds, 0), 2) as offline_rate_pct,
        bm.avg_build_duration_hours,
        bm.avg_build_delay_days,
        coalesce(bm.total_truck_value, 0) as total_truck_value,
        coalesce(bm.offline_truck_value, 0) as offline_truck_value,

        -- Offline KPIs
        coalesce(om.total_offline_cases, 0) as total_offline_cases,
        coalesce(om.open_offline_cases, 0) as open_offline_cases,
        om.avg_open_case_age_days,
        om.max_open_case_age_days,
        coalesce(om.sla_breaches, 0) as sla_breaches,
        coalesce(om.total_revenue_at_risk, 0) as total_revenue_at_risk,
        coalesce(om.open_shortage_cases, 0) as open_shortage_cases,
        coalesce(om.open_quality_cases, 0) as open_quality_cases,
        coalesce(om.open_engineering_cases, 0) as open_engineering_cases,

        -- Quality KPIs
        coalesce(im.total_inspections, 0) as total_inspections,
        coalesce(im.first_pass_rate, 0) as first_pass_rate,
        coalesce(im.total_defects_found, 0) as inspection_defects_found,
        coalesce(dm.total_defects, 0) as total_defects,
        coalesce(dm.open_defects, 0) as open_defects,
        coalesce(dm.open_critical_defects, 0) as open_critical_defects,
        coalesce(dm.open_major_defects, 0) as open_major_defects,
        dm.avg_repair_hours,

        -- Overall plant health score (0-100)
        greatest(0, least(100,
            100
            - coalesce(om.open_offline_cases, 0) * 2
            - coalesce(om.sla_breaches, 0) * 5
            - coalesce(dm.open_critical_defects, 0) * 10
            - coalesce(dm.open_major_defects, 0) * 3
            + coalesce(im.first_pass_rate, 0) * 0.5
        )) as plant_health_score,

        -- Plant status
        case
            when coalesce(om.sla_breaches, 0) > 5 or coalesce(dm.open_critical_defects, 0) > 3 then 'CRITICAL'
            when coalesce(om.open_offline_cases, 0) > 10 or coalesce(dm.open_major_defects, 0) > 5 then 'AT_RISK'
            when coalesce(om.open_offline_cases, 0) > 5 then 'MONITOR'
            else 'HEALTHY'
        end as plant_status

    from plants p
    left join build_metrics bm on p.plant_id = bm.plant_id
    left join offline_metrics om on p.plant_id = om.plant_id
    left join inspection_metrics im on p.plant_id = im.plant_id
    left join defect_metrics dm on p.plant_id = dm.plant_id
)

select * from final
order by plant_health_score asc
