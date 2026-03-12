{{ config(materialized='table') }}

{#
  Supplier impact analysis for material shortages.
  Helps identify problematic suppliers and their impact on offline trucks.
#}

with shortages as (
    select * from {{ ref('fct_fab_trucking__material_shortages') }}
),

offline_cases as (
    select * from {{ ref('fct_fab_trucking__offline_cases') }}
),

-- Get offline context for each shortage
shortage_with_context as (
    select
        s.*,
        oc.is_open as case_is_open,
        oc.offline_age_days as case_offline_age_days,
        oc.revenue_at_risk as case_revenue_at_risk,
        oc.priority_band as case_priority_band
    from shortages s
    left join offline_cases oc on s.truck_build_id = oc.truck_build_id
),

-- Aggregate by supplier
supplier_summary as (
    select
        supplier_id,

        -- Shortage counts
        count(*) as total_shortages,
        sum(case when is_open_shortage then 1 else 0 end) as open_shortages,
        sum(case when not is_open_shortage then 1 else 0 end) as resolved_shortages,

        -- Severity breakdown
        sum(case when severity_band = 'CRITICAL' then 1 else 0 end) as critical_shortages,
        sum(case when severity_band = 'HIGH' then 1 else 0 end) as high_shortages,
        sum(case when severity_band = 'MEDIUM' then 1 else 0 end) as medium_shortages,
        sum(case when severity_band = 'LOW' then 1 else 0 end) as low_shortages,

        -- Part analysis
        count(distinct part_id) as unique_parts_affected,

        -- Truck impact
        count(distinct truck_build_id) as trucks_affected,
        count(distinct case when case_is_open then truck_build_id end) as trucks_currently_offline,

        -- Financial impact
        sum(case when case_is_open then case_revenue_at_risk else 0 end) as current_revenue_at_risk,

        -- Timing metrics
        avg(shortage_age_days) filter (where is_open_shortage) as avg_open_shortage_age_days,
        max(shortage_age_days) filter (where is_open_shortage) as max_open_shortage_age_days,
        avg(days_until_arrival) filter (where is_open_shortage and days_until_arrival is not null) as avg_days_until_arrival,

        -- Quantity metrics
        sum(quantity_short) as total_quantity_short,
        sum(case when is_open_shortage then quantity_short else 0 end) as open_quantity_short,

        -- Critical parts
        sum(case when is_critical_part and is_open_shortage then 1 else 0 end) as critical_parts_short

    from shortage_with_context
    group by supplier_id
),

final as (
    select
        *,

        -- Supplier risk score
        (
            open_shortages * 10 +
            critical_shortages * 25 +
            high_shortages * 10 +
            trucks_currently_offline * 5 +
            coalesce(avg_open_shortage_age_days, 0) * 2 +
            critical_parts_short * 30
        ) as supplier_risk_score,

        -- Performance tier
        case
            when open_shortages = 0 then 'A - No Open Issues'
            when critical_shortages > 0 or trucks_currently_offline > 5 then 'D - Critical'
            when high_shortages > 2 or trucks_currently_offline > 2 then 'C - At Risk'
            when open_shortages > 0 then 'B - Monitor'
            else 'A - No Open Issues'
        end as supplier_tier

    from supplier_summary
)

select * from final
order by supplier_risk_score desc
