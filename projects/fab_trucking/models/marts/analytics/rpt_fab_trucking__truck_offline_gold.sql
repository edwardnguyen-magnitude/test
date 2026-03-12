{{ config(materialized='table') }}

{#
  Main semantic view for the Fab Trucking "Last Mile" offline management application.
  This is the primary data source for the operational dashboard.
#}

with offline_cases as (
    select * from {{ ref('fct_fab_trucking__offline_cases') }}
),

final as (
    select
        -- Primary identifiers
        offline_case_id,
        offline_case_key,
        vin,
        truck_build_id,
        tenant_id,

        -- Plant context
        plant_id,
        plant_code,
        plant_name,

        -- Truck context
        truck_model_id,
        model_code,
        model_name,
        configuration_id,
        configuration_code,

        -- Customer context
        customer_id,
        customer_code,
        customer_name,
        customer_segment,

        -- Offline case status
        is_open,
        current_offline_status,
        offline_reason_primary,
        offline_reason_secondary,
        responsible_function,

        -- Timing
        opened_ts,
        closed_ts,
        offline_age_days,
        offline_age_band,

        -- Priority
        priority_score,
        priority_band,

        -- Shortage impact
        shortage_count,
        open_shortage_count,
        has_open_shortage,
        max_shortage_age_days,
        shortage_part_ids,
        shortage_supplier_ids,

        -- Quality status
        qa_status,
        test_status,
        critical_open_defects,
        major_open_defects,
        total_open_defects,

        -- Approval status
        total_approvals,
        pending_approvals,
        max_approval_wait_hours,
        total_approval_wait_hours,
        pending_approval_steps,

        -- Financial impact
        total_truck_value,
        revenue_at_risk,

        -- SLA
        sla_breach_flag,
        ship_delay_days,

        -- Composite risk score (for sorting/prioritization)
        (
            priority_score * 0.3 +
            coalesce(offline_age_days, 0) * 2 +
            coalesce(open_shortage_count, 0) * 10 +
            coalesce(critical_open_defects, 0) * 20 +
            coalesce(major_open_defects, 0) * 5 +
            coalesce(pending_approvals, 0) * 3 +
            case when sla_breach_flag then 25 else 0 end
        ) as composite_risk_score,

        -- Action required flags
        has_open_shortage as action_resolve_shortage,
        pending_approvals > 0 as action_pending_approval,
        critical_open_defects > 0 as action_critical_defect,
        sla_breach_flag as action_sla_breach,

        -- Source tracking
        source_system,
        created_at,
        updated_at

    from offline_cases
)

select * from final
