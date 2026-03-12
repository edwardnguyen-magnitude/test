{{ config(materialized='view') }}

with shortages as (
    select * from {{ ref('stg_fab_trucking__material_shortages') }}
),

offline_cases as (
    select * from {{ ref('stg_fab_trucking__offline_cases') }}
),

shortage_with_case as (
    select
        s.shortage_id,
        s.tenant_id,
        s.vin,
        s.truck_build_id,
        s.plant_id,
        s.part_id,
        s.supplier_id,

        -- Shortage details
        s.shortage_detected_ts,
        s.shortage_resolved_ts,
        s.expected_arrival_ts,
        s.shortage_status,
        s.shortage_type,
        s.shortage_severity,
        s.qty_required,
        s.qty_available,
        s.qty_required - s.qty_available as qty_short,
        s.source_doc_type,
        s.source_doc_id,

        -- Calculate shortage age
        case
            when s.shortage_resolved_ts is not null
            then extract(epoch from (s.shortage_resolved_ts - s.shortage_detected_ts)) / 86400
            else extract(epoch from (current_timestamp - s.shortage_detected_ts)) / 86400
        end as shortage_age_days,

        -- Is shortage still open
        case
            when s.shortage_status in ('OPEN', 'IN_TRANSIT') then true
            else false
        end as is_open_shortage,

        -- Related offline case
        oc.offline_case_id,
        oc.offline_reason_primary,
        oc.current_offline_status as offline_status,
        oc.priority_band as offline_priority_band,

        -- Calculate days until expected arrival
        case
            when s.expected_arrival_ts is not null and s.shortage_resolved_ts is null
            then extract(epoch from (s.expected_arrival_ts - current_timestamp)) / 86400
            else null
        end as days_until_arrival,

        -- Severity score for aggregation
        case s.shortage_severity
            when 'CRITICAL' then 4
            when 'HIGH' then 3
            when 'MEDIUM' then 2
            when 'LOW' then 1
            else 0
        end as severity_score,

        s.created_at,
        s.updated_at

    from shortages s
    left join offline_cases oc
        on s.truck_build_id = oc.truck_build_id
        and oc.offline_reason_primary = 'PART_SHORTAGE'
)

select * from shortage_with_case
