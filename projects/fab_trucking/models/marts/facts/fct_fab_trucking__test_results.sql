{{ config(materialized='table') }}

with test_results as (
    select * from {{ ref('stg_fab_trucking__test_results') }}
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
        {{ dbt_utils.generate_surrogate_key(['tr.test_result_id']) }} as test_result_key,
        tr.test_result_id,
        tr.test_run_id,
        tr.vin,
        t.truck_build_id,
        tr.tenant_id,

        -- Plant info
        p.plant_key,
        tr.plant_id,
        p.plant_code,
        p.plant_name,

        -- Workcenter info
        tr.workcenter_id,

        -- Test details (canonical column names)
        tr.test_type,
        tr.start_ts,
        tr.end_ts,
        tr.start_ts::date as test_date,

        -- Duration
        case
            when tr.end_ts is not null and tr.start_ts is not null
            then extract(epoch from (tr.end_ts - tr.start_ts)) / 60
            else null
        end as test_duration_minutes,

        -- Results
        tr.result,
        tr.result = 'PASS' as is_passed,
        tr.result = 'FAIL' as is_failed,
        tr.result = 'INCOMPLETE' as is_incomplete,

        -- Metrics
        tr.metrics_json,

        -- Source
        tr.source_system,

        -- Timestamps
        tr.created_at

    from test_results tr
    left join trucks t on tr.vin = t.vin
    left join plants p on tr.plant_id = p.plant_id
)

select * from final
