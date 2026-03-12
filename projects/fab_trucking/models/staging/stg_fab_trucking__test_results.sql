{{ config(materialized='view') }}

with source as (
    select * from {{ ref('test_results') }}
),

staged as (
    select
        -- Primary key (canonical: test_result_id)
        test_result_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        plant_id,
        workcenter_id,

        -- Test attributes (matching canonical schema)
        test_type,
        test_run_id,
        start_ts,
        end_ts,
        result,
        metrics_json,
        source_system,

        -- Timestamps
        created_at

    from source
)

select * from staged
