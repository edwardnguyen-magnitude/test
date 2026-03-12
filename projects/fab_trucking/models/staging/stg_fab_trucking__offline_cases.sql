{{ config(materialized='view') }}

with source as (
    select * from {{ ref('offline_cases') }}
),

staged as (
    select
        -- Primary key
        offline_case_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        truck_build_id,
        plant_id,

        -- Offline timing
        opened_ts::timestamp as opened_ts,
        closed_ts::timestamp as closed_ts,

        -- Classification
        offline_reason_primary,
        offline_reason_secondary,
        current_offline_status,
        responsible_function,

        -- Priority
        priority_score::decimal(5,2) as priority_score,
        priority_band,

        -- Source
        source_system,

        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
)

select * from staged
