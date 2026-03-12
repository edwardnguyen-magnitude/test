{{ config(materialized='view') }}

with source as (
    select * from {{ ref('defects') }}
),

staged as (
    select
        -- Primary key (canonical: defect_id)
        defect_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        part_id,
        supplier_id,

        -- Defect attributes (matching canonical schema)
        defect_code,
        defect_description,
        defect_category,
        severity,
        detected_at_stage,
        detected_ts,
        resolved_ts,

        -- Timestamps
        created_at

    from source
)

select * from staged
