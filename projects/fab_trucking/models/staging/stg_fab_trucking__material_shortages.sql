{{ config(materialized='view') }}

with source as (
    select * from {{ ref('material_shortages') }}
),

staged as (
    select
        -- Primary key
        shortage_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        truck_build_id,
        plant_id,
        part_id,
        supplier_id,

        -- Shortage timing
        shortage_detected_ts::timestamp as shortage_detected_ts,
        shortage_resolved_ts::timestamp as shortage_resolved_ts,
        expected_arrival_ts::timestamp as expected_arrival_ts,

        -- Classification
        shortage_status,
        shortage_type,
        shortage_severity,

        -- Quantities
        qty_required::integer as qty_required,
        qty_available::integer as qty_available,

        -- Source document
        source_doc_type,
        source_doc_id,

        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
)

select * from staged
