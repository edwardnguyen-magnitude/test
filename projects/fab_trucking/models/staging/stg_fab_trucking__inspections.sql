{{ config(materialized='view') }}

with source as (
    select * from {{ ref('inspections') }}
),

staged as (
    select
        -- Primary key (canonical: inspection_event_id)
        inspection_event_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        plant_id,
        workcenter_id,

        -- Inspection attributes (matching canonical schema)
        inspection_type,
        scheduled_ts,
        started_ts,
        completed_ts,
        inspection_result,
        defect_count,
        defect_severity_max,
        inspector_employee_id,
        source_system,

        -- Timestamps
        created_at

    from source
)

select * from staged
