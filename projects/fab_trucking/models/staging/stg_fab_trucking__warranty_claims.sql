{{ config(materialized='view') }}

with source as (
    select * from {{ ref('warranty_claims') }}
),

staged as (
    select
        -- Primary key
        warranty_claim_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        customer_id,
        dealer_id,

        -- Claim attributes (matching canonical schema)
        claim_open_ts,
        claim_close_ts,
        claim_status,
        claimed_defect_code,
        cost_amount,
        source_system,

        -- Timestamps
        created_at

    from source
)

select * from staged
