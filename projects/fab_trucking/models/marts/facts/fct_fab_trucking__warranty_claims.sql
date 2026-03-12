{{ config(materialized='table') }}

with warranty_claims as (
    select * from {{ ref('stg_fab_trucking__warranty_claims') }}
),

trucks as (
    select * from {{ ref('stg_fab_trucking__trucks') }}
),

customers as (
    select * from {{ ref('dim_fab_trucking__customers') }}
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['wc.warranty_claim_id']) }} as warranty_claim_key,
        wc.warranty_claim_id,
        wc.vin,
        t.truck_build_id,
        wc.tenant_id,

        -- Customer info
        c.customer_key,
        wc.customer_id,
        c.customer_code,
        c.customer_name,
        c.customer_segment,

        -- Dealer info
        wc.dealer_id,

        -- Claim details (canonical column names)
        wc.claim_open_ts,
        wc.claim_close_ts,
        wc.claim_open_ts::date as claim_open_date,
        wc.claim_close_ts::date as claim_close_date,

        -- Duration calculations
        case
            when wc.claim_close_ts is not null then
                extract(epoch from (wc.claim_close_ts - wc.claim_open_ts)) / 86400
            else
                extract(epoch from (current_timestamp - wc.claim_open_ts)) / 86400
        end as claim_age_days,

        -- Status
        wc.claim_status,
        wc.claim_status = 'OPEN' as is_open,
        wc.claim_status = 'IN_REVIEW' as is_in_review,
        wc.claim_status = 'CLOSED' as is_closed,
        wc.claim_status = 'DENIED' as is_denied,

        -- Defect and cost
        wc.claimed_defect_code,
        wc.cost_amount,

        -- Source
        wc.source_system,

        -- Timestamps
        wc.created_at

    from warranty_claims wc
    left join trucks t on wc.vin = t.vin
    left join customers c on wc.customer_id = c.customer_id
)

select * from final
