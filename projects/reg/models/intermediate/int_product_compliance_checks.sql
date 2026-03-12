-- Per-product, per-substance, per-regulation compliance check
-- Determines whether each product-substance concentration exceeds the regulatory threshold

with products as (
    select * from {{ ref('stg_products') }}
),

product_subs as (
    select * from {{ ref('stg_product_substances') }}
),

substances as (
    select * from {{ ref('stg_restricted_substances') }}
),

reg_lists as (
    select * from {{ ref('stg_regulatory_lists') }}
),

checks as (
    select
        p.product_id,
        p.product_description,
        p.business_unit,
        p.product_category,
        ps.product_substance_id,
        ps.concentration_ppm as actual_concentration_ppm,
        ps.last_verified_date,
        ps.data_source,
        s.substance_id,
        s.substance_name,
        s.cas_number,
        s.hazard_class,
        s.restriction_type,
        s.threshold_ppm,
        rl.list_id,
        rl.list_code,
        rl.list_name,
        rl.region as regulation_region,
        case
            when s.threshold_ppm = 0 and ps.concentration_ppm > 0 then true
            when s.threshold_ppm > 0 and ps.concentration_ppm > s.threshold_ppm then true
            else false
        end as exceeds_threshold
    from product_subs ps
    inner join products p on ps.product_id = p.product_id
    inner join substances s on ps.substance_id = s.substance_id
    inner join reg_lists rl on s.list_id = rl.list_id
)

select * from checks
