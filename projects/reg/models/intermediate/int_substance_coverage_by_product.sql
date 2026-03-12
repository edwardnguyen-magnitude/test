-- Coverage analysis: how thoroughly each product is screened against each regulatory list

with products as (
    select * from {{ ref('stg_products') }}
),

product_subs as (
    select distinct product_id, substance_id
    from {{ ref('stg_product_substances') }}
),

substances as (
    select substance_id, list_id
    from {{ ref('stg_restricted_substances') }}
),

reg_lists as (
    select * from {{ ref('stg_regulatory_lists') }}
),

product_list_coverage as (
    select
        p.product_id,
        p.product_description,
        p.business_unit,
        p.product_category,
        rl.list_id,
        rl.list_code,
        rl.list_name,
        rl.region as regulation_region,
        count(distinct ps.substance_id) as substances_screened,
        rl.substance_count as total_substances_on_list,
        round(
            count(distinct ps.substance_id) * 100.0 / nullif(rl.substance_count, 0),
            2
        ) as screening_coverage_pct
    from products p
    cross join reg_lists rl
    left join product_subs ps on p.product_id = ps.product_id
    left join substances s on ps.substance_id = s.substance_id and s.list_id = rl.list_id
    group by
        p.product_id, p.product_description, p.business_unit, p.product_category,
        rl.list_id, rl.list_code, rl.list_name, rl.region, rl.substance_count
)

select * from product_list_coverage
