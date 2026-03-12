-- Coverage analysis of regulatory lists across the product portfolio

with coverage as (
    select * from {{ ref('int_substance_coverage_by_product') }}
),

data_sources as (
    select * from {{ ref('stg_regulatory_data_sources') }}
),

list_coverage as (
    select
        c.list_id,
        c.list_code,
        c.list_name,
        c.regulation_region,
        count(distinct c.product_id) as products_screened,
        round(avg(c.screening_coverage_pct), 2) as avg_screening_coverage_pct,
        sum(c.substances_screened) as total_substance_screenings,
        count(distinct case when c.screening_coverage_pct > 0 then c.product_id end) as products_with_any_coverage,
        count(distinct case when c.screening_coverage_pct >= 80 then c.product_id end) as products_with_good_coverage,
        (
            select count(*)
            from data_sources ds
            where ds.region = c.regulation_region or ds.region = 'GLOBAL'
        ) as available_data_sources
    from coverage c
    group by c.list_id, c.list_code, c.list_name, c.regulation_region
)

select * from list_coverage
