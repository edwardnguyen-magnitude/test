-- Risk assessment matrix for restricted substances across the product portfolio

with checks as (
    select * from {{ ref('int_product_compliance_checks') }}
),

risk_assessment as (
    select
        substance_id,
        substance_name,
        cas_number,
        hazard_class,
        restriction_type,
        list_code,
        list_name,
        regulation_region,
        count(distinct product_id) as products_containing,
        sum(case when exceeds_threshold then 1 else 0 end) as products_exceeding_threshold,
        max(actual_concentration_ppm) as max_concentration_ppm,
        round(avg(actual_concentration_ppm), 2) as avg_concentration_ppm,
        case
            when restriction_type = 'banned'
                and sum(case when exceeds_threshold then 1 else 0 end) > 0
                then 'CRITICAL'
            when restriction_type = 'restricted'
                and sum(case when exceeds_threshold then 1 else 0 end) > 0
                then 'HIGH'
            when restriction_type in ('declarable', 'candidate')
                and count(distinct product_id) > 5
                then 'MEDIUM'
            else 'LOW'
        end as risk_level
    from checks
    group by
        substance_id, substance_name, cas_number,
        hazard_class, restriction_type,
        list_code, list_name, regulation_region
)

select * from risk_assessment
