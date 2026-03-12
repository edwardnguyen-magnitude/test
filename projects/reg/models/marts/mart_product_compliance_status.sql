-- Executive-level product compliance summary
-- One row per product showing overall compliance posture

with checks as (
    select * from {{ ref('int_product_compliance_checks') }}
),

product_summary as (
    select
        product_id,
        product_description,
        business_unit,
        product_category,
        count(distinct list_code) as lists_screened,
        count(distinct substance_id) as substances_checked,
        sum(case when exceeds_threshold then 1 else 0 end) as violations_found,
        count(distinct case when exceeds_threshold then list_code end) as lists_with_violations,
        min(last_verified_date) as oldest_verification,
        max(last_verified_date) as latest_verification,
        case
            when sum(case when exceeds_threshold then 1 else 0 end) = 0 then 'COMPLIANT'
            when sum(case when exceeds_threshold and restriction_type = 'banned' then 1 else 0 end) > 0 then 'CRITICAL'
            when sum(case when exceeds_threshold then 1 else 0 end) > 0 then 'NON_COMPLIANT'
            else 'UNKNOWN'
        end as overall_compliance_status,
        string_agg(
            distinct case when exceeds_threshold then list_code end,
            ', '
        ) as violation_list_codes
    from checks
    group by product_id, product_description, business_unit, product_category
)

select * from product_summary
