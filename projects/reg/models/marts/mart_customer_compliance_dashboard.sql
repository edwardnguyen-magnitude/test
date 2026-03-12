-- Customer-facing compliance dashboard data

with turnaround as (
    select * from {{ ref('int_survey_turnaround') }}
),

customer_dash as (
    select
        customer_id,
        customer_name,
        customer_region,
        industry,
        count(*) as total_inquiries,
        count(distinct survey_type) as inquiry_types_used,
        sum(product_count) as total_products_assessed,
        sum(case when status = 'completed' then 1 else 0 end) as completed_inquiries,
        sum(case when timeliness_status = 'on_time' then 1 else 0 end) as on_time_deliveries,
        round(avg(actual_days), 1) as avg_response_days,
        sum(flagged_count) as total_substances_flagged,
        sum(non_compliant_count) as total_non_compliant_findings,
        min(requested_date) as first_inquiry_date,
        max(requested_date) as latest_inquiry_date,
        max(completed_date) as last_completed_date
    from turnaround
    group by customer_id, customer_name, customer_region, industry
)

select * from customer_dash
