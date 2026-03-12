-- Monthly aggregated survey volumes by region, business unit, and complexity

with surveys as (
    select * from {{ ref('stg_survey_requests') }}
),

monthly_volume as (
    select
        region,
        business_unit,
        complexity,
        survey_type,
        date_trunc('month', requested_date) as request_month,
        count(*) as survey_count,
        sum(product_count) as total_products_queried,
        sum(case when status = 'completed' then 1 else 0 end) as completed_count,
        sum(case when status = 'in_progress' then 1 else 0 end) as in_progress_count,
        sum(case when status = 'pending' then 1 else 0 end) as pending_count,
        sum(case when status = 'cancelled' then 1 else 0 end) as cancelled_count,
        round(avg(product_count), 1) as avg_products_per_survey
    from surveys
    group by
        region, business_unit, complexity, survey_type,
        date_trunc('month', requested_date)
)

select * from monthly_volume
