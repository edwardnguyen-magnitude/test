-- Regional performance summary for survey fulfillment

with turnaround as (
    select * from {{ ref('int_survey_turnaround') }}
),

regional_summary as (
    select
        region,
        business_unit,
        complexity,
        count(*) as total_surveys,
        sum(product_count) as total_products_assessed,
        sum(case when status = 'completed' then 1 else 0 end) as completed,
        sum(case when status = 'in_progress' then 1 else 0 end) as in_progress,
        sum(case when timeliness_status = 'on_time' then 1 else 0 end) as on_time,
        sum(case when timeliness_status = 'late' then 1 else 0 end) as late,
        sum(case when timeliness_status = 'overdue' then 1 else 0 end) as overdue,
        round(avg(actual_days), 1) as avg_turnaround_days,
        round(
            sum(case when timeliness_status = 'on_time' then 1 else 0 end) * 100.0
            / nullif(sum(case when status = 'completed' then 1 else 0 end), 0),
            1
        ) as on_time_rate_pct,
        sum(compliant_count) as total_compliant_responses,
        sum(non_compliant_count) as total_non_compliant_responses,
        round(
            sum(compliant_count) * 100.0
            / nullif(sum(compliant_count) + sum(non_compliant_count), 0),
            1
        ) as compliance_rate_pct
    from turnaround
    group by region, business_unit, complexity
)

select * from regional_summary
