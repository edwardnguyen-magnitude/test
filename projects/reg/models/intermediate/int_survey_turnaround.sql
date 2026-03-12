-- Survey-level enrichment with turnaround time, timeliness, and response aggregations

with surveys as (
    select * from {{ ref('stg_survey_requests') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

responses as (
    select
        survey_id,
        count(*) as total_responses,
        sum(case when compliance_status = 'compliant' then 1 else 0 end) as compliant_count,
        sum(case when compliance_status = 'non_compliant' then 1 else 0 end) as non_compliant_count,
        sum(case when compliance_status = 'flagged' then 1 else 0 end) as flagged_count,
        sum(case when compliance_status = 'under_review' then 1 else 0 end) as under_review_count
    from {{ ref('stg_survey_responses') }}
    group by survey_id
),

enriched as (
    select
        s.survey_id,
        s.customer_id,
        c.customer_name,
        c.region as customer_region,
        c.industry,
        s.region,
        s.survey_type,
        s.complexity,
        s.product_count,
        s.status,
        s.requested_date,
        s.due_date,
        s.completed_date,
        s.business_unit,
        s.completed_date - s.requested_date as actual_days,
        s.due_date - s.requested_date as allowed_days,
        case
            when s.status = 'completed' and s.completed_date <= s.due_date then 'on_time'
            when s.status = 'completed' and s.completed_date > s.due_date then 'late'
            when s.status in ('in_progress', 'pending') and current_date > s.due_date then 'overdue'
            else 'in_progress'
        end as timeliness_status,
        coalesce(r.total_responses, 0) as total_responses,
        coalesce(r.compliant_count, 0) as compliant_count,
        coalesce(r.non_compliant_count, 0) as non_compliant_count,
        coalesce(r.flagged_count, 0) as flagged_count,
        coalesce(r.under_review_count, 0) as under_review_count
    from surveys s
    left join customers c on s.customer_id = c.customer_id
    left join responses r on s.survey_id = r.survey_id
)

select * from enriched
