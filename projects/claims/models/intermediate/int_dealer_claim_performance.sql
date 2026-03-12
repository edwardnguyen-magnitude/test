with claims as (

    select * from {{ ref('stg_claims') }}

),

baselines as (

    select * from {{ ref('stg_dealer_baselines') }}

),

-- aggregate actual claim data per dealer per year
dealer_actuals as (

    select
        dealer_id,
        claim_year,
        count(*) as total_claims,
        sum(total_claim_amount) as total_claim_amount,
        round(avg(total_claim_amount), 2) as avg_claim_amount,
        round(avg(total_labor_hours), 2) as avg_labor_hours,
        round(avg(total_parts_cost), 2) as avg_parts_cost,
        sum(case when status = 'Approved' then 1 else 0 end) as approved_count,
        sum(case when status = 'Rejected' then 1 else 0 end) as rejected_count,
        sum(case when status = 'Escalated' then 1 else 0 end) as escalated_count,
        sum(case when status = 'Under Review' then 1 else 0 end) as under_review_count,
        sum(case when status = 'Submitted' then 1 else 0 end) as submitted_count,
        round(
            sum(case when status = 'Approved' then 1 else 0 end) * 100.0
            / nullif(
                sum(case when status in ('Approved', 'Rejected') then 1 else 0 end),
                0
            ),
            1
        ) as approval_rate_pct,
        round(
            sum(case when status = 'Rejected' then 1 else 0 end) * 100.0
            / nullif(
                sum(case when status in ('Approved', 'Rejected') then 1 else 0 end),
                0
            ),
            1
        ) as rejection_rate_pct,
        round(avg(risk_score), 1) as avg_risk_score,
        sum(case when is_anomaly then 1 else 0 end) as anomaly_count,
        count(distinct vin) as distinct_vehicles

    from claims
    group by dealer_id, claim_year

),

-- aggregate baseline data per dealer per year (across all categories)
baseline_summary as (

    select
        dealer_id,
        year,
        max(tier) as tier,
        round(avg(labor_vs_peer_pct), 1) as avg_labor_vs_peer_pct,
        round(avg(cost_vs_peer_pct), 1) as avg_cost_vs_peer_pct,
        round(avg(rejection_rate_pct), 1) as baseline_rejection_rate_pct
    from baselines
    group by dealer_id, year

),

combined as (

    select
        da.dealer_id,
        da.claim_year,
        da.total_claims,
        da.total_claim_amount,
        da.avg_claim_amount,
        da.avg_labor_hours,
        da.avg_parts_cost,
        da.approved_count,
        da.rejected_count,
        da.escalated_count,
        da.under_review_count,
        da.submitted_count,
        da.approval_rate_pct,
        da.rejection_rate_pct,
        da.avg_risk_score,
        da.anomaly_count,
        da.distinct_vehicles,

        -- baseline enrichment
        bs.tier,
        bs.avg_labor_vs_peer_pct,
        bs.avg_cost_vs_peer_pct,
        bs.baseline_rejection_rate_pct

    from dealer_actuals da
    left join baseline_summary bs
        on da.dealer_id = bs.dealer_id
        and da.claim_year = bs.year

)

select * from combined
