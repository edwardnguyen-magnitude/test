with dealer_perf as (

    select * from {{ ref('int_dealer_claim_performance') }}

),

labor_analysis as (

    select
        cl.claim_id,
        cl.labor_efficiency_status,
        cl.cost_flag
    from {{ ref('int_claim_line_labor_analysis') }} cl

),

claims as (

    select
        claim_id,
        dealer_id,
        claim_year
    from {{ ref('stg_claims') }}

),

-- aggregate labor efficiency per dealer per year
dealer_labor as (

    select
        c.dealer_id,
        c.claim_year,
        count(*) as total_line_items,
        sum(case when la.labor_efficiency_status = 'over_standard' then 1 else 0 end)
            as over_standard_count,
        sum(case when la.labor_efficiency_status = 'under_standard' then 1 else 0 end)
            as under_standard_count,
        sum(case when la.labor_efficiency_status = 'within_tolerance' then 1 else 0 end)
            as within_tolerance_count,
        round(
            sum(case when la.labor_efficiency_status = 'over_standard' then 1 else 0 end)
            * 100.0 / nullif(count(*), 0),
            1
        ) as pct_over_standard_labor,
        sum(case when la.cost_flag = 'high' then 1 else 0 end) as high_cost_lines,
        round(
            sum(case when la.cost_flag = 'high' then 1 else 0 end)
            * 100.0 / nullif(count(*), 0),
            1
        ) as pct_high_cost_lines
    from labor_analysis la
    inner join claims c on la.claim_id = c.claim_id
    group by c.dealer_id, c.claim_year

),

scorecard as (

    select
        dp.dealer_id,
        dp.claim_year,
        dp.total_claims,
        dp.total_claim_amount,
        dp.avg_claim_amount,
        dp.avg_labor_hours,
        dp.avg_parts_cost,
        dp.approved_count,
        dp.rejected_count,
        dp.escalated_count,
        dp.approval_rate_pct,
        dp.rejection_rate_pct,
        dp.avg_risk_score,
        dp.anomaly_count,
        dp.distinct_vehicles,

        -- baseline comparisons
        dp.tier,
        dp.avg_labor_vs_peer_pct,
        dp.avg_cost_vs_peer_pct,

        -- labor efficiency metrics
        dl.total_line_items,
        dl.pct_over_standard_labor,
        dl.pct_high_cost_lines,

        -- calculated: dealer risk tier
        case
            when dp.rejection_rate_pct > 30
                 or dp.avg_cost_vs_peer_pct > 15
                then 'HIGH_RISK'
            when dp.rejection_rate_pct > 15
                 or dp.avg_cost_vs_peer_pct > 10
                then 'MEDIUM_RISK'
            else 'LOW_RISK'
        end as dealer_risk_tier

    from dealer_perf dp
    left join dealer_labor dl
        on dp.dealer_id = dl.dealer_id
        and dp.claim_year = dl.claim_year

)

select * from scorecard
