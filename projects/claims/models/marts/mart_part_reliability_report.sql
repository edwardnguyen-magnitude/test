with part_failures as (

    select * from {{ ref('int_part_failure_actuals') }}

),

reliability_report as (

    select
        part_number,
        part_description,
        model_name,
        model_year,
        actual_claim_count,
        distinct_claims,
        total_part_cost,
        avg_unit_cost,
        avg_labor_hours,

        -- PFP reference data
        vehicles_in_population,
        expected_failure_rate_per_1000,
        pfp_actual_failure_count,
        pfp_actual_failure_rate,
        pfp_deviation_pct,
        trend,
        period,

        -- computed metrics
        computed_failure_rate_per_1000,
        computed_deviation_pct,

        -- calculated: reliability rating based on deviation from expected
        case
            when computed_deviation_pct is null then 'UNKNOWN'
            when abs(computed_deviation_pct) > 30 then 'CRITICAL'
            when abs(computed_deviation_pct) > 10 then 'WARNING'
            else 'GOOD'
        end as reliability_rating,

        -- calculated: cost impact severity
        case
            when total_part_cost > 50000 then 'HIGH_IMPACT'
            when total_part_cost > 10000 then 'MEDIUM_IMPACT'
            else 'LOW_IMPACT'
        end as cost_impact_tier

    from part_failures

)

select * from reliability_report
