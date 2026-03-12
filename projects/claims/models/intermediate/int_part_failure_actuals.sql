with claim_lines as (

    select * from {{ ref('stg_claim_lines') }}

),

claims as (

    select
        claim_id,
        vehicle_model,
        model_year
    from {{ ref('stg_claims') }}

),

pfp as (

    select * from {{ ref('stg_pfp') }}

),

-- aggregate claim line data per part + model + model_year
part_actuals as (

    select
        cl.part_number,
        c.vehicle_model as model_name,
        c.model_year,
        count(*) as actual_claim_count,
        sum(cl.line_total) as total_part_cost,
        round(avg(cl.unit_cost), 2) as avg_unit_cost,
        round(avg(cl.labor_hours), 2) as avg_labor_hours,
        count(distinct c.claim_id) as distinct_claims

    from claim_lines cl
    inner join claims c on cl.claim_id = c.claim_id
    group by cl.part_number, c.vehicle_model, c.model_year

),

-- join with PFP reference to compare expected vs actual
combined as (

    select
        pa.part_number,
        pa.model_name,
        pa.model_year,
        pa.actual_claim_count,
        pa.total_part_cost,
        pa.avg_unit_cost,
        pa.avg_labor_hours,
        pa.distinct_claims,

        -- PFP reference data
        pfp.pfp_id,
        pfp.part_description,
        pfp.vehicles_in_population,
        pfp.expected_failure_rate_per_1000,
        pfp.actual_failure_count as pfp_actual_failure_count,
        pfp.actual_failure_rate_per_1000 as pfp_actual_failure_rate,
        pfp.deviation_pct as pfp_deviation_pct,
        pfp.trend,
        pfp.period,

        -- calculated: compute failure rate from claims data
        case
            when pfp.vehicles_in_population > 0
            then round(pa.actual_claim_count * 1000.0 / pfp.vehicles_in_population, 2)
            else null
        end as computed_failure_rate_per_1000,

        -- calculated: deviation from expected
        case
            when pfp.expected_failure_rate_per_1000 > 0
                 and pfp.vehicles_in_population > 0
            then round(
                (pa.actual_claim_count * 1000.0 / pfp.vehicles_in_population
                 - pfp.expected_failure_rate_per_1000)
                * 100.0 / pfp.expected_failure_rate_per_1000,
                1
            )
            else null
        end as computed_deviation_pct

    from part_actuals pa
    left join pfp
        on pa.part_number = pfp.part_number
        and pa.model_name = pfp.model_name
        and pa.model_year = pfp.model_year

)

select * from combined
