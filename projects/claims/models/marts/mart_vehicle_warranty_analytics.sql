with claim_details as (

    select * from {{ ref('int_claim_details') }}

),

vehicle_summary as (

    select
        vin,
        min(vehicle_model) as vehicle_model,
        min(vehicle_class) as vehicle_class,
        min(model_year) as model_year,
        min(engine_type) as engine_type,
        min(transmission) as transmission,
        min(warranty_type) as warranty_type,
        min(lifecycle_stage) as lifecycle_stage,
        min(production_plant) as production_plant,
        min(warranty_start_date) as warranty_start_date,
        min(warranty_end_date) as warranty_end_date,
        min(dealer_id) as primary_dealer_id,

        -- claim volume metrics
        count(*) as total_claims,
        sum(total_claim_amount) as total_claim_amount,
        round(avg(total_claim_amount), 2) as avg_claim_amount,
        sum(total_parts_cost) as total_parts_cost,
        sum(total_labor_cost) as total_labor_cost,

        -- claim type diversity
        count(distinct claim_type) as distinct_claim_types,
        string_agg(distinct claim_type, ', ' order by claim_type) as claim_types_used,

        -- temporal metrics
        min(filed_date) as first_claim_date,
        max(filed_date) as latest_claim_date,
        max(filed_date) - min(filed_date) as days_between_first_last_claim,

        -- risk and anomaly
        round(avg(risk_score), 1) as avg_risk_score,
        sum(case when is_anomaly then 1 else 0 end) as anomaly_count,
        max(prior_claims_on_vin) as max_prior_claims,

        -- warranty context
        sum(case when warranty_active_at_filing then 1 else 0 end) as claims_within_warranty,
        sum(case when not warranty_active_at_filing then 1 else 0 end) as claims_outside_warranty,

        -- mileage at repairs
        min(mileage_at_repair) as min_mileage_at_repair,
        max(mileage_at_repair) as max_mileage_at_repair,
        round(avg(mileage_at_repair), 0) as avg_mileage_at_repair

    from claim_details
    group by vin

)

select * from vehicle_summary
