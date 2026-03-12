with claim_details as (

    select * from {{ ref('int_claim_details') }}

),

lifecycle as (

    select * from {{ ref('int_claim_lifecycle') }}

),

claim_summary as (

    select
        cd.claim_id,
        cd.dealer_id,
        cd.vin,
        cd.fleet_id,
        cd.technician_id,
        cd.filed_date,
        cd.repair_date,
        cd.claim_year,
        cd.claim_quarter,
        cd.mileage_at_repair,
        cd.mileage_bucket,
        cd.vehicle_model,
        cd.model_year,
        cd.claim_type,
        cd.symptom_reported,
        cd.total_parts_cost,
        cd.total_labor_hours,
        cd.total_labor_cost,
        cd.total_claim_amount,
        cd.num_line_items,
        cd.status,
        cd.submission_channel,
        cd.campaign_id,
        cd.pre_authorized,

        -- risk & anomaly
        cd.risk_score,
        cd.is_anomaly,
        cd.anomaly_tier,
        cd.anomaly_flags,
        cd.days_to_warranty_end,
        cd.prior_claims_on_vin,

        -- vehicle context
        cd.vehicle_class,
        cd.engine_type,
        cd.transmission,
        cd.warranty_type,
        cd.lifecycle_stage,
        cd.production_plant,
        cd.warranty_active_at_filing,

        -- cluster context
        cd.cluster_id,
        cd.cluster_outlier,
        cd.claim_amount_z_score,

        -- lifecycle metrics
        lc.final_status,
        lc.submitted_at,
        lc.resolved_at,
        lc.total_actions,
        round(lc.hours_to_route, 1) as hours_to_route,
        round(lc.hours_to_resolve, 1) as hours_to_resolve,
        lc.was_auto_routed,
        lc.routed_by

    from claim_details cd
    left join lifecycle lc on cd.claim_id = lc.claim_id

)

select * from claim_summary
