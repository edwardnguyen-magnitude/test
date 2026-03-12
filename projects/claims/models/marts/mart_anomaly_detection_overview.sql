with claim_details as (

    select * from {{ ref('int_claim_details') }}

),

lifecycle as (

    select * from {{ ref('int_claim_lifecycle') }}

),

-- focus on claims that are anomalous or have elevated risk
anomaly_claims as (

    select
        cd.claim_id,
        cd.dealer_id,
        cd.vin,
        cd.fleet_id,
        cd.technician_id,
        cd.filed_date,
        cd.claim_year,
        cd.claim_quarter,
        cd.vehicle_model,
        cd.claim_type,
        cd.total_claim_amount,
        cd.total_parts_cost,
        cd.total_labor_hours,
        cd.num_line_items,
        cd.status,
        cd.submission_channel,

        -- risk & anomaly fields
        cd.risk_score,
        cd.is_anomaly,
        cd.anomaly_tier,
        cd.anomaly_flags,
        cd.days_to_warranty_end,
        cd.prior_claims_on_vin,
        cd.pre_authorized,

        -- cluster context
        cd.cluster_id,
        cd.cluster_outlier,
        cd.claim_amount_z_score,
        cd.distance_from_centroid,
        cd.cluster_avg_claim_amount,

        -- vehicle context
        cd.vehicle_class,
        cd.warranty_type,
        cd.warranty_active_at_filing,
        cd.mileage_bucket,

        -- lifecycle context
        lc.final_status,
        lc.was_auto_routed,
        round(lc.hours_to_resolve, 1) as hours_to_resolve,
        lc.total_actions,

        -- calculated: anomaly severity based on risk score
        case
            when cd.risk_score > 70 then 'CRITICAL'
            when cd.risk_score > 50 then 'HIGH'
            when cd.risk_score > 30 then 'MEDIUM'
            else 'LOW'
        end as anomaly_severity,

        -- calculated: how far this claim is from cluster average
        round(cd.total_claim_amount - cd.cluster_avg_claim_amount, 2)
            as amount_vs_cluster_avg

    from claim_details cd
    left join lifecycle lc on cd.claim_id = lc.claim_id
    where cd.is_anomaly = true
       or cd.risk_score > 50
       or cd.cluster_outlier = true
       or abs(cd.claim_amount_z_score) > 2

)

select * from anomaly_claims
