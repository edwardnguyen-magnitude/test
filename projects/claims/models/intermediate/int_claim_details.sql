with claims as (

    select * from {{ ref('stg_claims') }}

),

vehicles as (

    select * from {{ ref('stg_vehicles') }}

),

clusters as (

    select * from {{ ref('stg_cluster_assignments') }}

),

enriched as (

    select
        c.claim_id,
        c.dealer_id,
        c.vin,
        c.fleet_id,
        c.technician_id,
        c.filed_date,
        c.repair_date,
        c.claim_year,
        c.claim_month,
        c.claim_quarter,
        c.mileage_at_repair,
        c.vehicle_model,
        c.model_year,
        c.claim_type,
        c.symptom_reported,
        c.total_parts_cost,
        c.total_labor_hours,
        c.total_labor_cost,
        c.total_claim_amount,
        c.num_line_items,
        c.technician_narrative,
        c.status,
        c.risk_score,
        c.is_anomaly,
        c.anomaly_tier,
        c.anomaly_flags,
        c.days_to_warranty_end,
        c.prior_claims_on_vin,
        c.campaign_id,
        c.pre_authorized,
        c.submission_channel,

        -- vehicle enrichment
        v.vehicle_class,
        v.engine_type,
        v.transmission,
        v.warranty_type,
        v.warranty_start_date,
        v.warranty_end_date,
        v.current_mileage as vehicle_current_mileage,
        v.lifecycle_stage,
        v.production_plant,
        v.build_date,

        -- cluster enrichment
        cl.cluster_id,
        cl.cluster_size,
        cl.distance_from_centroid,
        cl.is_outlier as cluster_outlier,
        cl.cluster_avg_claim_amount,
        cl.cluster_std_claim_amount,
        cl.claim_amount_z_score,

        -- calculated: warranty active at time of filing
        case
            when v.warranty_start_date is not null
                 and c.filed_date >= v.warranty_start_date
                 and c.filed_date <= v.warranty_end_date
            then true
            else false
        end as warranty_active_at_filing,

        -- calculated: mileage bucket for segmentation
        case
            when c.mileage_at_repair < 50000 then '0-50k'
            when c.mileage_at_repair < 100000 then '50k-100k'
            when c.mileage_at_repair < 200000 then '100k-200k'
            else '200k+'
        end as mileage_bucket

    from claims c
    left join vehicles v on c.vin = v.vin
    left join clusters cl on c.claim_id = cl.claim_id

)

select * from enriched
