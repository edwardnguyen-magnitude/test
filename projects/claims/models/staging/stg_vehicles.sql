select
    vin,
    dealer_id,
    fleet_id,
    trim(model_name) as model_name,
    trim(vehicle_class) as vehicle_class,
    cast(model_year as integer) as model_year,
    trim(engine_type) as engine_type,
    trim(transmission) as transmission,
    trim(warranty_type) as warranty_type,
    cast(warranty_start_date as date) as warranty_start_date,
    cast(warranty_end_date as date) as warranty_end_date,
    cast(current_mileage as integer) as current_mileage,
    trim(lifecycle_stage) as lifecycle_stage,
    trim(production_plant) as production_plant,
    cast(build_date as date) as build_date,
    trim(production_batch) as production_batch

from {{ ref('dim_vehicles') }}
