select
    dealer_id,
    cast(year as integer) as year,
    trim(category) as category,
    cast(claim_count as integer) as claim_count,
    cast(avg_labor_hours as numeric) as avg_labor_hours,
    cast(avg_parts_cost as numeric) as avg_parts_cost,
    cast(peer_avg_labor_hours as numeric) as peer_avg_labor_hours,
    cast(peer_avg_parts_cost as numeric) as peer_avg_parts_cost,
    cast(labor_vs_peer_pct as numeric) as labor_vs_peer_pct,
    cast(cost_vs_peer_pct as numeric) as cost_vs_peer_pct,
    cast(rejection_rate_pct as numeric) as rejection_rate_pct,
    trim(tier) as tier

from {{ source('raw', 'ref_dealer_baselines') }}
