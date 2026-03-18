select
    claim_id,
    trim(cluster_id) as cluster_id,
    cast(cluster_size as integer) as cluster_size,
    cast(distance_from_centroid as numeric) as distance_from_centroid,
    case when is_outlier = 'Y' then true else false end as is_outlier,
    cast(cluster_avg_claim_amount as numeric) as cluster_avg_claim_amount,
    cast(cluster_std_claim_amount as numeric) as cluster_std_claim_amount,
    cast(claim_amount_z_score as numeric) as claim_amount_z_score

from {{ source('raw', 'fact_cluster_assignments') }}
