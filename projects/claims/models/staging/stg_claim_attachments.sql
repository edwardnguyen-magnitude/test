select
    attachment_id,
    claim_id,
    trim(file_name) as file_name,
    trim(file_type) as file_type,
    cast(file_size_kb as integer) as file_size_kb,
    cast(upload_date as date) as upload_date,
    trim(perceptual_hash) as perceptual_hash,
    cast(ssim_match_score as numeric) as ssim_match_score,
    case when is_duplicate_flag = 'Y' then true else false end as is_duplicate_flag

from {{ ref('fact_claim_attachments') }}
