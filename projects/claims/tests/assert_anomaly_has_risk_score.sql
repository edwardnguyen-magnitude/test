-- Verify logical consistency: claims flagged as anomalies must have a risk score > 0

select claim_id
from {{ ref('stg_claims') }}
where is_anomaly = true
  and (risk_score is null or risk_score = 0)
