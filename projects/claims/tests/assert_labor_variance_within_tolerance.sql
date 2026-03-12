-- Verify that claim line labor variance does not exceed 200% in either direction.
-- Extreme variances (> 200%) suggest data quality issues.

select claim_line_id
from {{ ref('stg_claim_lines') }}
where abs(labor_variance_pct) > 200
