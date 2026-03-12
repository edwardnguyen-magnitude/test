-- Verify that total_claim_amount approximately equals total_parts_cost + total_labor_cost
-- Allows a $1.00 tolerance for rounding differences

select claim_id
from {{ ref('stg_claims') }}
where abs(total_claim_amount - (total_parts_cost + total_labor_cost)) > 1.0
