-- Verify referential integrity: every claim line must reference an existing claim

select cl.claim_line_id
from {{ ref('stg_claim_lines') }} cl
left join {{ ref('stg_claims') }} c on cl.claim_id = c.claim_id
where c.claim_id is null
