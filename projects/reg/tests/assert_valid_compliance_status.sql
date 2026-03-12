-- Products marked as CRITICAL should have at least one banned substance violation
select product_id
from {{ ref('mart_product_compliance_status') }}
where overall_compliance_status = 'CRITICAL'
  and violations_found = 0
