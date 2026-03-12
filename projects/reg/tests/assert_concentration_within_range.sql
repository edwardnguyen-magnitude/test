-- Concentrations should be between 0 and 1,000,000 ppm (100%)
select product_substance_id
from {{ ref('stg_product_substances') }}
where concentration_ppm < 0
   or concentration_ppm > 1000000
