-- Every survey response must reference an existing survey request
select r.response_id
from {{ ref('stg_survey_responses') }} r
left join {{ ref('stg_survey_requests') }} s on r.survey_id = s.survey_id
where s.survey_id is null
