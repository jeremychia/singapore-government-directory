-- Sample Unknown names for pattern identification
with unknown_sample as (
  select full_name, predicted_ethnicity, row_number() over (order by full_name) as rn
  from {{ ref('dim_person') }}
  where predicted_ethnicity = 'Unknown'
)
select full_name
from unknown_sample
where rn <= 200
order by rn