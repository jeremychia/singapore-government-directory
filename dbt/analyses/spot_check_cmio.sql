-- Sample Unknown names for pattern identification (excluding data quality issues)
with unknown_sample as (
  select full_name, predicted_ethnicity, row_number() over (order by full_name) as rn
  from {{ ref('dim_person') }}
  where predicted_ethnicity = 'Unknown' 
    and full_name not in ('(Vacant)', '-', '--')
    and length(full_name) > 3
)
select full_name
from unknown_sample
where rn <= 300
order by rn