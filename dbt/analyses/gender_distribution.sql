-- analysis to check gender distribution in dim_person
select 
    predicted_gender,
    count(*) as count,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct
from {{ ref('dim_person') }}
group by predicted_gender
order by count desc
