-- analysis to identify potential duplicate persons
-- these are different canonical names that share the same personal email
-- (excludes shared/generic emails used by multiple people)

with email_names as (
    select
        lower(rn.email) as email,
        coalesce(nm.name, rn.name) as canonical_name
    from {{ source("raw", "names") }} rn
    left join {{ source("preprocessed", "names_mapping") }} nm
        on rn.name = nm.extracted_name
    where rn.email is not null 
        and rn.email not in ('-', '.', 'na', '')
        and rn.email not like '%@%.gov.sg'  -- exclude generic gov emails for this analysis
    group by 1, 2
),

-- find emails used by exactly 2 canonical names (likely same person, name changed)
potential_duplicates as (
    select 
        email, 
        count(distinct canonical_name) as name_count, 
        array_agg(distinct canonical_name ignore nulls) as names
    from email_names
    group by email
    having count(distinct canonical_name) = 2
)

select *
from potential_duplicates
order by email
