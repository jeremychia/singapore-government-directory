with
    source as (select * from {{ source("scd", "departments") }}),
    renamed as (
        select
            {{ adapter.quote("parent_name") }},
            {{ adapter.quote("department_name") }},
            {{ adapter.quote("department_link") }} as department_url,
            {{ adapter.quote("ministry_name") }},
            {{ adapter.quote("_valid_from") }},
            {{ adapter.quote("_valid_to") }}
        from source
    )
select *
from renamed
