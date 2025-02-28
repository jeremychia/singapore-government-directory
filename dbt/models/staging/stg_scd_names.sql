with
    source as (select * from {{ source("scd", "names") }}),
    renamed as (
        select
            {{ adapter.quote("name") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("position") }},
            {{ adapter.quote("department") }} as department_name,
            {{ adapter.quote("url") }} as department_url,
            {{ adapter.quote("ministry_name") }},
            {{ adapter.quote("_valid_from") }},
            {{ adapter.quote("_valid_to") }}
        from source
    )
select *
from renamed
