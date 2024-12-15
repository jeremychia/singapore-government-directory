with
    source as (select * from {{ source("raw", "metadata") }}),
    renamed as (
        select
            {{ adapter.quote("table_name") }},
            {{ adapter.quote("ministry_name") }},
            {{ adapter.quote("num_rows") }},
            {{ adapter.quote("_accessed_at") }}

        from source
    )
select *
from renamed
