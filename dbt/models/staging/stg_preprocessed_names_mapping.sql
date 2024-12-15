with
    source as (select * from {{ source("preprocessed", "names_mapping") }}),
    renamed as (
        select {{ adapter.quote("extracted_name") }}, {{ adapter.quote("name") }}

        from source
    )
select *
from renamed
