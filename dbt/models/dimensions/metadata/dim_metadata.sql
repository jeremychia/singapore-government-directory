with
    stg_metadata as (
        select table_name, ministry_name, num_rows, _accessed_at,
        from {{ ref("stg_metadata") }}
    ),

    extraction_sessions as (
        select session_id, first_completion_of_session, last_completion_of_session,
        from {{ ref("dim_extraction_sessions") }}
    ),

    enrich_with_session_order as (
        select stg_metadata.*, extraction_sessions.session_id
        from stg_metadata
        left join
            extraction_sessions
            on stg_metadata._accessed_at
            between extraction_sessions.first_completion_of_session
            and extraction_sessions.last_completion_of_session
    ),

    add_unique_id as (
        select
            md5(
                concat(
                    coalesce(cast(session_id as string), ""),
                    coalesce(ministry_name, ""),
                    coalesce(table_name, "")
                )
            ) as metadata_id,
            enrich_with_session_order.*
        from enrich_with_session_order
    )

select *
from add_unique_id
order by _accessed_at
