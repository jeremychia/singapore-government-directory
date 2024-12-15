{% set threshold_days_between_sessions = 0.7 %}

with
    stg_metadata as (
        select _accessed_at, ministry_name, table_name from {{ ref("stg_metadata") }}
    ),

    /*
        General logic is to:

        1. Populate most of the sessions by relying on the gaps between extraction sessions.
        This is done using window functions. See logic `get_gaps_with_more_than_0_9_day_lag` and `populate_sessions_from_gaps`.
        (Typically the run is done once every week, with some irregularities)

        2. Because of window functions, the first and last session will not be in these CTEs. This is done seperately.
    */
    get_gaps_with_more_than_0_9_day_lag as (
        select
            _accessed_at,
            lead(_accessed_at, 1) over (order by _accessed_at) as next_accessed_at
        from stg_metadata
        -- assumes that there is a 0.9 day gap between extraction cycle
        qualify
            round(
                (
                    date_diff(
                        lead(_accessed_at, 1) over (order by _accessed_at),
                        _accessed_at,
                        second
                    )
                    / 86400
                ),
                2
            )
            > {{ threshold_days_between_sessions }}
    ),

    /*
        FIRST SESSION
    */

    -- this is the start of the first extraction cycle
    get_earliest_time as (
        select min(_accessed_at) as first_completion_of_session from stg_metadata
    ),

    -- this is the end of the first extraction cycle
    get_earliest_accessed_at_time_from_gaps as (
        select min(_accessed_at) as last_completion_of_session
        from get_gaps_with_more_than_0_9_day_lag
    ),

    earliest_session as (
        select first_completion_of_session, last_completion_of_session
        from get_earliest_time
        cross join get_earliest_accessed_at_time_from_gaps
    ),

    /*
        LATEST SESSION
    */

    -- this is the start of the latest extraction cycle
    get_latest_next_accessed_at_time_from_gaps as (
        select max(next_accessed_at) as first_completion_of_session
        from get_gaps_with_more_than_0_9_day_lag
    ),

    -- this is the end of the latest extraction cycle
    get_latest_time as (
        select max(_accessed_at) as last_completion_of_session from stg_metadata
    ),

    latest_session as (
        select
            get_latest_next_accessed_at_time_from_gaps.first_completion_of_session,
            get_latest_time.last_completion_of_session
        from get_latest_next_accessed_at_time_from_gaps
        cross join get_latest_time
    ),

    /*
        EVERYTHING IN BETWEEN
    */

    populate_sessions_from_gaps as (
        select
            next_accessed_at as first_completion_of_session,
            lead(_accessed_at, 1) over (
                order by _accessed_at
            ) as last_completion_of_session
        from get_gaps_with_more_than_0_9_day_lag
        qualify lead(_accessed_at, 1) over (order by _accessed_at) is not null
    ),

    /*
        COMBINE EVERYTHING
    */

    union_earliest_middle_latest_sessions as (

        select first_completion_of_session, last_completion_of_session
        from earliest_session
        union all
        select first_completion_of_session, last_completion_of_session
        from populate_sessions_from_gaps
        union all
        select first_completion_of_session, last_completion_of_session
        from latest_session
    ),

    post_processing as (
        select
            row_number() over (order by first_completion_of_session) as session_id,
            first_completion_of_session,
            last_completion_of_session,
            round(
                (
                    date_diff(
                        last_completion_of_session, first_completion_of_session, second
                    )
                    / 3600
                ),
                5
            ) as session_duration_hours
        from union_earliest_middle_latest_sessions

    )

select *
from post_processing
order by 1
