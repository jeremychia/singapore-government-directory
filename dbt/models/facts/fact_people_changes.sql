with
    -- standardise names
    records_names as (
        select
            name as original_name,
            email,
            position,
            department_url,
            ministry_name,
            -- first run had some values in 2024-05-06, should be grouped as part of
            -- 2024-05-07
            if(
                cast(_accessed_at as date) = '2024-05-06',
                '2024-05-07',
                cast(_accessed_at as date)
            ) as _accessed_at
        from {{ ref("stg_raw_names") }}
        where
            not (
                name in ('', '-', '--')
                or name is null
                or email in ('-')
                or email is null
            )
    ),

    names_mapping as (
        select extracted_name as name, name as original_name
        from {{ ref("stg_preprocessed_names_mapping") }}
    ),

    standardise_names as (
        select
            -- fall back to original_name if mapping doesn't exist or is empty
            coalesce(nullif(names_mapping.name, ''), records_names.original_name) as name,
            records_names.email,
            records_names.position,
            records_names.department_url,
            records_names.ministry_name,
            records_names._accessed_at
        from records_names
        left join
            names_mapping on records_names.original_name = names_mapping.original_name
    ),

    -- exclude generic/role-based emails that get reassigned frequently
    -- these don't represent actual person-level changes
    generic_email_patterns as (
        select lower(email) as email
        from standardise_names
        where
            -- URLs instead of emails
            lower(email) like 'http%'
            -- enquiry/feedback emails
            or lower(email) like '%enquir%'
            or lower(email) like '%feedback%'
            -- role-based emails (head, director, dean, etc.)
            or lower(email) like '%head@%'
            or lower(email) like 'director@%'
            or lower(email) like '%dean@%'
            or lower(email) like '%provost%'
            -- contact/QSM emails
            or lower(email) like 'contact@%'
            or lower(email) like '%_qsm@%'
            or lower(email) like '%qsm@%'
            -- generic role emails
            or lower(email) like 'chairman@%'
            or lower(email) like 'ceo@%'
            or lower(email) like 'president@%'
            or lower(email) like 'vpr@%'
            or lower(email) like 'execdir%'
            or lower(email) like 'ad-%'
            or lower(email) like 'd-%'
            or lower(email) like 'csoci@%'
            -- inspection/workright team emails (shared)
            or lower(email) like 'workright%'
            -- manually identified shared/role emails with frequent reassignments
            or lower(email) like 'chong_bao_yue@moe%'  -- shared between TAY Qin Xuan and CHONG Bao Yue
            or lower(email) like 'wei.kiat.ang@nhg%'   -- shared between FOO Chui Ngoh and ANG Wei Kiat James
        group by 1
    ),

    -- this table, for now, only applies to people with one position
    get_positions_across_dates as (
        select
            _accessed_at,
            lower(email) as email,
            count(
                distinct concat(position, department_url, ministry_name)
            ) as count_positions
        from standardise_names
        where lower(email) not in (select email from generic_email_patterns)
        group by all
    ),

    people_only_one_position as (
        select email
        from get_positions_across_dates
        group by all
        having max(count_positions) = 1
    ),

    -- limit to those with only one position now
    filter_standardise_names as (
        select *
        from standardise_names
        where lower(email) in (select (email) from people_only_one_position)
    ),

    -- analyses
    add_lag_values as (
        -- looking for new joiners and position changes
        -- partition by email only (not name) to handle name changes
        select
            _accessed_at,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag(_accessed_at) over (
                partition by lower(email) order by _accessed_at
            ) as lag_accessed_at,
            lag(name) over (
                partition by lower(email) order by _accessed_at
            ) as lag_name,
            lag(position) over (
                partition by lower(email) order by _accessed_at
            ) as lag_position,
            lag(department_url) over (
                partition by lower(email) order by _accessed_at
            ) as lag_department_url,
            lag(ministry_name) over (
                partition by lower(email) order by _accessed_at
            ) as lag_ministry_name,
        from filter_standardise_names
    ),

    lag_filter_out_similar as (
        select *
        from add_lag_values
        -- filter for records where something changed OR it's a new person
        where
            _accessed_at > '2024-05-07'
            and (
                -- new person (all lag values are null)
                lag_name is null
                -- or something changed
                or name != lag_name
                or position != lag_position
                or department_url != lag_department_url
                or ministry_name != lag_ministry_name
            )
    ),

    new_joiners as (
        select
            'new joiner' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            cast(null as string) as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            lag_name is null
            and lag_position is null
            and lag_department_url is null
            and lag_ministry_name is null
    ),

    name_changes_raw as (
        -- all records where name changed
        select
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_name,
            lag_accessed_at as compared_against_date,
            -- check if any word from old name appears in new name (exact or fuzzy match)
            -- if yes, it's likely a name format change; if no, it's likely a different person
            (
                select count(*) > 0
                from unnest(split(upper(lag_name), ' ')) as old_word
                where 
                    -- exact match for any word (including short surnames like OH, NG, OO)
                    (length(old_word) >= 2 and upper(name) like concat('%', old_word, '%'))
                    -- fuzzy match: first 4+ chars match (catches typos like Maxmilian/Maximilian)
                    or (length(old_word) >= 5 and upper(name) like concat('%', left(old_word, 4), '%'))
            ) as has_common_word
        from lag_filter_out_similar
        where
            lag_name is not null
            and name != lag_name
    ),

    name_change as (
        -- genuine name changes (same person, different formatting)
        select
            'name change' as activity,
            activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_name as old_information,
            compared_against_date
        from name_changes_raw
        where has_common_word = true
    ),

    email_reassignment as (
        -- different person taking over an email (likely role-based email)
        select
            'email reassignment' as activity,
            activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_name as old_information,
            compared_against_date
        from name_changes_raw
        where has_common_word = false
    ),

    ministry_change as (
        select
            'change ministry' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            concat(
                "(",
                lag_ministry_name,
                ", ",
                lag_position,
                ", ",
                lag_department_url,
                ")"
            ) as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where ministry_name != lag_ministry_name
    ),

    department_change_different_role as (
        select
            'change department (different role)' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            concat("(", lag_position, ", ", lag_department_url, ")") as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            ministry_name = lag_ministry_name
            and department_url != lag_department_url
            and position != lag_position
    ),

    department_change_same_role as (
        select
            'change department (same role)' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_department_url as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            ministry_name = lag_ministry_name
            and department_url != lag_department_url
            and position = lag_position
    ),

    same_department_different_role as (
        select
            'change role (same department)' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag_position as old_information,
            lag_accessed_at as compared_against_date
        from lag_filter_out_similar
        where
            ministry_name = lag_ministry_name
            and department_url = lag_department_url
            and position != lag_position
    ),

    max_date as (select max(_accessed_at) as latest_run from records_names),

    add_lead_values as (
        -- looking for resignations and rehires
        -- partition by email only (not name) to handle name changes
        select
            _accessed_at,
            name,
            email,
            position,
            department_url,
            ministry_name,
            lag(_accessed_at) over (
                partition by lower(email) order by _accessed_at
            ) as lag_accessed_at,
            lead(_accessed_at) over (
                partition by lower(email) order by _accessed_at
            ) as lead_accessed_at,
            lead(position) over (
                partition by lower(email) order by _accessed_at
            ) as lead_position,
            lead(department_url) over (
                partition by lower(email) order by _accessed_at
            ) as lead_department_url,
            lead(ministry_name) over (
                partition by lower(email) order by _accessed_at
            ) as lead_ministry_name
        from filter_standardise_names
    ),

    lead_filter_out_similar as (
        select *
        from add_lead_values
        -- person disappeared - all lead values are null
        where
            _accessed_at < (select latest_run from max_date)
            and lead_position is null
            and lead_department_url is null
            and lead_ministry_name is null
    ),

    -- Note: rehire detection is complex because different ministries are scraped
    -- at different times. A person missing from one scrape doesn't mean they left.
    -- True rehire detection would require tracking across multiple consecutive
    -- scrapes where the person's ministry WAS scraped but they were absent.

    resignees as (
        select
            'resigned' as activity,
            _accessed_at as activity_date,
            name,
            email,
            position,
            department_url,
            ministry_name,
            cast(null as string) as old_information,
            lead_accessed_at as compared_against_date
        from lead_filter_out_similar
    ),

    -- union activities
    unioned as (
        select *
        from new_joiners
        union all
        select *
        from name_change
        union all
        select *
        from email_reassignment
        union all
        select *
        from ministry_change
        union all
        select *
        from department_change_different_role
        union all
        select *
        from department_change_same_role
        union all
        select *
        from same_department_different_role
        union all
        select *
        from resignees
    )

select *
from
    unioned
    -- order by _accessed_at desc, name
    
