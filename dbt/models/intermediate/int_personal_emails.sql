-- int_personal_emails: derive the latest personal (non-shared) email for each canonical name
-- grain: one row per canonical_name that has a personal email
{{
    config(
        materialized='table'
    )
}}

with
    names_mapping as (
        select distinct name as canonical_name
        from {{ ref("stg_preprocessed_names_mapping") }}
        where name is not null
    ),

    -- get all emails ever associated with each canonical name
    all_emails as (
        select
            nm.name as canonical_name,
            lower(rn.email) as email,
            rn._accessed_at
        from {{ ref("stg_preprocessed_names_mapping") }} nm
        inner join {{ ref("stg_raw_names") }} rn
            on rn.name = nm.extracted_name
        where rn.email is not null 
            and rn.email not in ('-', '.', 'na', '')
    ),

    -- identify shared/generic emails (used by multiple canonical names)
    email_usage as (
        select
            email,
            count(distinct canonical_name) as person_count
        from all_emails
        group by email
    ),

    shared_emails as (
        select email
        from email_usage
        where person_count > 1
    ),

    -- get the latest personal (non-shared) email for each person
    personal_emails as (
        select
            ae.canonical_name,
            ae.email,
            ae._accessed_at
        from all_emails ae
        left join shared_emails se on ae.email = se.email
        where se.email is null  -- exclude shared emails
        qualify row_number() over (
            partition by ae.canonical_name
            order by ae._accessed_at desc
        ) = 1
    )

select
    canonical_name,
    email,
    true as has_personal_email
from personal_emails
