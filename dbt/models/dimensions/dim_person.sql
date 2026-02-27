-- dim_person: stable person dimension derived from canonical names
-- grain: one row per unique person (identified by canonical name)
-- 
-- deduplication strategy:
-- 1. canonical_name from names_mapping is the primary identifier
-- 2. email is an attribute (can be null or shared by multiple people)
-- 3. shared/generic emails are identified and flagged
-- 4. for unique personal emails, we can use them to detect if the same person
--    appears under different canonical names (requires manual review)
--
-- this model joins pre-computed intermediate tables for:
-- - personal emails (int_personal_emails)
-- - ethnicity classification (int_ethnicity_classification)
-- - gender classification (int_gender_classification)

with
    names_mapping as (
        select distinct name as canonical_name
        from {{ ref("stg_preprocessed_names_mapping") }}
        where name is not null
    ),

    -- get current prefixes and postfixes
    prefixes as (
        select extracted_name as canonical_name, prefix
        from {{ ref("stg_preprocessed_prefixes_history") }}
        where is_latest = true
    ),

    postfixes as (
        select extracted_name as canonical_name, postfix
        from {{ ref("stg_preprocessed_postfixes_history") }}
        where is_latest = true
    ),

    -- join intermediate tables
    persons as (
        select
            {{ dbt_utils.generate_surrogate_key(["nm.canonical_name"]) }} as person_key,
            nm.canonical_name as full_name,
            pf.prefix,
            po.postfix,
            pe.email,
            coalesce(pe.has_personal_email, false) as has_personal_email,
            gc.predicted_gender,
            gc.gender_source,
            ec.predicted_ethnicity
        from names_mapping nm
        left join {{ ref("int_personal_emails") }} pe on nm.canonical_name = pe.canonical_name
        left join prefixes pf on nm.canonical_name = pf.canonical_name
        left join postfixes po on nm.canonical_name = po.canonical_name
        left join {{ ref("int_gender_classification") }} gc on nm.canonical_name = gc.canonical_name
        left join {{ ref("int_ethnicity_classification") }} ec on nm.canonical_name = ec.canonical_name
    )

select *
from persons
