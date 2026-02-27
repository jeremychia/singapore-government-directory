-- int_gender_classification: derive predicted gender using layered fallback approach
-- grain: one row per canonical_name
-- layers (in priority order):
--   1. current prefix (most reliable, includes sir/lord/dame/lady)
--   2. historical prefix (person was once "Ms X", now "Dr X")
--   3. gendered prefix in name itself (e.g. "Ms Ruby Chopra")
--   4. patronymic patterns in name (bin/s/o → M, binti/d/o → F)
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

    -- get current prefixes (deduplicated - take longest prefix if multiple)
    prefixes as (
        select extracted_name as canonical_name, prefix
        from {{ ref("stg_preprocessed_prefixes_history") }}
        where is_latest = true
        qualify row_number() over (
            partition by extracted_name
            order by length(prefix) desc
        ) = 1
    ),

    -- layer 1: gender from current prefix
    current_prefix_gender as (
        select
            canonical_name,
            case
                when lower(replace(prefix, '.', ''))
                    in ('mr', 'sir', 'lord')
                then 'M'
                when lower(replace(prefix, '.', ''))
                    in ('ms', 'mdm', 'miss', 'mrs', 'assoc prof (ms)', 'dame', 'lady')
                then 'F'
                else null
            end as gender_from_current_prefix
        from prefixes
    ),

    -- layer 2: scan all prefix history for gendered prefixes (fallback when current prefix is gender-neutral)
    -- e.g. "Ms Jane DOE" → "Dr Jane DOE" still yields F from historical prefix
    historical_prefix_gender as (
        select
            extracted_name as canonical_name,
            case
                when lower(replace(prefix, '.', ''))
                    in ('mr', 'sir', 'lord')
                then 'M'
                when lower(replace(prefix, '.', ''))
                    in ('ms', 'mdm', 'miss', 'mrs', 'assoc prof (ms)', 'dame', 'lady')
                then 'F'
                else null
            end as gender_from_historical_prefix
        from {{ ref("stg_preprocessed_prefixes_history") }}
        where case
                when lower(replace(prefix, '.', ''))
                    in ('mr', 'sir', 'lord')
                then 'M'
                when lower(replace(prefix, '.', ''))
                    in ('ms', 'mdm', 'miss', 'mrs', 'assoc prof (ms)', 'dame', 'lady')
                then 'F'
                else null
            end is not null
        qualify row_number() over (
            partition by extracted_name
            order by effective_to desc
        ) = 1
    ),

    -- layer 3: extract gender from gendered prefixes embedded in the canonical name itself
    -- e.g. "Ms RUBY CHOPRA", "Mdm Chilukuri Dimps Rao", "Mr John Doe"
    -- this catches names where the prefix wasn't separated into the prefix table
    name_prefix_gender as (
        select
            canonical_name,
            case
                when regexp_contains(lower(canonical_name), r'^(mr|sir|lord)[\s\.]')
                then 'M'
                when regexp_contains(lower(canonical_name), r'^(ms|mdm|miss|mrs|dame|lady)[\s\.]')
                then 'F'
                else null
            end as gender_from_name_prefix
        from names_mapping
    ),

    -- layer 4: extract gender from patronymic patterns in the canonical name
    -- Malay: bin/b. = male, binti/bt./bte/binte = female
    -- Indian: s/o/a/l = male, d/o/a/p = female
    patronymic_gender as (
        select
            canonical_name,
            case
                when regexp_contains(
                    lower(canonical_name),
                    r'(?:^|\s)(?:bin|b\.)(?:\s|$)'
                ) then 'M'
                when regexp_contains(
                    lower(canonical_name),
                    r'(?:^|\s)(?:binti|bt\.|bte|binte)(?:\s|$)'
                ) then 'F'
                when regexp_contains(
                    lower(canonical_name),
                    r'(?:^|\s)(?:s/o|a/l)(?:\s|$)'
                ) then 'M'
                when regexp_contains(
                    lower(canonical_name),
                    r'(?:^|\s)(?:d/o|a/p)(?:\s|$)'
                ) then 'F'
                else null
            end as gender_from_patronymic
        from names_mapping
    ),

    -- combine all layers with coalesce (first non-null wins)
    combined as (
        select
            nm.canonical_name,
            coalesce(
                cpg.gender_from_current_prefix,
                hpg.gender_from_historical_prefix,
                npg.gender_from_name_prefix,
                pg.gender_from_patronymic
            ) as predicted_gender,
            -- expose which layer was used for debugging/analysis
            case
                when cpg.gender_from_current_prefix is not null then 'current_prefix'
                when hpg.gender_from_historical_prefix is not null then 'historical_prefix'
                when npg.gender_from_name_prefix is not null then 'name_prefix'
                when pg.gender_from_patronymic is not null then 'patronymic'
                else null
            end as gender_source
        from names_mapping nm
        left join current_prefix_gender cpg on nm.canonical_name = cpg.canonical_name
        left join historical_prefix_gender hpg on nm.canonical_name = hpg.canonical_name
        left join name_prefix_gender npg on nm.canonical_name = npg.canonical_name
        left join patronymic_gender pg on nm.canonical_name = pg.canonical_name
    )

select *
from combined
