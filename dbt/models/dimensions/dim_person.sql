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
-- ethnicity classification:
-- uses Singapore's CMIO framework (Chinese, Malay, Indian, Others) with name pattern matching
-- classification order (by distinctiveness): Malay → Indian → Chinese → Others
-- see ethnicity_patterns seed and schema.yml for methodology and limitations

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
    ),

    -- get prefixes and postfixes
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

    -- scan all prefix history for gendered prefixes (fallback when current prefix is gender-neutral)
    -- e.g. "Ms Jane DOE" → "Dr Jane DOE" still yields F from historical prefix
    historical_gender_from_prefix as (
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
            end as gender_from_prefix
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

    -- extract gender from patronymic patterns in the canonical name
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

    -- ethnicity patterns from seed
    ethnicity_patterns as (
        select * from {{ ref("ethnicity_patterns") }}
    ),

    -- list of common prefixes to strip out before surname extraction
    prefix_list as (
        select prefix from unnest([
            'Mr', 'Mr.', 'Ms', 'Ms.', 'Mrs', 'Mrs.', 'Miss', 'Mdm', 'Mdm.',
            'MR', 'MR.', 'MS', 'MS.', 'MRS', 'MRS.', 'MISS', 'MDM', 'MDM.',
            'Dr', 'Dr.', 'DR', 'DR.',
            'Prof', 'Prof.', 'PROF', 'PROF.',
            'Er', 'Er.', 'ER', 'ER.',
            'Ar', 'Ar.', 'AR', 'AR.',
            'Assoc Prof', 'Assoc Prof.',
            'ASSOC PROF', 'ASSOC PROF.',
            'Adj Assoc Prof', 'Adj Assoc Prof.',
            'Clin Assoc Prof', 'Clin Assoc Prof.',
            'Asst Prof', 'Asst Prof.',
            'Sr Asst Prof', 'Sr Asst Prof.',
            'Adj Asst Prof', 'Adj Asst Prof.',
            'Visiting Prof', 'Visiting Prof.',
            'Emeritus Prof', 'Emeritus Prof.',
            'Professor', 'Professor.',
            'Associate Professor', 'Associate Professor.',
            'Assistant Professor', 'Assistant Professor.',
            'AC', 'DAC', 'SAC', 'SUPT', 'DSP', 'ASP', 'INSP',
            'LTC', 'MAJ', 'CPT', 'LTA', 'COL', 'SLTC', 'BG', 'MG', 'LG',
            'RADM', 'ME', 'SLTC(NS)', 'COL(NS)', 'LTC(NS)', 'MAJ(NS)',
            'Senior Minister', 'Minister', 'Justice', 'Judge',
            'Hon', 'Hon.', 'The Hon', 'The Hon.',
            'Sen', 'Sen.',
            'Sir', 'Dame', 'Lady', 'Lord',
            'A/Prof', 'A/Prof.',
            'Adj. A/Prof', 'Adj. A/Prof.',
            'Clin A/Prof', 'Clin A/Prof.',
            'Adj A/Prof', 'Adj A/Prof.'
        ]) as prefix
    ),

    -- extract surname (first word after stripping prefix) for surname matching
    names_with_parts as (
        select
            nm.canonical_name,
            -- remove known prefixes to get the actual name (case-insensitive)
            -- A/Prof must come before A/ patterns to avoid false positives
            trim(
                regexp_replace(
                    nm.canonical_name,
                    r'^(?i)(Adj\.? A/Prof\.?|Clin A/Prof\.?|A/Prof\.?|Mr\.|Mrs\.|Ms\.|Miss|Mdm\.|Dr\.|Prof\.|Er\.|Ar\.|Assoc Prof\.?|Adj Assoc Prof\.?|Clin Assoc Prof\.?|Asst Prof\.?|Sr Asst Prof\.?|Adj Asst Prof\.?|Visiting Prof\.?|Emeritus Prof\.?|Professor\.?|Associate Professor\.?|Assistant Professor\.?|AC|DAC|SAC|SUPT|DSP|ASP|INSP|LTC|MAJ|CPT|LTA|COL|SLTC|BG|MG|LG|RADM|ME|SLTC\(NS\)|COL\(NS\)|LTC\(NS\)|MAJ\(NS\)|Senior Minister|Minister|Justice|Judge|Hon\.|The Hon\.|Sen\.|Mr|Mrs|Ms|Mdm|Dr|Prof|Er|Ar|Hon|Sen|Sir|Dame|Lady|Lord)\s+',
                    ''
                )
            ) as clean_name,
            -- lowercase for case-insensitive matching
            lower(nm.canonical_name) as name_lower
        from names_mapping nm
    ),

    -- now extract surname from cleaned name
    -- strategies:
    -- 1. Look for FULLY CAPITALIZED word (common SG convention to capitalize surnames)
    -- 2. First word (traditional Chinese format: Surname GivenName)
    -- 3. Last word (westernized format: GivenName SURNAME)
    -- 4. Check all words for surname matches
    names_with_surname as (
        select
            canonical_name,
            clean_name,
            split(clean_name, ' ')[safe_offset(0)] as first_word,
            -- last word (for westernized names like "Rachel TAN")
            array_reverse(split(clean_name, ' '))[safe_offset(0)] as last_word,
            -- find fully capitalized words (likely surnames) - must be 2+ chars and all uppercase
            -- excludes single letters and mixed case
            (
                select string_agg(word, ' ')
                from unnest(split(clean_name, ' ')) as word
                where length(word) >= 2 
                and word = upper(word)
                and regexp_contains(word, r'^[A-Z]+$')
            ) as capitalized_words,
            -- all words for comprehensive matching
            split(clean_name, ' ') as all_words,
            -- word count helps identify single-word names
            array_length(split(clean_name, ' ')) as word_count
        from names_with_parts
    ),

    -- classify ethnicity using pattern matching
    -- order of precedence: Malay (most distinctive patterns) → Indian → Chinese → Others
    ethnicity_classification as (
        select
            n.canonical_name,
            n.first_word,
            n.last_word,
            n.capitalized_words,
            lower(n.clean_name) as name_lower,
            case
                -- 1. Malay: check for bin/binti patterns first (most distinctive)
                -- Use word boundaries to avoid matching 'bin' inside 'Bing'
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Malay'
                    and p.pattern_type = 'pattern'
                    and (
                        lower(n.clean_name) like '% ' || lower(p.pattern) || ' %'  -- middle of name
                        or lower(n.clean_name) like lower(p.pattern) || ' %'  -- start of name
                        or lower(n.clean_name) like '% ' || lower(p.pattern)  -- end of name
                    )
                ) then 'Malay'
                
                -- 2. Malay: check for name_start patterns (must be followed by space)
                -- Skip if the first word is ALL UPPERCASE (likely a surname, not a Malay prefix)
                when n.first_word != upper(n.first_word) 
                and exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Malay'
                    and p.pattern_type = 'name_start'
                    and (
                        lower(n.first_word) = lower(p.pattern)
                        or lower(n.clean_name) like lower(p.pattern) || ' %'
                    )
                ) then 'Malay'
                
                -- 3. Malay: check for name_end patterns (must be a whole word ending)
                -- Skip if the last word is ALL UPPERCASE (likely a surname)
                when n.last_word != upper(n.last_word)
                and exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Malay'
                    and p.pattern_type = 'name_end'
                    -- Only check the LAST word, not any word in the name
                    and lower(n.last_word) like '%' || lower(p.pattern)
                ) then 'Malay'
                
                -- 4. Malay: check for full name patterns (must be whole word match)
                -- Skip if the matching word is ALL UPPERCASE (likely a surname)
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Malay'
                    and p.pattern_type = 'name'
                    and (
                        (lower(n.first_word) = lower(p.pattern) and n.first_word != upper(n.first_word))
                        or (lower(n.last_word) = lower(p.pattern) and n.last_word != upper(n.last_word))
                        or lower(n.clean_name) like '% ' || lower(p.pattern) || ' %'
                    )
                ) then 'Malay'
                
                -- 5. Indian: check for s/o, d/o patterns (very distinctive)
                -- must be surrounded by spaces to avoid matching "A/Prof" etc.
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Indian'
                    and p.pattern_type = 'pattern'
                    and (
                        lower(n.clean_name) like '% ' || lower(p.pattern) || ' %'
                        or lower(n.clean_name) like lower(p.pattern) || ' %'
                        or lower(n.clean_name) like '% ' || lower(p.pattern)
                    )
                ) then 'Indian'
                
                -- 6. Indian: check for surname in CAPITALIZED words
                when n.capitalized_words is not null and exists (
                    select 1 
                    from unnest(split(n.capitalized_words, ' ')) as cap_word
                    join ethnicity_patterns p
                        on p.ethnicity = 'Indian'
                        and p.pattern_type = 'surname'
                        and lower(cap_word) = lower(p.pattern)
                ) then 'Indian'
                
                -- 7. Indian: check for surname patterns (first or last word)
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Indian'
                    and p.pattern_type = 'surname'
                    and (lower(n.first_word) = lower(p.pattern) or lower(n.last_word) = lower(p.pattern))
                ) then 'Indian'
                
                -- 7. Indian: check for name_start patterns (must be followed by space or end of string)
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Indian'
                    and p.pattern_type = 'name_start'
                    and (
                        lower(n.first_word) = lower(p.pattern)
                        or lower(n.clean_name) like lower(p.pattern) || ' %'
                    )
                ) then 'Indian'
                
                -- 8. Chinese: check for surname in CAPITALIZED words first (most reliable)
                -- In Singapore, surnames are often written in ALL CAPS (e.g., "Jackson CHUA Boon Keng")
                when n.capitalized_words is not null and exists (
                    select 1 
                    from unnest(split(n.capitalized_words, ' ')) as cap_word
                    join ethnicity_patterns p
                        on p.ethnicity = 'Chinese'
                        and p.pattern_type = 'surname'
                        and lower(cap_word) = lower(p.pattern)
                ) then 'Chinese'
                
                -- 9. Chinese: check for surname in first word (traditional format: Surname GivenName)
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Chinese'
                    and p.pattern_type = 'surname'
                    and lower(n.first_word) = lower(p.pattern)
                ) then 'Chinese'
                
                -- 10. Chinese: check for surname in last word (westernized format: GivenName SURNAME)
                when n.word_count > 1 and exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Chinese'
                    and p.pattern_type = 'surname'
                    and lower(n.last_word) = lower(p.pattern)
                ) then 'Chinese'
                
                -- 11. Chinese: check ALL words for any Chinese surname match
                -- This catches names like "Jackson CHUA Boon Keng" where surname is in the middle
                when exists (
                    select 1 
                    from unnest(n.all_words) as word
                    join ethnicity_patterns p
                        on p.ethnicity = 'Chinese'
                        and p.pattern_type = 'surname'
                        and lower(word) = lower(p.pattern)
                ) then 'Chinese'
                
                -- 12. Indian: check ALL words for any Indian surname match
                when exists (
                    select 1 
                    from unnest(n.all_words) as word
                    join ethnicity_patterns p
                        on p.ethnicity = 'Indian'
                        and p.pattern_type = 'surname'
                        and lower(word) = lower(p.pattern)
                ) then 'Indian'
                
                -- 13. Others: check for Eurasian/Western surnames (first or last word)
                -- This comes AFTER Chinese/Indian to avoid misclassifying Asian names with
                -- surnames that happen to match Western surnames (e.g., "King", "Young")
                when exists (
                    select 1 from ethnicity_patterns p
                    where p.ethnicity = 'Other'
                    and p.pattern_type = 'surname'
                    and (lower(n.first_word) = lower(p.pattern) or lower(n.last_word) = lower(p.pattern))
                ) then 'Other'
                
                -- 14. Default: Unknown (could not classify)
                else 'Unknown'
            end as predicted_ethnicity
        from names_with_surname n
    ),

    persons as (
        select
            {{ dbt_utils.generate_surrogate_key(["nm.canonical_name"]) }} as person_key,
            nm.canonical_name as full_name,
            pf.prefix,
            po.postfix,
            pe.email,
            pe.email is not null as has_personal_email,
            -- predicted_gender: layered approach
            -- 1. current prefix (most reliable, includes sir/lord/dame/lady)
            -- 2. historical prefix (person was once "Ms X", now "Dr X")
            -- 3. patronymic patterns in name (bin/s/o → M, binti/d/o → F)
            coalesce(
                case
                    when lower(replace(pf.prefix, '.', ''))
                        in ('mr', 'sir', 'lord')
                    then 'M'
                    when lower(replace(pf.prefix, '.', ''))
                        in ('ms', 'mdm', 'miss', 'mrs', 'assoc prof (ms)', 'dame', 'lady')
                    then 'F'
                    else null
                end,
                hgp.gender_from_prefix,
                pg.gender_from_patronymic
            ) as predicted_gender,
            ec.predicted_ethnicity
        from names_mapping nm
        left join personal_emails pe on nm.canonical_name = pe.canonical_name
        left join prefixes pf on nm.canonical_name = pf.canonical_name
        left join postfixes po on nm.canonical_name = po.canonical_name
        left join ethnicity_classification ec on nm.canonical_name = ec.canonical_name
        left join historical_gender_from_prefix hgp on nm.canonical_name = hgp.canonical_name
        left join patronymic_gender pg on nm.canonical_name = pg.canonical_name
    )

select *
from persons
