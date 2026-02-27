-- fact_role_history: scd type 2 tracking of role assignments
-- grain: one row per person + department + position + stint
-- a stint is a continuous period of employment in the same role

with
    -- join raw names with mapping to get canonical names
    names_with_mapping as (
        select
            nm.name as canonical_name,
            rn.email,
            rn.position,
            rn.department_url,
            rn.ministry_name,
            -- first run had some values in 2024-05-06, should be grouped as part of 2024-05-07
            if(
                cast(rn._accessed_at as date) = '2024-05-06',
                '2024-05-07',
                cast(rn._accessed_at as date)
            ) as observed_date
        from {{ ref("stg_raw_names") }} rn
        left join {{ ref("stg_preprocessed_names_mapping") }} nm
            on rn.name = nm.extracted_name
        where
            rn.name is not null
            and rn.name not in ('', '-', '--')
            and rn.email is not null
            and rn.email not in ('-')
    ),

    -- detect gaps: if >30 days between observations, treat as new stint
    with_gaps as (
        select
            *,
            case
                when date_diff(
                    observed_date,
                    lag(observed_date) over (
                        partition by canonical_name, position, department_url
                        order by observed_date
                    ),
                    day
                ) > 30
                then 1
                else 0
            end as is_new_stint
        from names_with_mapping
    ),

    with_stint_id as (
        select
            *,
            sum(is_new_stint) over (
                partition by canonical_name, position, department_url
                order by observed_date
            ) as stint_id
        from with_gaps
    ),

    -- aggregate to get validity periods
    role_assignments as (
        select
            canonical_name,
            email,
            position,
            department_url,
            ministry_name,
            stint_id,
            min(observed_date) as observed_from,
            max(observed_date) as observed_to,
            count(*) as observation_count
        from with_stint_id
        group by all
    ),

    -- add is_current flag
    with_current_flag as (
        select
            *,
            observed_to = max(observed_to) over (partition by canonical_name) as is_current
        from role_assignments
    ),

    -- join with dim_person to get person_key
    final as (
        select
            {{ dbt_utils.generate_surrogate_key([
                "ra.canonical_name",
                "ra.position",
                "ra.department_url",
                "ra.stint_id"
            ]) }} as role_assignment_key,
            dp.person_key,
            ra.canonical_name as full_name,
            ra.email,
            ra.position,
            ra.department_url,
            ra.ministry_name,
            ra.observed_from,
            ra.observed_to,
            ra.observation_count,
            ra.is_current
        from with_current_flag ra
        left join {{ ref("dim_person") }} dp
            on ra.canonical_name = dp.full_name
    )

select *
from final
