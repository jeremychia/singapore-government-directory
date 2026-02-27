-- fact_current_roles: current role assignments for all active civil servants
-- grain: one row per person (who is currently employed)
-- note: a person may have multiple rows if they hold multiple concurrent positions

with
    current_roles as (
        select
            role_assignment_key,
            person_key,
            full_name,
            email,
            position,
            department_url,
            ministry_name,
            observed_from,
            observed_to,
            observation_count
        from {{ ref("fact_role_history") }}
        where is_current = true
    )

select *
from current_roles
