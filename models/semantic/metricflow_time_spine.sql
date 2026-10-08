{{
    config(
        materialized='table',
    )
}}

with days as (
    {{
        dbt_date.date_spine(
            'day',
            "cast('2026-01-01' as date)",
            "cast('2027-01-01' as date)"
        )
        if False else ""
    }}
    select
        cast(range as date) as date_day
    from range(
        date '2026-01-01',
        date '2027-01-01',
        interval 1 day
    )
)

select
    date_day
from days
