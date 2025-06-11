{{ config(
    materialized='table',
) }}

with days as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

cast_to_date as (
    select cast(date_day as date) as date_day
    from days
)

select * from cast_to_date