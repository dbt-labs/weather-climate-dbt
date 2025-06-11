{{
    config(
        materialized='view'
    )
}}

with weather_history as (
    select * from {{ ref('stg_weather_history') }}
),

weather_climatology as (
    select * from {{ ref('stg_weather_climatology') }}
),

daily_weather as (
    select
        -- Identifiers
        h.postal_code,
        h.country,
        h.observation_date,
        
        -- Date dimensions
        h.observation_year,
        h.observation_month,
        h.observation_day,
        h.day_of_year,
        
        -- Actual temperatures (Fahrenheit)
        h.temp_min_f as actual_temp_min_f,
        h.temp_avg_f as actual_temp_avg_f,
        h.temp_max_f as actual_temp_max_f,
        
        -- Actual temperatures (Celsius)
        h.temp_min_c as actual_temp_min_c,
        h.temp_avg_c as actual_temp_avg_c,
        h.temp_max_c as actual_temp_max_c,
        
        -- Climate normals (Fahrenheit)
        c.temp_min_avg_f as normal_temp_min_f,
        c.temp_avg_avg_f as normal_temp_avg_f,
        c.temp_max_avg_f as normal_temp_max_f,
        
        -- Climate normals (Celsius)
        c.temp_min_avg_c as normal_temp_min_c,
        c.temp_avg_avg_c as normal_temp_avg_c,
        c.temp_max_avg_c as normal_temp_max_c,
        
        -- Deviations from normal (Fahrenheit)
        h.temp_min_f - c.temp_min_avg_f as temp_min_deviation_f,
        h.temp_avg_f - c.temp_avg_avg_f as temp_avg_deviation_f,
        h.temp_max_f - c.temp_max_avg_f as temp_max_deviation_f,
        
        -- Deviations from normal (Celsius)
        h.temp_min_c - c.temp_min_avg_c as temp_min_deviation_c,
        h.temp_avg_c - c.temp_avg_avg_c as temp_avg_deviation_c,
        h.temp_max_c - c.temp_max_avg_c as temp_max_deviation_c,
        
        -- Standard deviations from normal
        case 
            when c.temp_avg_std_f > 0 
            then (h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f 
            else 0 
        end as temp_avg_std_devs_from_normal,
        
        -- Other weather metrics
        h.dewpoint_min_f,
        h.dewpoint_avg_f,
        h.dewpoint_max_f,
        h.wetbulb_min_f,
        h.wetbulb_avg_f,
        h.wetbulb_max_f,
        
        -- Climate variability
        c.temp_min_std_f as normal_temp_min_std_f,
        c.temp_avg_std_f as normal_temp_avg_std_f,
        c.temp_max_std_f as normal_temp_max_std_f
        
    from weather_history h
    left join weather_climatology c
        on h.postal_code = c.postal_code
        and h.country = c.country
        and h.day_of_year = c.day_of_year
)

select * from daily_weather