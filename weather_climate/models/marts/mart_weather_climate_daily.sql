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

climate_context as (
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
        
        -- Day of week and seasonality
        extract(dayofweek from h.observation_date) as day_of_week,
        case
            when h.observation_month in (12, 1, 2) then 'Winter'
            when h.observation_month in (3, 4, 5) then 'Spring'
            when h.observation_month in (6, 7, 8) then 'Summer'
            when h.observation_month in (9, 10, 11) then 'Fall'
        end as season,
        
        -- Actual temperatures
        h.temp_min_f as actual_temp_min_f,
        h.temp_avg_f as actual_temp_avg_f,
        h.temp_max_f as actual_temp_max_f,
        h.temp_avg_c as actual_temp_avg_c,
        
        -- Climate normals
        c.temp_avg_avg_f as normal_temp_avg_f,
        c.temp_avg_avg_c as normal_temp_avg_c,
        c.temp_avg_std_f as normal_temp_std_f,
        
        -- Temperature anomaly analysis
        h.temp_avg_f - c.temp_avg_avg_f as temp_anomaly_f,
        h.temp_avg_c - c.temp_avg_avg_c as temp_anomaly_c,
        
        -- Standardized anomaly (z-score)
        case 
            when c.temp_avg_std_f > 0 
            then (h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f 
            else 0 
        end as temp_anomaly_z_score,
        
        -- Anomaly classification
        case
            when c.temp_avg_std_f > 0 then
                case 
                    when abs((h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f) < 1 then 'Normal'
                    when (h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f >= 2 then 'Much Above Normal'
                    when (h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f >= 1 then 'Above Normal'
                    when (h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f <= -2 then 'Much Below Normal'
                    when (h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f <= -1 then 'Below Normal'
                end
            else 'Normal'
        end as temperature_category,
        
        -- Extreme indicators
        case 
            when c.temp_avg_std_f > 0 
                and abs((h.temp_avg_f - c.temp_avg_avg_f) / c.temp_avg_std_f) >= 3 
            then true 
            else false 
        end as is_extreme_temperature,
        
        -- Temperature range
        h.temp_max_f - h.temp_min_f as daily_temp_range_f,
        h.temp_max_c - h.temp_min_c as daily_temp_range_c,
        
        -- Comfort indicators (basic)
        case
            when h.temp_avg_f between 65 and 75 then true
            else false
        end as is_comfortable_temp,
        
        -- Heat/cold stress indicators
        case when h.temp_max_f >= 90 then true else false end as is_hot_day,
        case when h.temp_max_f >= 100 then true else false end as is_extreme_heat_day,
        case when h.temp_min_f <= 32 then true else false end as is_freezing_day,
        case when h.temp_min_f <= 0 then true else false end as is_extreme_cold_day
        
    from weather_history h
    left join weather_climatology c
        on h.postal_code = c.postal_code
        and h.country = c.country
        and h.day_of_year = c.day_of_year
)

select * from climate_context