{{
    config(
        materialized='view'
    )
}}

with weather_forecast as (
    select * from {{ ref('stg_weather_forecast') }}
),

weather_history as (
    select * from {{ ref('stg_weather_history') }}
),

forecast_with_actuals as (
    select
        -- Identifiers
        f.postal_code,
        f.country,
        f.forecast_date,
        f.forecast_init_time,
        
        -- Date dimensions
        f.forecast_year,
        f.forecast_month,
        f.forecast_day,
        f.day_of_year,
        f.days_ahead,
        
        -- Forecast temperatures (Fahrenheit)
        f.temp_min_f as forecast_temp_min_f,
        f.temp_avg_f as forecast_temp_avg_f,
        f.temp_max_f as forecast_temp_max_f,
        
        -- Forecast temperatures (Celsius)
        f.temp_min_c as forecast_temp_min_c,
        f.temp_avg_c as forecast_temp_avg_c,
        f.temp_max_c as forecast_temp_max_c,
        
        -- Actual temperatures (if available)
        h.temp_min_f as actual_temp_min_f,
        h.temp_avg_f as actual_temp_avg_f,
        h.temp_max_f as actual_temp_max_f,
        
        h.temp_min_c as actual_temp_min_c,
        h.temp_avg_c as actual_temp_avg_c,
        h.temp_max_c as actual_temp_max_c,
        
        -- Forecast errors (when actuals are available)
        case 
            when h.observation_date is not null 
            then f.temp_min_f - h.temp_min_f 
        end as forecast_error_min_f,
        
        case 
            when h.observation_date is not null 
            then f.temp_avg_f - h.temp_avg_f 
        end as forecast_error_avg_f,
        
        case 
            when h.observation_date is not null 
            then f.temp_max_f - h.temp_max_f 
        end as forecast_error_max_f,
        
        -- Absolute errors
        case 
            when h.observation_date is not null 
            then abs(f.temp_avg_f - h.temp_avg_f)
        end as forecast_abs_error_avg_f,
        
        -- Forecast metadata
        case 
            when h.observation_date is not null 
            then true 
            else false 
        end as has_actual_data,
        
        -- Other forecast metrics
        f.dewpoint_min_f as forecast_dewpoint_min_f,
        f.dewpoint_avg_f as forecast_dewpoint_avg_f,
        f.dewpoint_max_f as forecast_dewpoint_max_f,
        
        f.wetbulb_min_f as forecast_wetbulb_min_f,
        f.wetbulb_avg_f as forecast_wetbulb_avg_f,
        f.wetbulb_max_f as forecast_wetbulb_max_f
        
    from weather_forecast f
    left join weather_history h
        on f.postal_code = h.postal_code
        and f.country = h.country
        and f.forecast_date = h.observation_date
)

select * from forecast_with_actuals