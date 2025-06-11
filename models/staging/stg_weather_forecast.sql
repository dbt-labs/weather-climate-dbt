with source as (
    select * from {{ source('weather_data', 'FORECAST_DAY') }}
),

renamed as (
    select
        postal_code,
        country,
        time_init_utc as forecast_init_time,
        date_valid_std as forecast_date,
        doy_std as day_of_year,
        
        -- Temperature metrics in Fahrenheit
        min_temperature_air_2m_f as temp_min_f,
        avg_temperature_air_2m_f as temp_avg_f,
        max_temperature_air_2m_f as temp_max_f,
        
        -- Temperature metrics in Celsius (converted)
        {{ fahrenheit_to_celsius('min_temperature_air_2m_f') }} as temp_min_c,
        {{ fahrenheit_to_celsius('avg_temperature_air_2m_f') }} as temp_avg_c,
        {{ fahrenheit_to_celsius('max_temperature_air_2m_f') }} as temp_max_c,
        
        -- Other weather metrics if available
        min_temperature_dewpoint_2m_f as dewpoint_min_f,
        avg_temperature_dewpoint_2m_f as dewpoint_avg_f,
        max_temperature_dewpoint_2m_f as dewpoint_max_f,
        
        min_temperature_wetbulb_2m_f as wetbulb_min_f,
        avg_temperature_wetbulb_2m_f as wetbulb_avg_f,
        max_temperature_wetbulb_2m_f as wetbulb_max_f,
        
        -- Add derived fields
        extract(year from date_valid_std) as forecast_year,
        extract(month from date_valid_std) as forecast_month,
        extract(day from date_valid_std) as forecast_day,
        
        -- Forecast horizon (days ahead)
        datediff('day', current_date(), date_valid_std) as days_ahead
        
    from source
)

select * from renamed