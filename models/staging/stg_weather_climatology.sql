with source as (
    select * from {{ source('weather_data', 'CLIMATOLOGY_DAY') }}
),

renamed as (
    select
        postal_code,
        country,
        doy_std as day_of_year,
        
        -- Temperature averages in Fahrenheit
        avg_of__daily_min_temperature_air_f as temp_min_avg_f,
        avg_of__daily_avg_temperature_air_f as temp_avg_avg_f,
        avg_of__daily_max_temperature_air_f as temp_max_avg_f,
        
        -- Temperature standard deviations in Fahrenheit
        std_of__daily_min_temperature_air_f as temp_min_std_f,
        std_of__daily_avg_temperature_air_f as temp_avg_std_f,
        std_of__daily_max_temperature_air_f as temp_max_std_f,
        
        -- Temperature averages in Celsius (converted)
        {{ fahrenheit_to_celsius('avg_of__daily_min_temperature_air_f') }} as temp_min_avg_c,
        {{ fahrenheit_to_celsius('avg_of__daily_avg_temperature_air_f') }} as temp_avg_avg_c,
        {{ fahrenheit_to_celsius('avg_of__daily_max_temperature_air_f') }} as temp_max_avg_c,
        
        -- Dewpoint climatology
        avg_of__daily_min_temperature_dewpoint_f as dewpoint_min_avg_f,
        avg_of__daily_avg_temperature_dewpoint_f as dewpoint_avg_avg_f,
        avg_of__daily_max_temperature_dewpoint_f as dewpoint_max_avg_f,
        
        -- Wetbulb climatology
        avg_of__daily_min_temperature_wetbulb_f as wetbulb_min_avg_f,
        avg_of__daily_avg_temperature_wetbulb_f as wetbulb_avg_avg_f,
        avg_of__daily_max_temperature_wetbulb_f as wetbulb_max_avg_f
        
    from source
)

select * from renamed