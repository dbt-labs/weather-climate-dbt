# Weather Climate dbt Project

A dbt project for analyzing global weather and climate data using the Snowflake Marketplace weather dataset.

## 🌡️ **Project Overview**

This project transforms raw weather data into analytics-ready models and provides a semantic layer for easy querying. Built for exploratory data analysis and weather insights.

## 📊 **Data Sources**

- **Source**: `DX_GLOBAL_WEATHER__CLIMATE_DATA_FOR_BI` (Snowflake Marketplace)
- **Tables**:
  - `HISTORY_DAY` - Historical daily weather observations
  - `FORECAST_DAY` - Weather forecasts
  - `CLIMATOLOGY_DAY` - Long-term climate averages

## 🏗️ **Project Structure**

```
models/
├── staging/           # Clean, renamed source data
│   ├── stg_weather_history.sql
│   ├── stg_weather_forecast.sql
│   └── stg_weather_climatology.sql
├── marts/            # Business logic models
│   ├── mart_weather_daily.sql
│   ├── mart_weather_forecast_daily.sql
│   ├── mart_weather_climate_daily.sql
│   └── metricflow_time_spine.sql
└── semantic/         # Semantic layer
    ├── semantic_weather_daily.yml
    └── metrics.yml
```

## 📈 **Available Metrics**

- `average_temperature_f/c` - Average temperatures
- `min/max_temperature_f` - Temperature extremes
- `average_daily_temp_range` - Daily temperature variation
- `temperature_anomaly` - Deviation from climate normal
- `temperature_z_score` - Standardized temperature anomaly
- `total_observations` - Count of weather records

## 🚀 **Getting Started**

1. **Install dependencies:**
   ```bash
   dbt deps
   ```

2. **Run the project:**
   ```bash
   dbt run
   ```

3. **Generate documentation:**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

## 🔧 **Requirements**

- dbt Core 1.9+
- Snowflake connection
- Access to weather marketplace data

## 📝 **Notes**

Built for demo and exploratory analysis of weather patterns, climate anomalies, and forecast accuracy.
