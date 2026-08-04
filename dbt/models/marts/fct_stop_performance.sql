-- Grain: one row per trip / stop. Adds a human-readable weather bucket.

select
    *,
    case
        when coalesce(precipitation_mm, 0) >= 2.5 then 'rain'
        when coalesce(precipitation_mm, 0) > 0    then 'light_precip'
        when coalesce(wind_speed_kmh, 0) >= 40    then 'high_wind'
        else 'clear'
    end as weather_condition
from {{ ref('stg_stop_performance') }}