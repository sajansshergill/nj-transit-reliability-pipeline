-- Fails if any weather-level on-time percentage falls outside 0-100.
select line_name, weather_condition, on_time_pct
from {{ ref('mart_ontime_by_weather') }}
where on_time_pct < 0 or on_time_pct > 100