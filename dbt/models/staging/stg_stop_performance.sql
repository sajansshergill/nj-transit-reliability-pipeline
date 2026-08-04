-- On DuckDB (local/CI) we read the seeded sample; on Snowflake (prod) we read
-- the loaded raw source. One model, portable across both targets.

with source as (
    {% if target.name == 'duckdb' %}
    select * from {{ ref('raw_stop_performance') }}
    {% else %}
    select * from {{ source('raw', 'stop_performance') }}
    {% endif %}
)

select
    trip_id,
    route_id,
    coalesce(route_short_name, route_id)          as line_name,
    cast(direction_id as varchar)                 as direction_id,
    stop_id,
    stop_name,
    cast(stop_sequence as integer)                as stop_sequence,
    cast(delay_seconds as integer)                as delay_seconds,
    cast(on_time_flag as integer)                 as on_time_flag,
    cast(service_date as date)                    as service_date,
    arrival_hour_ts,
    weather_station,
    cast(temperature_c as double)                 as temperature_c,
    cast(precipitation_mm as double)              as precipitation_mm,
    cast(wind_speed_kmh as double)                as wind_speed_kmh,
    cast(weather_code as integer)                 as weather_code
from source
where trip_id is not null
  and stop_id is not null
  and delay_seconds is not null