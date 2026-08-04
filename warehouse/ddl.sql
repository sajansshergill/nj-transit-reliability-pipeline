-- Snowflake DDL for the raw landing table + external stage.
-- Run once during setup (or via `snowsql -f warehouse/ddl.sql`).

CREATE DATABASE IF NOT EXISTS NJT_RELIABILITY;
CREATE SCHEMA   IF NOT EXISTS NJT_RELIABILITY.RAW;

-- External stage pointing at the silver layer in S3.
CREATE STAGE IF NOT EXISTS NJT_RELIABILITY.RAW.NJT_SILVER_STAGE
    URL = 's3://njt-reliability-dev/silver/stop_performance/'
    FILE_FORMAT = (TYPE = PARQUET);

CREATE TABLE IF NOT EXISTS NJT_RELIABILITY.RAW.STOP_PERFORMANCE (
    trip_id               STRING,
    route_id              STRING,
    route_short_name      STRING,
    direction_id          STRING,
    stop_id               STRING,
    stop_name             STRING,
    stop_sequence         INTEGER,
    actual_arrival_epoch  BIGINT,
    delay_seconds         INTEGER,
    on_time_flag          INTEGER,
    service_date          DATE,
    arrival_hour_ts       STRING,
    weather_station       STRING,
    temperature_c         FLOAT,
    precipitation_mm      FLOAT,
    wind_speed_kmh        FLOAT,
    weather_code          INTEGER,
    _loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);