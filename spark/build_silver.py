"""PySpark job: build the silver stop-performance table.

Reads the bronze layer (parsed realtime rows + static GTFS + weather),
joins scheduled vs. actual to compute delay and an on-time flag, assigns
each stop to its nearest weather station, and joins hourly weather.

Business rules come from common.transforms so Spark and the unit tests
share one definition. Run with:

    spark-submit spark/build_silver.py --ds 2026-08-02
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType

from common.config import settings
from common.constants import SILVER_PREFIX, WEATHER_STATIONS
from common.transforms import gtfs_time_to_epoch, nearest_station


def _lake(path: str) -> str:
    return f"s3a://{settings.s3_bucket}/{path}"


def build(spark: SparkSession, ds: str) -> None:
    threshold = settings.on_time_threshold_seconds
    stations = [(n, lat, lon) for n, (lat, lon) in WEATHER_STATIONS.items()]
    scheduled_epoch_udf = F.udf(
        lambda service_date, arrival_time: gtfs_time_to_epoch(service_date, arrival_time)
        if service_date and arrival_time
        else None,
        LongType(),
    )

    # --- read bronze ---
    rt = spark.read.json(_lake(f"bronze/gtfs_realtime/dt={ds}/*/*.jsonl"))
    stop_times = spark.read.option("header", True).csv(
        _lake(f"bronze/gtfs_static/dt={ds}/stop_times.txt")
    )
    trips = spark.read.option("header", True).csv(
        _lake(f"bronze/gtfs_static/dt={ds}/trips.txt")
    )
    routes = spark.read.option("header", True).csv(
        _lake(f"bronze/gtfs_static/dt={ds}/routes.txt")
    )
    stops = spark.read.option("header", True).csv(
        _lake(f"bronze/gtfs_static/dt={ds}/stops.txt")
    )
    weather = spark.read.json(_lake(f"bronze/weather/dt={ds}/weather.jsonl"))

    # --- scheduled side: stop_times + trips + routes + stop coords ---
    scheduled = (
        stop_times.select("trip_id", "stop_id", "stop_sequence", "arrival_time")
        .withColumn(
            "scheduled_arrival_epoch",
            scheduled_epoch_udf(F.lit(ds), F.col("arrival_time")),
        )
        .join(trips.select("trip_id", "route_id", "direction_id"), "trip_id")
        .join(
            routes.select("route_id", "route_short_name", "route_long_name"),
            "route_id",
            "left",
        )
        .join(
            stops.select(
                "stop_id",
                "stop_name",
                stops["stop_lat"].cast("double").alias("stop_lat"),
                stops["stop_lon"].cast("double").alias("stop_lon"),
            ),
            "stop_id",
            "left",
        )
    )

    # --- join realtime to scheduled (referential integrity via inner join) ---
    joined = (
        rt.withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
        .withColumn("actual_arrival_epoch", F.col("actual_arrival_epoch").cast("long"))
        .join(
            scheduled.withColumn(
                "stop_sequence", F.col("stop_sequence").cast("int")
            ),
            ["trip_id", "stop_id", "stop_sequence"],
            "inner",
        )
        .withColumn(
            "delay_seconds",
            F.coalesce(
                F.col("arrival_delay").cast("int"),
                (
                    F.col("actual_arrival_epoch")
                    - F.col("scheduled_arrival_epoch")
                ).cast("int"),
            ),
        )
        .withColumn("on_time_flag", (F.col("delay_seconds") <= threshold).cast("int"))
        .withColumn("service_date", F.lit(ds))
        .withColumn(
            "arrival_hour_ts",
            F.date_format(
                F.from_unixtime("actual_arrival_epoch"), "yyyy-MM-dd'T'HH:00"
            ),
        )
    )

    # --- nearest weather station via shared haversine logic (UDF) ---
    nearest_udf = F.udf(
        lambda lat, lon: nearest_station(lat, lon, stations)
        if lat is not None and lon is not None
        else None,
        StringType(),
    )
    joined = joined.withColumn(
        "weather_station", nearest_udf(F.col("stop_lat"), F.col("stop_lon"))
    )

    # --- join hourly weather on (station, hour) ---
    weather_hourly = weather.withColumn(
        "obs_hour_ts",
        F.date_format(
            F.to_timestamp("obs_time", "yyyy-MM-dd'T'HH:mm"), "yyyy-MM-dd'T'HH:00"
        ),
    )
    silver = joined.join(
        weather_hourly,
        (joined.weather_station == weather_hourly.weather_station)
        & (joined.arrival_hour_ts == weather_hourly.obs_hour_ts),
        "left",
    ).drop(weather_hourly.weather_station)

    out = silver.select(
        "trip_id",
        "route_id",
        "route_short_name",
        "direction_id",
        "stop_id",
        "stop_name",
        "stop_sequence",
        "actual_arrival_epoch",
        "delay_seconds",
        "on_time_flag",
        "service_date",
        "arrival_hour_ts",
        "weather_station",
        "temperature_c",
        "precipitation_mm",
        "wind_speed_kmh",
        "weather_code",
    )

    out.write.mode("overwrite").parquet(
        _lake(f"{SILVER_PREFIX}/stop_performance/dt={ds}")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="service date YYYY-MM-DD")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("njt-build-silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        build(spark, args.ds)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()