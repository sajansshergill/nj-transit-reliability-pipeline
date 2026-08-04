"""Central configuration, driven entirely by environment variables.

Nothing here reads a secret at import time beyond os.environ, so the module
is safe to import in unit tests. Point S3_ENDPOINT_URL at a MinIO instance
to run the lake locally.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # Data lake
    s3_bucket: str = _get("S3_BUCKET", "njt-reliability-dev")
    s3_endpoint_url: str | None = _get("S3_ENDPOINT_URL")  # set for MinIO
    aws_region: str = _get("AWS_REGION", "us-east-1")

    # Sources
    gtfs_static_url: str | None = _get("NJT_GTFS_STATIC_URL")
    gtfs_rt_trip_updates_url: str | None = _get("NJT_GTFS_RT_TRIP_UPDATES_URL")
    njt_api_key: str | None = _get("NJT_API_KEY")

    # Weather (Open-Meteo, no key required)
    weather_base_url: str = _get(
        "WEATHER_BASE_URL", "https://api.open-meteo.com/v1/forecast"
    )

    # Warehouse (Snowflake, prod target)
    sf_account: str | None = _get("SNOWFLAKE_ACCOUNT")
    sf_user: str | None = _get("SNOWFLAKE_USER")
    sf_password: str | None = _get("SNOWFLAKE_PASSWORD")
    sf_database: str = _get("SNOWFLAKE_DATABASE", "NJT_RELIABILITY")
    sf_schema: str = _get("SNOWFLAKE_SCHEMA", "RAW")
    sf_warehouse: str = _get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    sf_role: str | None = _get("SNOWFLAKE_ROLE")
    sf_stage: str = _get("SNOWFLAKE_STAGE", "NJT_SILVER_STAGE")

    # Business rule: a rail stop is "on time" if it arrives within N seconds.
    on_time_threshold_seconds: int = int(_get("ON_TIME_THRESHOLD_SECONDS", "300"))


settings = Settings()