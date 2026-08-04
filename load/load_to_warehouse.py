"""Load the silver parquet for a given service date into Snowflake RAW.

Uses COPY INTO from the external stage created in warehouse/ddl.sql. Called
by the Airflow `load_to_warehouse` task after silver validation passes.
"""
from __future__ import annotations

import argparse

from common.config import settings

COPY_SQL = """
COPY INTO {db}.{schema}.STOP_PERFORMANCE
FROM (
    SELECT
        $1:trip_id::string,
        $1:route_id::string,
        $1:route_short_name::string,
        $1:direction_id::string,
        $1:stop_id::string,
        $1:stop_name::string,
        $1:stop_sequence::integer,
        $1:actual_arrival_epoch::bigint,
        $1:delay_seconds::integer,
        $1:on_time_flag::integer,
        $1:service_date::date,
        $1:arrival_hour_ts::string,
        $1:weather_station::string,
        $1:temperature_c::float,
        $1:precipitation_mm::float,
        $1:wind_speed_kmh::float,
        $1:weather_code::integer,
        CURRENT_TIMESTAMP()
    FROM @{db}.{schema}.{stage}/dt={ds}/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = ABORT_STATEMENT;
"""


def load(ds: str) -> int:
    import snowflake.connector

    missing = [
        name
        for name, value in {
            "SNOWFLAKE_ACCOUNT": settings.sf_account,
            "SNOWFLAKE_USER": settings.sf_user,
            "SNOWFLAKE_PASSWORD": settings.sf_password,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"missing required Snowflake settings: {', '.join(missing)}")

    conn = snowflake.connector.connect(
        account=settings.sf_account,
        user=settings.sf_user,
        password=settings.sf_password,
        warehouse=settings.sf_warehouse,
        database=settings.sf_database,
        schema=settings.sf_schema,
        role=settings.sf_role,
    )
    try:
        cur = conn.cursor()
        # Idempotent reload: clear this date's rows first.
        cur.execute(
            f"DELETE FROM {settings.sf_database}.{settings.sf_schema}."
            f"STOP_PERFORMANCE WHERE service_date = %s",
            (ds,),
        )
        cur.execute(
            COPY_SQL.format(
                db=settings.sf_database,
                schema=settings.sf_schema,
                stage=settings.sf_stage,
                ds=ds,
            )
        )
        rowcount = cur.rowcount or 0
        conn.commit()
        return rowcount
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    args = parser.parse_args()
    loaded = load(args.ds)
    print(f"loaded rows: {loaded}")


if __name__ == "__main__":
    main()