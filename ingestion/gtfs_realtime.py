"""Ingest the GTFS-Realtime TripUpdates feed.

The protobuf parsing (`parse_trip_updates`) is a pure function that takes raw
feed bytes and returns plain dict rows, so it can be unit-tested offline
against a hand-built FeedMessage. The network fetch and the lake write are
kept separate from it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import requests
from google.transit import gtfs_realtime_pb2

from common.config import settings
from ingestion.storage import bronze_key, write_ndjson


def parse_trip_updates(feed_bytes: bytes) -> List[dict]:
    """Explode a serialized FeedMessage into one row per stop-time update."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_bytes)

    rows: List[dict] = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        for stu in tu.stop_time_update:
            arrival = stu.arrival if stu.HasField("arrival") else None
            rows.append(
                {
                    "trip_id": trip.trip_id or None,
                    "route_id": trip.route_id or None,
                    "start_date": trip.start_date or None,
                    "stop_id": stu.stop_id or None,
                    "stop_sequence": stu.stop_sequence
                    if stu.HasField("stop_sequence")
                    else None,
                    "actual_arrival_epoch": arrival.time
                    if arrival is not None and arrival.time
                    else None,
                    "arrival_delay": arrival.delay
                    if arrival is not None and arrival.HasField("delay")
                    else None,
                }
            )
    return rows


def fetch_feed_bytes() -> bytes:
    if not settings.gtfs_rt_trip_updates_url:
        raise RuntimeError("NJT_GTFS_RT_TRIP_UPDATES_URL is not configured")
    headers = {}
    if settings.njt_api_key:
        headers["Authorization"] = settings.njt_api_key
    resp = requests.get(
        settings.gtfs_rt_trip_updates_url, headers=headers, timeout=30
    )
    resp.raise_for_status()
    return resp.content


def ingest(ds: str) -> str:
    """Fetch the live feed and land parsed rows in bronze. Returns the key."""
    now = datetime.now(timezone.utc)
    rows = parse_trip_updates(fetch_feed_bytes())
    for r in rows:
        r["ingest_epoch"] = int(now.timestamp())
    hour = now.strftime("%H")
    key = bronze_key(
        "gtfs_realtime", ds, f"trip_updates_{now.strftime('%H%M%S')}.jsonl", hour=hour
    )
    write_ndjson(key, rows)
    return key