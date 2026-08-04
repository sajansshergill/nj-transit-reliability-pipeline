"""Pure, dependency-light transform logic.

Everything here is a plain function with no I/O so it can be unit-tested
offline and reused from both the Spark job and the ingestion code. This is
the single source of truth for the business rules (delay, on-time,
nearest-station), which keeps Spark and the tests from drifting apart.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Tuple


def parse_gtfs_time(hhmmss: str) -> int:
    """Return seconds-since-service-midnight for a GTFS time string.

    GTFS allows times past 24:00:00 for trips that run after midnight
    (e.g. "25:30:00" == 1:30 AM the next day), so this cannot use
    datetime.strptime.
    """
    parts = hhmmss.strip().split(":")
    if len(parts) != 3:
        raise ValueError(f"invalid GTFS time: {hhmmss!r}")
    h, m, s = (int(p) for p in parts)
    if m > 59 or s > 59 or h < 0 or m < 0 or s < 0:
        raise ValueError(f"invalid GTFS time components: {hhmmss!r}")
    return h * 3600 + m * 60 + s


def gtfs_time_to_epoch(service_date: str, hhmmss: str) -> int:
    """Convert a service date ("YYYY-MM-DD") + GTFS time to a UTC epoch.

    Times are treated as UTC for simplicity; a production build would apply
    the agency timezone (America/New_York) with DST handling.
    """
    base = datetime.strptime(service_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    offset = timedelta(seconds=parse_gtfs_time(hhmmss))
    return int((base + offset).timestamp())


def compute_delay_seconds(
    scheduled_epoch: Optional[int],
    actual_epoch: Optional[int],
    provided_delay: Optional[int],
) -> Optional[int]:
    """Prefer the delay the feed provides; otherwise derive it.

    GTFS-Realtime usually carries an explicit `arrival.delay`. When it does
    not, fall back to actual minus scheduled. Returns None if neither path
    is possible.
    """
    if provided_delay is not None:
        return int(provided_delay)
    if scheduled_epoch is not None and actual_epoch is not None:
        return int(actual_epoch - scheduled_epoch)
    return None


def is_on_time(delay_seconds: Optional[int], threshold_seconds: int) -> Optional[bool]:
    """A stop is on time if it is not later than `threshold_seconds`.

    Early arrivals (negative delay) count as on time. Returns None when the
    delay is unknown, so unknowns are excluded from metrics rather than
    silently counted as on time.
    """
    if delay_seconds is None:
        return None
    return delay_seconds <= threshold_seconds


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in kilometres."""
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def nearest_station(
    lat: float,
    lon: float,
    stations: Iterable[Tuple[str, float, float]],
) -> Optional[str]:
    """Return the name of the closest weather station to (lat, lon)."""
    best_name, best_dist = None, float("inf")
    for name, s_lat, s_lon in stations:
        d = haversine_km(lat, lon, s_lat, s_lon)
        if d < best_dist:
            best_name, best_dist = name, d
    return best_name