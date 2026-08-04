"""Ingest hourly weather for the curated station list via Open-Meteo.

Open-Meteo needs no API key, which keeps the whole pipeline runnable by
anyone who clones the repo. `parse_open_meteo_hourly` is pure and testable.
"""
from __future__ import annotations

from typing import List

import requests

from common.config import settings
from common.constants import WEATHER_STATIONS
from ingestion.storage import bronze_key, write_ndjson


def parse_open_meteo_hourly(station: str, payload: dict) -> List[dict]:
    """Flatten an Open-Meteo hourly response into one row per hour."""
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    precip = hourly.get("precipitation", [])
    wind = hourly.get("wind_speed_10m", [])
    codes = hourly.get("weather_code", [])

    rows: List[dict] = []
    for i, t in enumerate(times):
        rows.append(
            {
                "weather_station": station,
                "obs_time": t,  # ISO "YYYY-MM-DDTHH:MM"
                "temperature_c": temps[i] if i < len(temps) else None,
                "precipitation_mm": precip[i] if i < len(precip) else None,
                "wind_speed_kmh": wind[i] if i < len(wind) else None,
                "weather_code": codes[i] if i < len(codes) else None,
            }
        )
    return rows


def fetch_station(lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,weather_code",
        "timezone": "UTC",
        "past_days": 1,
        "forecast_days": 1,
    }
    resp = requests.get(settings.weather_base_url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest(ds: str) -> str:
    all_rows: List[dict] = []
    for name, (lat, lon) in WEATHER_STATIONS.items():
        all_rows.extend(parse_open_meteo_hourly(name, fetch_station(lat, lon)))
    key = bronze_key("weather", ds, "weather.jsonl")
    write_ndjson(key, all_rows)
    return key