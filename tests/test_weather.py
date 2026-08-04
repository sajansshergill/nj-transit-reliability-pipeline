from ingestion.weather import parse_open_meteo_hourly

SAMPLE = {
    "hourly": {
        "time": ["2026-08-02T14:00", "2026-08-02T15:00"],
        "temperature_2m": [27.5, 29.0],
        "precipitation": [0.0, 3.2],
        "wind_speed_10m": [11.0, 14.0],
        "weather_code": [1, 61],
    }
}


def test_parse_open_meteo_hourly_row_count():
    rows = parse_open_meteo_hourly("Newark Penn", SAMPLE)
    assert len(rows) == 2


def test_parse_open_meteo_hourly_fields():
    rows = parse_open_meteo_hourly("Newark Penn", SAMPLE)
    r = rows[1]
    assert r["weather_station"] == "Newark Penn"
    assert r["obs_time"] == "2026-08-02T15:00"
    assert r["temperature_c"] == 29.0
    assert r["precipitation_mm"] == 3.2
    assert r["weather_code"] == 61


def test_parse_open_meteo_empty_payload():
    assert parse_open_meteo_hourly("Newark Penn", {}) == []