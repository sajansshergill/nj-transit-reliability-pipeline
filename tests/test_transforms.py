import pytest

from common.transforms import (
    compute_delay_seconds,
    gtfs_time_to_epoch,
    haversine_km,
    is_on_time,
    nearest_station,
    parse_gtfs_time,
)

STATIONS = [
    ("Newark Penn", 40.7357, -74.1643),
    ("Trenton", 40.2181, -74.7546),
    ("Hoboken", 40.7348, -74.0277),
]


def test_parse_gtfs_time_basic():
    assert parse_gtfs_time("00:00:00") == 0
    assert parse_gtfs_time("01:02:03") == 3723


def test_parse_gtfs_time_after_midnight():
    # GTFS allows hours >= 24 for post-midnight service.
    assert parse_gtfs_time("25:30:00") == 25 * 3600 + 30 * 60


def test_parse_gtfs_time_rejects_bad_input():
    with pytest.raises(ValueError):
        parse_gtfs_time("12:60:00")
    with pytest.raises(ValueError):
        parse_gtfs_time("12:00")


def test_gtfs_time_to_epoch_rolls_past_midnight():
    midnight = gtfs_time_to_epoch("2026-08-02", "00:00:00")
    late = gtfs_time_to_epoch("2026-08-02", "25:00:00")
    assert late - midnight == 25 * 3600


def test_compute_delay_prefers_provided():
    assert compute_delay_seconds(1000, 1300, provided_delay=240) == 240


def test_compute_delay_falls_back_to_difference():
    assert compute_delay_seconds(1000, 1300, provided_delay=None) == 300


def test_compute_delay_unknown():
    assert compute_delay_seconds(None, None, None) is None


def test_is_on_time_boundaries():
    assert is_on_time(300, 300) is True     # exactly on the threshold
    assert is_on_time(301, 300) is False
    assert is_on_time(-60, 300) is True     # early counts as on time
    assert is_on_time(None, 300) is None    # unknown stays unknown


def test_haversine_known_distance():
    # Newark Penn -> Trenton is ~76 km as the crow flies.
    d = haversine_km(40.7357, -74.1643, 40.2181, -74.7546)
    assert 73 < d < 79


def test_nearest_station_picks_closest():
    # A point right by Newark should map to Newark Penn.
    assert nearest_station(40.74, -74.17, STATIONS) == "Newark Penn"
    # A point near Trenton should map to Trenton.
    assert nearest_station(40.22, -74.75, STATIONS) == "Trenton"