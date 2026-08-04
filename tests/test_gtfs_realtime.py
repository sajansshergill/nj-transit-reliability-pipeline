from google.transit import gtfs_realtime_pb2

from ingestion.gtfs_realtime import parse_trip_updates


def _build_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"

    entity = feed.entity.add()
    entity.id = "1"
    tu = entity.trip_update
    tu.trip.trip_id = "NEC_101"
    tu.trip.route_id = "NEC"

    stu = tu.stop_time_update.add()
    stu.stop_id = "NP"
    stu.stop_sequence = 3
    stu.arrival.time = 1754130600
    stu.arrival.delay = 120

    # A second stop with no arrival info (should still parse, nulls allowed).
    stu2 = tu.stop_time_update.add()
    stu2.stop_id = "NB"
    stu2.stop_sequence = 7

    return feed.SerializeToString()


def test_parse_trip_updates_explodes_stops():
    rows = parse_trip_updates(_build_feed())
    assert len(rows) == 2

    first = rows[0]
    assert first["trip_id"] == "NEC_101"
    assert first["route_id"] == "NEC"
    assert first["stop_id"] == "NP"
    assert first["stop_sequence"] == 3
    assert first["arrival_delay"] == 120
    assert first["actual_arrival_epoch"] == 1754130600


def test_parse_trip_updates_handles_missing_arrival():
    rows = parse_trip_updates(_build_feed())
    second = rows[1]
    assert second["stop_id"] == "NB"
    assert second["arrival_delay"] is None
    assert second["actual_arrival_epoch"] is None


def test_parse_ignores_non_trip_entities():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    ent = feed.entity.add()
    ent.id = "alert-1"
    ent.alert.header_text.translation.add().text = "delays expected"
    assert parse_trip_updates(feed.SerializeToString()) == []