import pytest

pytest.importorskip("great_expectations")

import pandas as pd  # noqa: E402

from quality.validate import (  # noqa: E402
    silver_expectations,
    validate_dataframe,
)


def test_silver_suite_passes_on_clean_data():
    df = pd.DataFrame(
        {
            "trip_id": ["A", "B"],
            "stop_id": ["NP", "NB"],
            "service_date": ["2026-08-02", "2026-08-02"],
            "route_id": ["NEC", "NEC"],
            "delay_seconds": [120, 540],
            "on_time_flag": [1, 0],
        }
    )
    result = validate_dataframe(df, "silver_ok", silver_expectations)
    assert result.success, result.summary()


def test_silver_suite_flags_bad_flag_value():
    df = pd.DataFrame(
        {
            "trip_id": ["A"],
            "stop_id": ["NP"],
            "service_date": ["2026-08-02"],
            "route_id": ["NEC"],
            "delay_seconds": [120],
            "on_time_flag": [9],  # invalid
        }
    )
    result = validate_dataframe(df, "silver_bad", silver_expectations)
    assert not result.success