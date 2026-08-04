"""Data-quality gates built on Great Expectations (pinned to the 0.18 API).

Each layer boundary has a suite. `validate_dataframe` runs a suite against a
pandas frame and returns a simple result object. The DAG uses the result to
decide whether to quarantine a batch and stop the run, so a bad feed can
never silently reach the gold marts.

Kept as plain functions (no on-disk GE project) so it is easy to read and to
exercise in tests.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List

import pandas as pd
import great_expectations as gx


@dataclass
class ValidationResult:
    success: bool
    failed_expectations: List[str]

    def summary(self) -> str:
        if self.success:
            return "all expectations passed"
        return "failed: " + "; ".join(self.failed_expectations)


def validate_dataframe(
    df: pd.DataFrame,
    suite_name: str,
    build_expectations: Callable,
) -> ValidationResult:
    context = gx.get_context()
    datasource = context.sources.add_pandas(name=f"{suite_name}_src")
    asset = datasource.add_dataframe_asset(name=f"{suite_name}_asset")
    batch_request = asset.build_batch_request(dataframe=df)
    context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=suite_name
    )

    build_expectations(validator)
    results = validator.validate()

    failed = [
        r.expectation_config.expectation_type
        for r in results.results
        if not r.success
    ]
    return ValidationResult(success=bool(results.success), failed_expectations=failed)


# --- Suites -----------------------------------------------------------------

def bronze_realtime_expectations(v) -> None:
    """Schema + freshness contract for the raw realtime feed."""
    for col in ["trip_id", "stop_id", "ingest_epoch"]:
        v.expect_column_values_to_not_be_null(col)
    v.expect_column_values_to_be_of_type("stop_sequence", "int64")
    # Freshness: the newest ingest must be recent. A feed that goes quiet is a
    # classic silent failure, so we assert it explicitly.
    now = int(datetime.now(timezone.utc).timestamp())
    v.expect_column_max_to_be_between(
        "ingest_epoch", min_value=now - 3600, max_value=now + 300
    )


def silver_expectations(v) -> None:
    """Post-join integrity checks on stop-level performance."""
    v.expect_column_values_to_not_be_null("trip_id")
    v.expect_column_values_to_not_be_null("stop_id")
    v.expect_column_values_to_not_be_null("service_date")
    # Referential integrity: every silver row joined to a scheduled trip.
    v.expect_column_values_to_not_be_null("route_id")
    # Anomaly bounds: catch clock-skew / parsing bugs (delays beyond +/- 6h).
    v.expect_column_values_to_be_between(
        "delay_seconds", min_value=-21600, max_value=21600, mostly=0.99
    )
    v.expect_column_values_to_be_in_set("on_time_flag", [0, 1])


def gold_marts_expectations(v) -> None:
    """Sanity checks on an aggregated mart before it is trusted."""
    v.expect_column_values_to_be_between("on_time_pct", min_value=0, max_value=100)
    v.expect_column_values_to_be_between("trips", min_value=1, max_value=None)