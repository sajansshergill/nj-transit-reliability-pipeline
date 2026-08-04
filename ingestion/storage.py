"""Thin data-lake I/O layer over S3 (or any S3-compatible store like MinIO).

Keeping every S3 call behind these helpers means the rest of the codebase
never touches boto3 directly, and the bronze/silver path conventions live
in exactly one place.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Iterable

import boto3

from common.config import settings
from common.constants import BRONZE_PREFIX, QUARANTINE_PREFIX


def _client():
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        endpoint_url=settings.s3_endpoint_url,  # None -> real AWS
    )


def bronze_key(dataset: str, ds: str, filename: str, hour: str | None = None) -> str:
    parts = [BRONZE_PREFIX, dataset, f"dt={ds}"]
    if hour is not None:
        parts.append(f"hour={hour}")
    parts.append(filename)
    return "/".join(parts)


def write_ndjson(key: str, rows: Iterable[dict]) -> str:
    """Write an iterable of dicts as newline-delimited JSON to the lake."""
    body = "\n".join(json.dumps(r, default=str) for r in rows).encode("utf-8")
    _client().put_object(Bucket=settings.s3_bucket, Key=key, Body=body)
    return f"s3://{settings.s3_bucket}/{key}"


def upload_file(local_path: str, key: str) -> str:
    _client().upload_file(local_path, settings.s3_bucket, key)
    return f"s3://{settings.s3_bucket}/{key}"


def quarantine(source_key: str, reason: str) -> str:
    """Copy an offending object into the quarantine prefix with a reason tag."""
    dest_key = f"{QUARANTINE_PREFIX}/{source_key}"
    c = _client()
    c.copy_object(
        Bucket=settings.s3_bucket,
        CopySource={"Bucket": settings.s3_bucket, "Key": source_key},
        Key=dest_key,
        Metadata={"quarantine_reason": reason[:1024]},
        MetadataDirective="REPLACE",
    )
    return f"s3://{settings.s3_bucket}/{dest_key}"


def today_ds() -> str:
    return date.today().isoformat()


def read_ndjson_df(key: str):
    """Read a newline-delimited JSON object from the lake into a DataFrame."""
    import pandas as pd  # local import keeps pandas out of prod ingest path

    obj = _client().get_object(Bucket=settings.s3_bucket, Key=key)
    lines = obj["Body"].read().decode("utf-8").splitlines()
    records = [json.loads(line) for line in lines if line.strip()]
    return pd.DataFrame.from_records(records)