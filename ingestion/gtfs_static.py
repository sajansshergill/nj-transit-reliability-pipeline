"""Ingest the static GTFS schedule zip and land the files we use in bronze."""
from __future__ import annotations

import io
import zipfile

import requests

from common.config import settings
from common.constants import GTFS_STATIC_FILES
from ingestion.storage import bronze_key
from ingestion.storage import _client  # single S3 client factory


def ingest(ds: str) -> list[str]:
    if not settings.gtfs_static_url:
        raise RuntimeError("NJT_GTFS_STATIC_URL is not configured")
    headers = {}
    if settings.njt_api_key:
        headers["Authorization"] = settings.njt_api_key

    resp = requests.get(settings.gtfs_static_url, headers=headers, timeout=120)
    resp.raise_for_status()

    written = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        available = set(zf.namelist())
        for fname in GTFS_STATIC_FILES:
            if fname not in available:
                raise RuntimeError(f"static GTFS missing expected file: {fname}")
            key = bronze_key("gtfs_static", ds, fname)
            _client().put_object(
                Bucket=settings.s3_bucket, Key=key, Body=zf.read(fname)
            )
            written.append(key)
    return written