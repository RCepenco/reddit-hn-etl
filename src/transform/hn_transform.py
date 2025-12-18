from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import pandas as pd

from src.common.files import latest_file_by_name
from src.common.logging_utils import get_logger

RAW_DIR = Path("data/raw/hn")
STAGING_DIR = Path("data/staging/hn")

logger = get_logger(__name__, "hn_transform.log")


@dataclass(frozen=True)
class HnSchema:
    required: tuple[str, ...] = ("id", "by", "time", "title", "type")


def get_latest_raw_file() -> Path:
    return latest_file_by_name(RAW_DIR, "hn_raw_*.json")


def parse_ts_from_raw_filename(raw_file: Path) -> Tuple[str, datetime]:
    """
    Example:
      hn_raw_20251218_145959.json -> ("20251218_145959", dt_utc)
    """
    ts_str = raw_file.stem.replace("hn_raw_", "")
    dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
    return ts_str, dt


def transform_raw_to_df(raw_file: Path) -> pd.DataFrame:
    """
    RAW -> typed STAGING DataFrame.

    Why:
    - stable types for downstream load (int64, timestamptz)
    - enrichment: time_utc, kids_count, extracted_at
    - dedup by business key (id)
    """
    logger.info(f"Reading RAW: {raw_file}")

    with raw_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("RAW payload must be list[dict]")

    data_clean = [x for x in data if x]
    if len(data_clean) != len(data):
        logger.warning(f"Skipped {len(data) - len(data_clean)} null records")

    df = pd.DataFrame(data_clean)

    schema = HnSchema()
    missing = [c for c in schema.required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for col, default in [
        ("url", None),
        ("text", None),
        ("kids", None),
        ("descendants", 0),
        ("score", 0),
    ]:
        if col not in df.columns:
            df[col] = default

    df["id"] = pd.to_numeric(df["id"], errors="raise").astype("int64")
    df["time"] = pd.to_numeric(df["time"], errors="raise").astype("int64")
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype("int64")
    df["descendants"] = (
        pd.to_numeric(df["descendants"], errors="coerce").fillna(0).astype("int64")
    )

    df["kids_count"] = df["kids"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    ).astype("int64")

    df["time_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)

    ts_str, extracted_at_dt = parse_ts_from_raw_filename(raw_file)
    df["extracted_at"] = pd.Timestamp(extracted_at_dt)

    out_cols = [
        "id",
        "type",
        "by",
        "time",
        "time_utc",
        "title",
        "url",
        "score",
        "descendants",
        "kids_count",
        "text",
        "extracted_at",
    ]
    df = df[out_cols].copy()

    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)
    after = len(df)

    if after == 0:
        raise ValueError("Transform result is empty (fail-fast)")
    if after != before:
        logger.warning(f"Dedup: {before} -> {after}")

    return df


def save_parquet(df: pd.DataFrame, ts_str: str) -> Path:
    """
    Writes STAGING artifact to Parquet.

    Why Parquet:
    - keeps types
    - standard DE staging format
    """
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / f"hn_staging_{ts_str}.parquet"
    df.to_parquet(out, index=False, engine="pyarrow")
    logger.info(f"Saved STAGING: {out}")
    return out


def run() -> None:
    logger.info("=== Phase 3: Transform (RAW -> STAGING Parquet) ===")

    raw_file = get_latest_raw_file()
    ts_str, _ = parse_ts_from_raw_filename(raw_file)

    df = transform_raw_to_df(raw_file)
    save_parquet(df, ts_str)

    logger.info("✓ Phase 3 complete")


if __name__ == "__main__":
    run()
