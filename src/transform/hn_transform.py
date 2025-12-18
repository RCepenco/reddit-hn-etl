import glob
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

RAW_DIR = "data/raw/hn"
STAGING_DIR = "data/staging/hn"
LOG_DIR = "logs"


def log_line(message: str) -> None:
    """
    Minimal logger for Transform phase.
    Writes to stdout and to logs/hn_transform.log
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    line = f"{datetime.utcnow().isoformat()}Z | {message}"
    print(line)
    with open(os.path.join(LOG_DIR, "hn_transform.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")


def find_latest_raw_file(raw_dir: str = RAW_DIR) -> Optional[str]:
    """
    Find latest RAW file by filename timestamp.
    Files look like: hn_raw_YYYYMMDD_HHMMSS.json
    Sorting by filename works because timestamp is lexicographically sortable.
    """
    pattern = os.path.join(raw_dir, "hn_raw_*.json")
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def parse_extracted_at_from_filename(path: str) -> str:
    """
    Extract extracted_at from RAW filename: hn_raw_YYYYMMDD_HHMMSS.json
    Returns ISO UTC string.
    """
    name = os.path.basename(path)  # hn_raw_20251217_092720.json
    stem = name.replace(".json", "")
    # expected: ["hn", "raw", "YYYYMMDD", "HHMMSS"]
    parts = stem.split("_")
    if len(parts) < 4:
        return ""

    ts_compact = parts[-2] + parts[-1]  # YYYYMMDD + HHMMSS
    try:
        dt = datetime.strptime(ts_compact, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


def load_raw_records(path: str) -> List[Dict[str, Any]]:
    """Load RAW JSON list from disk and filter out empty/null items."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"RAW file is not a list: {path}")

    return [item for item in data if item]


def unix_to_utc_str(unix_ts: Any) -> str:
    """Convert unix timestamp to ISO UTC string; return empty string if missing."""
    try:
        ts_int = int(unix_ts)
        dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


def normalize_records(records: List[Dict[str, Any]], extracted_at: str) -> pd.DataFrame:
    """
    Convert list of JSON dicts to normalized staging DataFrame with fixed columns.
    Adds derived fields: kids_count, time_utc, extracted_at.
    """
    df = pd.DataFrame(records)

    # kids_count from kids list
    if "kids" in df.columns:
        df["kids_count"] = df["kids"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    else:
        df["kids_count"] = 0

    # time_utc derived
    if "time" in df.columns:
        df["time_utc"] = df["time"].apply(unix_to_utc_str)
    else:
        df["time"] = pd.NA
        df["time_utc"] = ""

    # extracted_at from RAW filename
    df["extracted_at"] = extracted_at

    wanted_cols = [
        "id", "type", "by", "time", "time_utc",
        "title", "url", "score", "descendants", "kids_count", "text",
        "extracted_at",
    ]

    # create missing columns
    for c in wanted_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[wanted_cols]

    # type casting (keep simple and safe)
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["time"] = pd.to_numeric(df["time"], errors="coerce").astype("Int64")
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype("Int64")
    df["descendants"] = pd.to_numeric(df["descendants"], errors="coerce").fillna(0).astype("Int64")
    df["kids_count"] = pd.to_numeric(df["kids_count"], errors="coerce").fillna(0).astype("Int64")

    return df


def dq_checks(df: pd.DataFrame) -> None:
    """
    Minimal data quality checks (fail-fast).
    """
    if df.empty:
        raise ValueError("STAGING dataframe is empty")

    if df["id"].isna().all():
        raise ValueError("All IDs are null -> RAW input likely broken")


def save_staging_csv(df: pd.DataFrame, staging_dir: str = STAGING_DIR) -> str:
    """Save staging dataset as timestamped CSV."""
    os.makedirs(staging_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(staging_dir, f"hn_staging_{ts}.csv")
    df.to_csv(path, index=False)
    return path


def run() -> None:
    """
    Airflow-ready entrypoint for Transform.
    Reads latest RAW -> produces STAGING CSV.
    """
    log_line("HN Transform started")

    latest = find_latest_raw_file()
    if not latest:
        log_line("No RAW files found, nothing to transform")
        return

    extracted_at = parse_extracted_at_from_filename(latest)
    log_line(f"Using RAW file: {latest}")
    log_line(f"extracted_at={extracted_at}")

    records = load_raw_records(latest)
    log_line(f"Loaded RAW records: {len(records)}")

    df = normalize_records(records, extracted_at=extracted_at)
    log_line(f"Normalized rows: {len(df)}")

    before = len(df)
    df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)
    log_line(f"Deduplicated: {before} -> {len(df)} rows")

    dq_checks(df)
    log_line("DQ checks passed")

    out = save_staging_csv(df)
    log_line(f"Saved STAGING to {out}")


if __name__ == "__main__":
    run()
