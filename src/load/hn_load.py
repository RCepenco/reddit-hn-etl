from __future__ import annotations

import glob
import logging
import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

STAGING_DIR = Path("data/staging/hn")

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler("logs/hn_load.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

HN_STORIES_COLS: List[str] = [
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


def get_latest_staging_file() -> Path:
    files = sorted(glob.glob(str(STAGING_DIR / "hn_staging_*.parquet")))
    if not files:
        raise FileNotFoundError("No STAGING parquet found. Run Phase 3 first.")
    return Path(files[-1])


def get_conn():
    """Creates a psycopg2 connection using env vars (works in Docker + locally)."""
    return psycopg2.connect(
        host=os.getenv("PGHOST", os.getenv("POSTGRES_HOST", "postgres")),
        port=os.getenv("PGPORT", os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("PGUSER", os.getenv("POSTGRES_USER", "postgres")),
        password=os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
        dbname=os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "postgres")),
    )


def ensure_schema() -> None:
    """
    Creates staging schema + hn_stories table (idempotent).

    PRIMARY KEY (id) is required for ON CONFLICT to work.
    Safe to run on every pipeline execution.
    """
    sql = """
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.hn_stories (
        id BIGINT PRIMARY KEY,
        type TEXT,
        by TEXT,
        time BIGINT NOT NULL,
        time_utc TIMESTAMPTZ NOT NULL,
        title TEXT NOT NULL,
        url TEXT,
        score BIGINT,
        descendants BIGINT,
        kids_count BIGINT,
        text TEXT,
        extracted_at TIMESTAMPTZ NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_hn_time ON staging.hn_stories(time);
    CREATE INDEX IF NOT EXISTS idx_hn_score ON staging.hn_stories(score);
    CREATE INDEX IF NOT EXISTS idx_hn_extracted ON staging.hn_stories(extracted_at);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info("Schema ensured")


def load_idempotent(df: pd.DataFrame) -> Tuple[int, int]:
    """
    Loads STAGING DataFrame into PostgreSQL with idempotency.

    Strategy:
    - Batch insert via execute_values
    - ON CONFLICT (id) DO NOTHING
    - RETURNING id is NOT used to avoid execute_values+page_size trap.
      We count inserted as (before_count -> after_count delta) via SQL.
    """
    values = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in df[HN_STORIES_COLS].to_numpy()
    ]

    insert_sql = f"""
        INSERT INTO staging.hn_stories ({", ".join(HN_STORIES_COLS)})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM staging.hn_stories;")
            before = cur.fetchone()[0]

            execute_values(cur, insert_sql, values, page_size=500)

            cur.execute("SELECT COUNT(*) FROM staging.hn_stories;")
            after = cur.fetchone()[0]

    inserted = after - before
    skipped = len(df) - inserted
    return inserted, skipped


def run() -> None:
    """Phase 4 entrypoint: STAGING Parquet -> PostgreSQL staging.hn_stories."""
    logger.info("=== Phase 4: Load (STAGING Parquet -> Postgres) ===")

    staging_file = get_latest_staging_file()
    logger.info(f"Reading STAGING: {staging_file}")

    df = pd.read_parquet(staging_file, engine="pyarrow")
    if df.empty:
        raise ValueError("STAGING is empty")

    ensure_schema()
    inserted, skipped = load_idempotent(df)

    logger.info(f"Inserted={inserted}, Skipped={skipped}")
    logger.info("✓ Phase 4 complete")


if __name__ == "__main__":
    run()
