import logging
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def _setup_logger() -> logging.Logger:
    Path("logs").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("phase5_mart")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        fh = logging.FileHandler("logs/phase5_mart.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    return logger


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def main() -> int:
    load_dotenv()
    logger = _setup_logger()

    db_host = os.getenv("PGHOST", "localhost")
    db_port = int(os.getenv("PGPORT", "5432"))
    db_name = os.getenv("PGDATABASE", "postgres")
    db_user = os.getenv("PGUSER", "postgres")
    db_pass = os.getenv("PGPASSWORD", "postgres")

    scripts = [
        Path("sql/mart/01_schema.sql"),
        Path("sql/mart/02_marts.sql"),
    ]

    logger.info("Phase 5 starting: building MART (analytics layer) in PostgreSQL")
    logger.info("Connection: db=%s host=%s port=%s user=%s", db_name, db_host, db_port, db_user)

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass,
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for script in scripts:
                logger.info("Running SQL: %s", script.as_posix())
                cur.execute(_read_sql(script.as_posix()))

        conn.commit()
        logger.info("Phase 5 complete: MART refreshed successfully (idempotent)")
        return 0

    except Exception as e:
        conn.rollback()
        logger.exception("Phase 5 failed, rolled back: %s", e)
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
