import io
import logging
import os
import uuid
from pathlib import Path

import pandas as pd
import psycopg2


def setup_logger() -> logging.Logger:
    Path("logs").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("phase4_load")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh = logging.FileHandler("logs/phase4_load.log")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


def read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def pick_latest_parquet(dir_path: Path) -> Path:
    files = sorted(dir_path.glob("hn_staging_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No staging parquet files found in {dir_path}")
    return files[-1]


def main() -> int:
    logger = setup_logger()

    pg = dict(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "hn"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )

    staging_dir = Path("data/staging/hn")
    run_id = uuid.uuid4()
    source_file: Path | None = None

    logger.info("Phase 4 v1.1 starting. run_id=%s", run_id)

    conn = psycopg2.connect(**pg)
    conn.autocommit = False

    try:
        source_file = pick_latest_parquet(staging_dir)
        logger.info("Source parquet: %s", source_file.as_posix())

        # 1) Ensure audit exists + write RUNNING (commit immediately)
        with conn.cursor() as cur:
            cur.execute(read_sql("sql/load/01_audit.sql"))
            cur.execute(
                """
                INSERT INTO audit.load_runs (run_id, phase, source_file, status)
                VALUES (%s, 'phase4', %s, 'running');
                """,
                (str(run_id), source_file.as_posix()),
            )
        conn.commit()

        # 2) Work transaction: tmp -> copy -> merge -> success
        try:
            with conn.cursor() as cur:
                cur.execute(read_sql("sql/load/02_tmp.sql"))

                df = pd.read_parquet(source_file)
                cols = [
                    "id", "type", "by", "time", "time_utc", "title", "url",
                    "score", "descendants", "kids_count", "text", "extracted_at"
                ]
                df = df[cols]

                buf = io.StringIO()
                df.to_csv(buf, index=False, na_rep="\\N")
                buf.seek(0)

                cur.copy_expert(
                    """
                    COPY staging.hn_stories_tmp (
                      id, type, by, time, time_utc, title, url,
                      score, descendants, kids_count, text, extracted_at
                    )
                    FROM STDIN WITH (FORMAT csv, HEADER true, NULL '\\N');
                    """,
                    buf,
                )

                cur.execute("SELECT COUNT(*) FROM staging.hn_stories_tmp;")
                rows_copied = int(cur.fetchone()[0])
                if rows_copied == 0:
                    raise RuntimeError("COPY loaded 0 rows into staging.hn_stories_tmp")

                cur.execute(read_sql("sql/load/03_merge.sql"))
                inserted, updated = cur.fetchone()
                inserted, updated = int(inserted), int(updated)

                cur.execute(
                    """
                    UPDATE audit.load_runs
                    SET finished_at = now(),
                        status = 'success',
                        rows_copied = %s,
                        rows_merged_inserted = %s,
                        rows_merged_updated = %s
                    WHERE run_id = %s;
                    """,
                    (rows_copied, inserted, updated, str(run_id)),
                )

                # (Опционально) если хочешь tmp пустую после — раскомментируй:
                # cur.execute("TRUNCATE TABLE staging.hn_stories_tmp;")

            conn.commit()
            logger.info(
                "Phase 4 v1.1 complete. copied=%s inserted=%s updated=%s",
                rows_copied, inserted, updated
            )
            return 0

        except Exception:
            conn.rollback()
            raise

    except Exception as e:
        # Write FAILED (robustly)
        try:
            with conn.cursor() as cur:
                cur.execute(read_sql("sql/load/01_audit.sql"))

                cur.execute(
                    """
                    UPDATE audit.load_runs
                    SET finished_at = now(),
                        status = 'failed',
                        error_message = %s
                    WHERE run_id = %s;
                    """,
                    (str(e)[:4000], str(run_id)),
                )

                if cur.rowcount == 0:
                    cur.execute(
                        """
                        INSERT INTO audit.load_runs (run_id, phase, source_file, status, finished_at, error_message)
                        VALUES (%s, 'phase4', %s, 'failed', now(), %s);
                        """,
                        (str(run_id), source_file.as_posix() if source_file else None, str(e)[:4000]),
                    )

            conn.commit()
        except Exception as audit_err:
            conn.rollback()
            logger.error("Failed to write audit failed record: %s", audit_err)

        logger.exception("Phase 4 v1.1 failed: %s", e)
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
