# Hacker News ETL (Portfolio)

End-to-end ETL pipeline for Hacker News data following a simple and reproducible architecture:

RAW (JSON) → STAGING (Parquet) → PostgreSQL (staging schema)

The project is built as a data engineering portfolio and focuses on:
- clear phase separation
- deterministic processing
- typed staging layer
- idempotent data loads

---

## Phases

### Phase 2 (Extract)
Hacker News API → RAW JSON

Output:
```
data/raw/hn/hn_raw_YYYYMMDD_HHMMSS.json
```

### Phase 3 (Transform)
RAW JSON → typed STAGING Parquet

Output:
```
data/staging/hn/hn_staging_YYYYMMDD_HHMMSS.parquet
```

### Phase 4 (Load)
STAGING Parquet → PostgreSQL

Target table:
```
staging.hn_stories
```

Load is idempotent.

---

## Why Phase 3 and Phase 4 are committed together

Phase 3 defines the STAGING contract (typed Parquet with fixed schema).  
Phase 4 proves that this contract works downstream by loading it into PostgreSQL
in an idempotent way.

Together they form one end-to-end deliverable:
a reproducible staging layer validated by a real load step.

---

## Requirements

- Docker
- Docker Compose

---

## Run with Docker (recommended)

```bash
docker compose down -v
docker compose up -d postgres
docker compose up --build app
```

Expected output on first run:
```
Phase 2: Extract ... fetched records
Phase 3: Transform ... STAGING parquet saved
Phase 4: Load ... Inserted > 0, Skipped = 0
ETL pipeline finished
```

---

## Idempotency check

Re-run the pipeline:

```bash
docker compose up app
```

Expected result:
- Inserted = 0 (or a very small number)
- Skipped = N

Idempotency is enforced by:
- PRIMARY KEY (id)
- ON CONFLICT DO NOTHING

---

## Database validation

```bash
docker compose exec postgres psql -U de -d de -c \
  "SELECT COUNT(*) FROM staging.hn_stories;"
```

```bash
docker compose exec postgres psql -U de -d de -c \
  "SELECT COUNT(*) FROM (
     SELECT id FROM staging.hn_stories
     GROUP BY id HAVING COUNT(*) > 1
   ) d;"
```

---

## Project structure

```
reddit_hn_etl/
├── src/
│   ├── common/              # Shared utilities
│   ├── extract/             # Phase 2: API → RAW
│   ├── transform/           # Phase 3: RAW → STAGING
│   ├── load/                # Phase 4: STAGING → DB
│   └── pipeline.py          # Phase orchestration
├── data/
│   ├── raw/hn/              # RAW JSON files
│   └── staging/hn/          # STAGING Parquet files
├── docs/                    # Development workflow logs
├── logs/                    # Structured logs
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## STAGING table

Table: staging.hn_stories

| Column       | Type        |
|--------------|-------------|
| id           | BIGINT (PK) |
| type         | TEXT        |
| by           | TEXT        |
| time         | BIGINT      |
| time_utc     | TIMESTAMPTZ |
| title        | TEXT        |
| url          | TEXT        |
| score        | BIGINT      |
| descendants  | BIGINT      |
| kids_count   | BIGINT      |
| text         | TEXT        |
| extracted_at | TIMESTAMPTZ |

---

## Notes

- STAGING uses Parquet to preserve data types
- Files are selected deterministically by filename timestamp
- The pipeline is safe to re-run multiple times

---

## Development Workflow

**Note:** Command history tracking was implemented partway through development as a process improvement.

Command history is now tracked with timestamps for reproducibility and debugging.

To save today's work log:
```bash
save-history
```

This creates a dated log file: `docs/history_YYYY-MM-DD.txt`

See [`docs/README.md`](docs/README.md) for details on the logging approach.

Files are excluded from version control but can be shared for reproducibility audits.

---

## Roadmap

- Phase 5: Analytics MART (views)
- Scheduling (Airflow)
- Data quality checks
- BI / visualization layer
