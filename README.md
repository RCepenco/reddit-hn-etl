# Hacker News ETL Pipeline

![Python](https://img.shields.io/badge/python-3.12-blue)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue)
![Docker](https://img.shields.io/badge/docker--compose-v5.0-blue)

End-to-end ETL pipeline for Hacker News data following medallion architecture principles:

**RAW (JSON) → STAGING (Parquet) → PostgreSQL → MART (Analytics)**

The project is built as a data engineering portfolio and focuses on:
- Clear phase separation (Extract, Transform, Load, MART)
- Deterministic processing with idempotent loads
- Typed staging layer using Parquet
- Production-ready PostgreSQL schema design
- Full refresh analytics layer

---

## Quick Start

Run the complete pipeline:

```bash
# Clean start
docker compose down -v

# Start PostgreSQL
docker compose up -d postgres

# Run ETL (Phases 2-4: Extract → Transform → Load)
docker compose up --build app

# Build MART layer (Phase 5: Analytics)
docker compose run --rm mart
```

Verify results:

```bash
# Check staging data
docker compose exec postgres psql -U de -d de -c \
  "SELECT COUNT(*) FROM staging.hn_stories;"

# Check MART metrics
docker compose exec postgres psql -U de -d de -c \
  "SELECT metric_date, stories_count, avg_score FROM mart.daily_story_metrics ORDER BY metric_date DESC LIMIT 5;"
```

---

## Architecture

### ETL Phases

#### Phase 2: Extract
Hacker News API → RAW JSON

Output:
```
data/raw/hn/hn_raw_YYYYMMDD_HHMMSS.json
```

#### Phase 3: Transform
RAW JSON → typed STAGING Parquet

Output:
```
data/staging/hn/hn_staging_YYYYMMDD_HHMMSS.parquet
```

#### Phase 4: Load
STAGING Parquet → PostgreSQL

Target table: `staging.hn_stories`

Load is idempotent using `ON CONFLICT DO NOTHING`.

---

### Phase 5: Analytics MART

**STAGING (PostgreSQL) → MART (PostgreSQL)**

The MART layer provides read-optimized analytical tables for BI-style queries.
It is rebuilt using a **full refresh strategy** and is fully idempotent.

- Source: `staging.hn_stories`
- Target schema: `mart`
- Execution: SQL-only transformations
- Orchestration: Docker Compose

---

## MART Tables

### `mart.daily_story_metrics`
Daily aggregated metrics for Hacker News stories.

| Column | Description |
|--------|-------------|
| `metric_date` | Story creation date (UTC) |
| `stories_count` | Number of stories |
| `total_score` | Sum of scores |
| `avg_score` | Average score |
| `total_comments` | Sum of comments |
| `avg_comments` | Average comments |
| `last_batch_extracted_at` | Latest batch timestamp |

### `mart.top_domains_daily`
Top domains per day.

| Column | Description |
|--------|-------------|
| `metric_date` | Story creation date |
| `domain` | Normalized domain |
| `stories_count` | Stories per domain |
| `avg_score` | Average score |
| `last_batch_extracted_at` | Latest batch timestamp |

### `mart.user_activity_daily`
Daily user activity metrics.

| Column | Description |
|--------|-------------|
| `metric_date` | Story creation date |
| `author` | Story author |
| `stories_count` | Stories posted |
| `avg_score` | Average score |
| `last_batch_extracted_at` | Latest batch timestamp |

---

## STAGING Schema

Table: `staging.hn_stories`

| Column       | Type        | Description |
|--------------|-------------|-------------|
| id           | BIGINT (PK) | Story ID |
| type         | TEXT        | Item type |
| by           | TEXT        | Author username |
| time         | BIGINT      | Unix timestamp |
| time_utc     | TIMESTAMPTZ | UTC timestamp |
| title        | TEXT        | Story title |
| url          | TEXT        | Story URL |
| score        | BIGINT      | Story score |
| descendants  | BIGINT      | Comment count |
| kids_count   | BIGINT      | Direct replies |
| text         | TEXT        | Story text |
| extracted_at | TIMESTAMPTZ | Extraction timestamp |

---

## Running the Pipeline

### Full ETL (Phases 2-4)

```bash
docker compose up --build app
```

Expected output on first run:

```
Phase 2: Extract ... fetched records
Phase 3: Transform ... STAGING parquet saved
Phase 4: Load ... Inserted > 0, Skipped = 0
ETL pipeline finished
```

### Build MART Layer (Phase 5)

```bash
docker compose run --rm mart
```

Expected output:

```
Phase 5 starting: building MART (analytics layer) in PostgreSQL
Phase 5 complete: MART refreshed successfully (idempotent)
```

### Idempotency Check

Re-run the pipeline:

```bash
docker compose up app
```

Expected result:
- Inserted = 0 (or very small number)
- Skipped = N

Idempotency is enforced by:
- PRIMARY KEY (id)
- ON CONFLICT DO NOTHING

---

## Validation

### Check for duplicates in staging

```bash
docker compose exec postgres psql -U de -d de -c \
  "SELECT COUNT(*) FROM (
     SELECT id FROM staging.hn_stories
     GROUP BY id HAVING COUNT(*) > 1
   ) d;"
```

Expected: `0`

### Verify MART data

```bash
docker compose exec postgres psql -U de -d de -c \
  "SELECT * FROM mart.daily_story_metrics ORDER BY metric_date DESC LIMIT 3;"
```

---


## Project Structure

```
hn_etl/
├── src/
│   ├── common/              # Shared utilities
│   ├── extract/             # Phase 2: API → RAW
│   ├── transform/           # Phase 3: RAW → STAGING
│   ├── load/                # Phase 4: STAGING → DB
│   ├── mart/                # Phase 5: Analytics MART
│   └── pipeline.py          # Phase orchestration
├── data/
│   ├── raw/hn/              # RAW JSON files
│   ├── staging/hn/          # STAGING Parquet files
│   └── mart/                # MART intermediate files
├── sql/
│   ├── load/                # SQL scripts for Phase 4
│   └── mart/                # SQL scripts for Phase 5
├── docker/
│   └── Dockerfile.mart      # Dockerfile for MART runner
├── docs/                    # Development workflow logs
├── logs/                    # Structured logs
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Design Decisions

### Why PostgreSQL
PostgreSQL was chosen as the primary analytical database because it is widely used in production, provides strong SQL capabilities, supports transactional guarantees, and is sufficient for small-to-medium analytical workloads. This makes the project realistic and easy to reason about without unnecessary complexity. For larger datasets, this could be replaced with Snowflake or BigQuery without changing the SQL logic.

### Why Full Refresh MART
The MART layer is rebuilt using a full refresh strategy based on the complete STAGING history. This ensures deterministic, idempotent results and simplifies data correctness for a portfolio project. Incremental updates were intentionally deferred as a future optimization to keep the logic transparent and easy to validate.

### Why Docker Compose
Docker Compose is used to provide a reproducible, environment-agnostic setup for PostgreSQL and the MART runner. This eliminates "works on my machine" issues and mirrors real-world deployment practices, while staying lightweight enough for local development and CI/CD.

### Why Separation into Phases
The pipeline is split into clear phases (Extract, Transform, Load, MART) to enforce separation of concerns. Each phase has a single responsibility, making the system easier to test, debug, and extend (e.g., replacing Python with Airflow DAGs or SQL with dbt models).

### Why Parquet for STAGING
STAGING uses Parquet to preserve data types and provide a typed contract between Extract and Load phases. This ensures schema consistency and makes the pipeline more robust to API changes.

---

## Requirements

- Docker
- Docker Compose

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

## Notes

- Files are selected deterministically by filename timestamp
- The pipeline is safe to re-run multiple times
- All transformations are deterministic and reproducible
- MART tables include analytical indexes for time-series queries

---

## Roadmap

- [ ] Orchestration with Airflow
- [ ] Data quality checks (Great Expectations)
- [ ] Incremental MART updates
- [ ] BI visualization layer (Metabase / Superset)
- [ ] CI/CD pipeline




