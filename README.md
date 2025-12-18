# Reddit & Hacker News ETL Pipeline

Production-grade ETL pipeline for data engineering practice.  
Extracts data from public REST APIs, processes it through staging layers, and loads it into a PostgreSQL analytical store.

---

## Architecture

```
API (REST) → RAW (JSON) → STAGING (normalized) → MART (PostgreSQL)
```

The pipeline follows **medallion architecture** principles and is designed with:
- idempotent processing
- clear data lineage
- incremental extensibility

---

## Stack

- **Runtime**: Python 3.12
- **Database**: PostgreSQL 16
- **Infrastructure**: Docker, Docker Compose v2
- **Libraries**: `requests`, `pandas`, `sqlalchemy`

---

## Quick Start

```bash
# 1. Configure environment
cp .env.example .env

# 2. Start infrastructure
docker compose up -d --build

# 3. Verify database connectivity
docker exec -it reddit_hn_etl-postgres-1 \
  psql -U de -d de -c "SELECT version();"

# 4. Stop services
docker compose down
```

---

## Project Structure

```
├── data/
│   ├── mart/          # Analytics-ready tables
│   ├── raw/           # Immutable source data (JSON)
│   └── staging/       # Intermediate processed data
├── logs/              # Execution logs
└── src/
    ├── extract/       # API ingestion modules
    ├── load/          # Database write operations
    └── transform/     # Data normalization logic
```

---

## Implementation Status

**Phase 1: Infrastructure** 
- Docker containerization
- PostgreSQL instance
- Volume mounts for data persistence
- Environment variable injection
- Isolated Docker network

**Phase 2: Extract** 
- Hacker News API ingestion
- Immutable RAW JSON persistence
- Robust handling of empty / null API responses
- Logging to logs/
- Airflow-ready entrypoint

**Phase 3: Transform** (planned) 
**Phase 4: Load** (planned) 
**Phase 5: Orchestration** (Airflow)

---

## Development

### Prerequisites
- Docker Engine 20.10+
- Docker Compose v2+

### Environment Variables (`.env`)
```
POSTGRES_USER=de
POSTGRES_PASSWORD=de
POSTGRES_DB=de
POSTGRES_PORT=5432
```

### Container Names
- `reddit_hn_etl-app-1` — Python runtime
- `reddit_hn_etl-postgres-1` — PostgreSQL database

---

## Notes

- All generated data and logs are excluded from version control
- The pipeline is developed incrementally with strict separation of concerns
- Code structure is prepared for Airflow DAG integration
- SQL migrations and schema versioning will be introduced in the Load phase

