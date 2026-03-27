# Veredas

A Brazilian macroeconomic data dashboard with natural-language query support. It fetches data from BCB, IBGE, and Tesouro Nacional, processes it through a medallion pipeline (bronze/silver/gold), and serves it via an interactive React dashboard and natural language query interface.

## Architecture

```
GitHub Actions (cron 06:00 UTC)
        |
        v
 +---------------+     +-----------------+     +-------------+
 | IngestionTask | --> | QualityTask     | --> | Transform.  |
 | (BCB, IBGE,   |     | (post-ingest)   |     | Task        |
 |  Tesouro)     |     +-----------------+     +------+------+
 +---------------+                                     |
                                                      v
                                              +------------------+
                                              | QualityTask      |
                                              | (post-transform) |
                                              +--------+---------+
                                                       |
                   +-----------------------------------+
                   v                                   v
          +----------------+                  +------------------+
          | Cloudflare R2  |  -- sync -->     | Railway (API)    |
          | bronze/silver/ |  webhook         | FastAPI + DuckDB |
          | gold Parquet   |                  +---------+--------+
          +----------------+                            |
                                                        v
                                               +-----------------+
                                               | Vercel          |
                                               | React Dashboard |
                                               +-----------------+
```

## Tech Stack

| Layer | Technology |
|---|---|
| Pipeline + API | Python 3.12, FastAPI, DuckDB, Pydantic v2, structlog |
| AI Agents | Anthropic Claude SDK (Sonnet) |
| Frontend | React 19 + TypeScript + Vite, Tailwind CSS v4, Recharts, TanStack Query |
| Storage | Cloudflare R2 (Parquet), Neon Postgres, Upstash Redis |
| Hosting | Railway (API), Vercel (frontend) |
| CI/CD | GitHub Actions |
| Security | L1 regex sanitization, L3 XML data fencing |

## Data Sources

| Series | Source | ID |
|---|---|---|
| SELIC rate | [BCB SGS](https://www3.bcb.gov.br/sgspub/) | 432 |
| IPCA inflation | [BCB SGS](https://www3.bcb.gov.br/sgspub/) | 433 |
| USD/BRL exchange | [BCB SGS](https://www3.bcb.gov.br/sgspub/) | 1 |
| Unemployment | [IBGE PNAD](https://sidra.ibge.gov.br/) | PNAD Continua |
| GDP | [IBGE](https://sidra.ibge.gov.br/) | Quarterly GDP |
| Treasury yields | [Tesouro Direto](https://www.tesourotransparente.com.br/) | Bond yields |

## Local Development

### Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Node.js 20+
- Docker (for Postgres + Redis)

### Setup

```bash
# Clone
git clone https://github.com/<your-username>/veredas.git
cd veredas

# Python dependencies
uv sync

# Start local services
docker compose up -d

# Create local env
cp .env.example .env.local
# Edit .env.local — set STORAGE_BACKEND=local

# Download test fixtures
uv run python scripts/download_fixtures.py

# Seed local data (optional)
uv run python scripts/seed_local_data.py
```

### Run

```bash
# API
uv run uvicorn api.main:app --reload --port 8000

# Frontend
cd frontend && npm install && npm run dev

# Pipeline (writes to ./data/local/)
uv run python -m pipeline.flow
```

### Test

```bash
# All checks
uv run ruff check .
uv run mypy . --ignore-missing-imports
uv run pytest tests/ -x --cov

# Frontend
cd frontend && npx tsc --noEmit && npm run build
```

## Pipeline Quality

The pipeline runs daily and publishes quality reports. View the latest at [`/api/quality/latest`](/api/quality/latest).

Quality checks include: null rates, value ranges, row counts, schema validation, duplicate detection, and data freshness monitoring.

## License

MIT
