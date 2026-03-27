# Veredas

Open-source data intelligence engine. Build domain-specific AI agents for economic and financial data analysis — configurable per domain, with bilingual support (EN/PT), tiered query routing, and full pipeline observability.

## How It Works

Each **domain** is a YAML configuration file that defines data sources, AI prompts, query routing, series metadata, and landing page content. The platform reads the active domain config at startup and serves a fully customized experience — no code changes needed.

```
config/domains/
├── br_macro.yaml     # Brazilian macroeconomic data (default)
├── test_demo.yaml    # Test domain for white-label validation
└── README.md         # How to create a new domain
```

**Example:** The `br_macro` domain tracks SELIC, IPCA, USD/BRL, unemployment, GDP, and treasury yields from BCB, IBGE, and Tesouro Nacional. A different domain could track entirely different indicators from different sources.

## Architecture

```
GitHub Actions (cron)
        |
        v
 +---------------+     +-----------------+     +-------------+
 | IngestionTask | --> | QualityTask     | --> | Transform.  |
 | (per domain   |     | (post-ingest)   |     | Task        |
 |  feed configs)|     +-----------------+     +------+------+
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
| Config | YAML domain configs with Pydantic v2 validation |

## Domain Configuration

Each domain YAML defines:

| Section | What it configures |
|---|---|
| `domain` | Country, currency, languages, timezone |
| `ai` | Analyst role, safety message, anomaly context (bilingual) |
| `data_sources` | External APIs the pipeline ingests from |
| `router` | Regex patterns for direct-lookup query routing |
| `series` | Tracked data series with labels, units, colors, keywords |
| `app` | Title, cookie name, GitHub URL |
| `landing` | Hero text, feature cards (bilingual) |

See [`config/domains/README.md`](config/domains/README.md) for how to create a new domain.

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
# Edit .env.local — set STORAGE_BACKEND=local, DOMAIN_ID=br_macro

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
