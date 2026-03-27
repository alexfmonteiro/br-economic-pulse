# Veredas — Claude Code Instructions

This is a domain-agnostic data intelligence engine. Each domain (e.g., Brazilian
macroeconomics) is defined by a YAML config in `config/domains/`. The active
domain is selected via the `DOMAIN_ID` environment variable (defaults to `br_macro`).

## Package Manager

This project uses **uv** for Python package management. All Python commands
must be prefixed with `uv run` (e.g., `uv run pytest`, `uv run ruff check .`).
Never use `pip install`. To add dependencies: `uv add <package>` or
`uv add --dev <package>`.

## Domain Configuration

- Domain configs live in `config/domains/*.yaml` (Pydantic v2 validated)
- Loader: `from config import get_domain_config` — returns cached `DomainConfig`
- Models: `config/domain.py` — `DomainConfig`, `SeriesDisplayConfig`, `AIConfig`, etc.
- To add a new domain: copy `br_macro.yaml`, update all fields, create feed YAMLs
- Backend reads all domain-specific values from config (prompts, router, series, app)
- Frontend fetches domain config via `GET /api/config/domain`

## Architectural Invariants (never violate these)

### Python Standards
- Python 3.12, strict type hints on ALL function signatures
- Pydantic v2 BaseModel with `model_config = ConfigDict(strict=True, extra="forbid")`
- All data models in `api/models.py` — never define inline Pydantic models
- structlog for all logging — never use `print()` or stdlib `logging`
- async/await for all I/O operations

### Naming Conventions
- **Task** = pure Python, no LLM (IngestionTask, TransformationTask, QualityTask)
- **Agent** = calls Claude API (InsightAgent, QueryAgent)
- **Flow** = orchestrates Tasks and Agents (PipelineFlow)
- Never blur these boundaries. A Task must never call the Anthropic API.

### Storage
- All file I/O through `StorageBackend` Protocol — never boto3/pathlib directly
- Import: `from storage import get_storage_backend`

### Security
- L1 regex sanitization + L3 XML data fencing. Never interpolate raw strings into prompts.

### Testing
- Every new function gets a test. No exceptions.
- Tests use `LocalStorageBackend` with `tmp_path` — never mock storage
- Claude API mocked with `unittest.mock.AsyncMock` — never real calls in tests
- Fixture data in `tests/fixtures/`

### Query Routing
- Simple lookups → Tier 1 regex match → DuckDB ($0). Everything else → Claude Sonnet.
- Every QueryResponse includes `tier_used` and `llm_tokens_used`

## When Adding a New Task or Agent

1. Define Pydantic model in `api/models.py` FIRST
2. Write test file FIRST (test-driven)
3. Implement the class
4. Run `uv run ruff check . && uv run mypy . --ignore-missing-imports && uv run pytest tests/ -x`
5. Commit only if all pass
