# Contributing to Veredas

## Philosophy

This project values production discipline: every change must be tested, typed, linted, and secure. We ship small, well-documented increments rather than large, untested batches.

## How to Propose Changes

### New Data Series

1. Create a feed config YAML in `data/schemas/feeds/` following the existing patterns
2. Add the series ID to `SERIES` in `frontend/src/lib/api.ts`
3. Add fixture data in `tests/fixtures/`
4. Write tests for ingestion and transformation
5. Open a PR with a description of the data source and why it's valuable

### New Task (pure Python, no LLM)

1. Define any new Pydantic models in `api/models.py` first
2. Write the test file in `tests/unit/` first (test-driven)
3. Implement in `tasks/<name>/task.py` extending `BaseTask`
4. A Task must never call the Anthropic API

### New Agent (LLM-powered)

1. Define Pydantic models in `api/models.py`
2. Write tests with `unittest.mock.AsyncMock` for the Claude client
3. Implement in `agents/<name>/` extending `BaseAgent`
4. Use L1 sanitization + L3 XML fencing for all user-facing input
5. Never make real API calls in tests

## PR Requirements

Every PR must pass:

```bash
uv run ruff check .              # Lint
uv run mypy . --ignore-missing-imports  # Types
uv run pytest tests/ -x --cov   # Tests (80% coverage gate)
```

For frontend changes:
```bash
cd frontend && npx tsc --noEmit && npm run build
```

### Checklist

- [ ] Tests written for new functionality
- [ ] Type hints on all function signatures
- [ ] No `print()` statements (use `structlog`)
- [ ] No direct `boto3` or `pathlib` usage outside `storage/`
- [ ] No inline Pydantic models (all in `api/models.py`)
- [ ] Security: user input sanitized before prompt interpolation

## Code Style

### Python

- Formatter/linter: [ruff](https://docs.astral.sh/ruff/)
- Type checker: [mypy](https://mypy-lang.org/)
- All Pydantic models use `model_config = ConfigDict(strict=True, extra="forbid")`
- Async/await for all I/O operations
- structlog for all logging

### TypeScript

- ESLint with the project config
- Strict TypeScript (`strict: true`)
- TanStack Query for all API calls

## Dependencies

Use `uv add <package>` to add Python dependencies, never `pip install`. For dev-only dependencies use `uv add --dev <package>`.

For frontend: `cd frontend && npm install <package>`.

## Naming Conventions

| Term | Meaning |
|---|---|
| **Task** | Pure Python, no LLM (e.g., `IngestionTask`) |
| **Agent** | Calls Claude API (e.g., `InsightAgent`) |
| **Flow** | Orchestrates Tasks and Agents (e.g., `PipelineFlow`) |
