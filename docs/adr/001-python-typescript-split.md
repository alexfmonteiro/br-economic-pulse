# ADR-001: Python + TypeScript Split Architecture

## Status
Accepted

## Context
Veredas requires both a data pipeline (ingestion, transformation,
quality checks, AI agents) and an interactive frontend dashboard. We needed
to decide whether to use a single language across the stack or split between
Python and TypeScript.

Python dominates the data ecosystem — DuckDB, pandas, Parquet tooling, and
the Anthropic SDK are all Python-first. TypeScript with React is the
best-in-class choice for interactive data dashboards with charting libraries
like Recharts.

## Decision
Use Python 3.12 for the pipeline, AI agents, and FastAPI backend. Use
TypeScript with React 18, Vite, and Tailwind for the frontend. This is a
deliberate architectural split, not a compromise.

## Consequences
- Pipeline and API share the same Python process — no language boundary
  or serialization overhead for internal calls
- Frontend team (or future contributors) can work independently in the
  TypeScript codebase
- Two dependency ecosystems to maintain (pip-audit + npm audit)
- CI pipeline must test both stacks
- DuckDB client maturity is strongest in Python — no risk of second-class
  driver issues
