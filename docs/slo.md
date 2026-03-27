# Service Level Objectives (SLOs)

> Initial SLO definitions for Veredas. These targets guide
> alerting thresholds and architectural decisions. Revisit quarterly
> as usage patterns emerge.

## API Availability

| Metric | Target | Measurement |
|--------|--------|-------------|
| Uptime | 99.5% | Successful responses (2xx/3xx) / total requests, measured monthly |

Excludes scheduled maintenance windows (announced 24h in advance).

## Query Latency (p95)

| Tier | Target | Notes |
|------|--------|-------|
| DIRECT_LOOKUP (Tier 1) | < 100ms | Regex match + DuckDB read from gold parquet |
| FULL_LLM (Tier 3) | < 3s | Includes Claude API round-trip |
| Cache hit | < 50ms | Upstash Redis GET + response deserialization |

Measured at the API gateway (Railway), end-to-end from request received
to response sent.

## Data Freshness

| Metric | Target | Notes |
|--------|--------|-------|
| Gold data lag | < 2 hours after pipeline completion | Time between pipeline `finished_at` and gold parquet availability via API |
| R2 → local sync | < 30 minutes | Sync job runs every 15 minutes in production |

Series-specific freshness thresholds are defined in `api/series_config.py`
(e.g., SELIC: 72h, IPCA: 1080h) and reflected in the `/api/quality/latest`
endpoint.

## Pipeline Success Rate

| Metric | Target | Notes |
|--------|--------|-------|
| Successful runs | > 95% | Measured over a rolling 30-day window |
| Partial failures | Counted as success | If at least 1 stage completes and writes data |

Upstream API outages (BCB, IBGE, Tesouro) are expected and tolerated.
A run that fails due to a transient source error should be retried by
the next scheduled cron execution.

## Error Rate

| Metric | Target | Notes |
|--------|--------|-------|
| Query error rate | < 1% | 5xx responses / total query requests |
| Rate-limited responses (429) | Not counted as errors | Expected behavior under load |

## Current Monitoring

- **Sentry**: Error tracking for API and frontend (two separate projects)
- **structlog**: Structured logging with `duration_ms`, `tier_used`, `cache_hit` fields
- **`/api/query/usage`**: Token usage, cost, and latency aggregates
- **`/api/quality/latest`**: Series freshness and pipeline quality checks
- **`/api/runs`**: Pipeline run history with per-stage reconciliation

## Future Work

- Grafana dashboard for real-time SLO tracking (#35)
- Automated alerting when SLO budget is consumed (#34)
- Synthetic uptime monitoring (external probe)
