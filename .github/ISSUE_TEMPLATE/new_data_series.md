---
name: New Data Series
about: Propose a new YAML feed config for data ingestion
title: "[Series] "
labels: enhancement, data
---

## Series Information

**Series name:** <!-- e.g., IGP-M, CDI, CAGED -->
**Source API:** <!-- e.g., BCB SGS, IBGE SIDRA, Tesouro Transparente -->
**Source series code:** <!-- e.g., BCB code 189 for IGP-M -->

## Update Frequency

<!-- How often does the source publish new data? -->
- [ ] Daily (business days)
- [ ] Monthly
- [ ] Quarterly
- [ ] Other: <!-- specify -->

## Expected Schema

| Field | Type | Example |
|-------|------|---------|
| date | date | 2026-01-15 |
| value | float | 0.45 |
| <!-- add more --> | | |

**Unit:** <!-- e.g., % a.m., R$, index -->

## Feed Config Draft

```yaml
series_id: ""
source: ""
api_url: ""
frequency: ""
unit: ""
freshness_hours: 72
tags: []
```

## Context

<!-- Why is this series valuable? What questions does it help answer? -->

## Checklist

- [ ] Source API is publicly accessible (no authentication required)
- [ ] Data is available in JSON or CSV format
- [ ] Series has at least 1 year of historical data
- [ ] No existing series already covers this indicator
