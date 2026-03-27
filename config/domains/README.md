# Domain Configuration

Each YAML file in this directory defines a complete domain configuration
for the Veredas platform. The active domain is selected via the `DOMAIN_ID`
environment variable (defaults to `br_macro`).

## Creating a New Domain

1. **Copy `br_macro.yaml`** as a template:
   ```bash
   cp br_macro.yaml my_domain.yaml
   ```

2. **Update all fields** — every section must be filled:
   - `domain`: country info, supported languages
   - `ai`: analyst role, safety message, anomaly context (bilingual)
   - `data_sources`: external APIs the pipeline ingests from
   - `router`: regex patterns for direct-lookup query routing
   - `series`: all tracked data series with labels, units, colors, keywords
   - `app`: title, cookie name, GitHub URL
   - `landing`: hero text, feature cards (bilingual)

3. **Create feed configs** in `data/feeds/` for each series in your domain.
   Each feed YAML defines the ingestion source, schedule, and processing rules.

4. **Set the environment variable**:
   ```bash
   export DOMAIN_ID=my_domain
   ```

5. **Run the pipeline** to verify ingestion:
   ```bash
   uv run python -m pipeline.flow
   ```

6. **Start the API** and verify your config is served:
   ```bash
   curl http://localhost:8000/api/config/domain | jq .domain.name
   ```

7. **Test query routing** — verify your series keywords are recognized:
   ```bash
   curl -X POST http://localhost:8000/api/query \
     -H 'Content-Type: application/json' \
     -d '{"question": "What is the current <your-keyword>?"}'
   ```

## Schema Reference

All fields are validated by the Pydantic v2 models in `config/domain.py`:

- `DomainConfig` — root model
- `DomainInfo` — country, language, currency
- `AIConfig` — prompts, roles, safety messages
- `DataSourceConfig` — external data source metadata
- `RouterConfig` / `RouterPatternConfig` — query routing rules
- `SeriesDisplayConfig` — per-series display and freshness config
- `AppConfig` — application title, cookie, GitHub URL
- `LandingConfig` / `LandingFeatureConfig` — landing page content

All text fields that appear in the UI use `LocalizedStr` (with `en` and `pt`
sub-fields) to support bilingual rendering.

## Files

- `br_macro.yaml` — Brazilian macroeconomic data (production domain)
- `test_demo.yaml` — Test domain for white-label validation (tests only)
