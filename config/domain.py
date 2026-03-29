"""Pydantic v2 models for domain configuration."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class LocalizedStr(BaseModel):
    """Bilingual string — enforces both languages present at parse time."""

    model_config = ConfigDict(strict=True, extra="forbid")

    en: str
    pt: str


class DomainInfo(BaseModel):
    """Top-level domain metadata."""

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str
    name: str
    description: str
    country: str
    country_code: str
    currency: str
    currency_symbol: str
    timezone: str
    default_language: str
    supported_languages: list[str]


class AIConfig(BaseModel):
    """AI agent configuration — prompts, roles, safety messages."""

    model_config = ConfigDict(strict=True, extra="forbid")

    scope_description: LocalizedStr
    analyst_role: LocalizedStr
    safety_message: LocalizedStr
    example_indicators: LocalizedStr
    anomaly_context: LocalizedStr


class DataSourceConfig(BaseModel):
    """External data source metadata."""

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str
    name: str
    url: str
    description: LocalizedStr


class RouterPatternConfig(BaseModel):
    """A single regex pattern for direct lookup routing."""

    model_config = ConfigDict(strict=True, extra="forbid")

    pattern: str
    handler: str


class RouterConfig(BaseModel):
    """Query routing configuration."""

    model_config = ConfigDict(strict=True, extra="forbid")

    direct_lookup_patterns: list[RouterPatternConfig]


class TypicalRange(BaseModel):
    """Expected value range for sanity-checking data quality."""

    model_config = ConfigDict(strict=True, extra="forbid")

    min: float
    max: float


class SeriesDisplayConfig(BaseModel):
    """Display metadata for a tracked data series."""

    model_config = ConfigDict(strict=True, extra="forbid")

    label: LocalizedStr
    unit: str
    source: str
    color: str
    freshness_hours: int
    domain: str
    description: LocalizedStr
    keywords: list[str]
    chart_granularity: str = "day"
    typical_range: TypicalRange | None = None


class LandingFeatureConfig(BaseModel):
    """A feature card on the landing page."""

    model_config = ConfigDict(strict=True, extra="forbid")

    icon: str
    title: LocalizedStr
    description: LocalizedStr


class AppConfig(BaseModel):
    """Application-level configuration."""

    model_config = ConfigDict(strict=True, extra="forbid")

    title: str
    session_cookie_name: str
    meta_description: LocalizedStr
    github_url: str


class LandingConfig(BaseModel):
    """Landing page configuration."""

    model_config = ConfigDict(strict=True, extra="forbid")

    hero_title: LocalizedStr
    hero_subtitle: LocalizedStr
    features: list[LandingFeatureConfig]


class DomainConfig(BaseModel):
    """Root domain configuration — loaded from YAML."""

    model_config = ConfigDict(strict=True, extra="forbid")

    domain: DomainInfo
    ai: AIConfig
    data_sources: list[DataSourceConfig]
    router: RouterConfig
    series: dict[str, SeriesDisplayConfig]
    app: AppConfig
    landing: LandingConfig
