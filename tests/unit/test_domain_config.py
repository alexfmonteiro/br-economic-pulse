"""Tests for the domain configuration module."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from config.domain import DomainConfig, LocalizedStr
from config.loader import get_domain_config, load_domain_config, reset_domain_config

EXPECTED_SERIES = [
    "bcb_selic",
    "bcb_ipca",
    "bcb_usd_brl",
    "ibge_pnad",
    "ibge_gdp",
    "tesouro_prefixado_curto",
    "tesouro_prefixado_longo",
    "tesouro_ipca",
]

EXPECTED_DATA_SOURCES = ["bcb", "ibge", "tesouro"]


@pytest.fixture(autouse=True)
def _clear_singleton() -> None:
    """Reset the singleton before each test."""
    reset_domain_config()


def test_load_br_macro_config() -> None:
    """Loading br_macro.yaml produces a valid DomainConfig."""
    config = load_domain_config("br_macro")
    assert isinstance(config, DomainConfig)
    assert config.domain.name == "Veredas Brazil"
    assert config.domain.country_code == "BR"
    assert len(config.series) == 8


def test_all_series_present() -> None:
    """All 8 expected series IDs exist in the config."""
    config = load_domain_config("br_macro")
    for series_id in EXPECTED_SERIES:
        assert series_id in config.series, f"Missing series: {series_id}"


def test_all_data_sources_present() -> None:
    """All 3 expected data sources exist."""
    config = load_domain_config("br_macro")
    source_ids = [ds.id for ds in config.data_sources]
    for ds_id in EXPECTED_DATA_SOURCES:
        assert ds_id in source_ids, f"Missing data source: {ds_id}"


def test_all_series_have_keywords() -> None:
    """Every series has at least 1 keyword."""
    config = load_domain_config("br_macro")
    for series_id, series in config.series.items():
        assert len(series.keywords) >= 1, f"{series_id} has no keywords"


def test_localized_strings_complete() -> None:
    """All LocalizedStr fields have non-empty en and pt values."""
    config = load_domain_config("br_macro")

    def check_localized(obj: object, path: str) -> None:
        if isinstance(obj, LocalizedStr):
            assert obj.en.strip(), f"{path}.en is empty"
            assert obj.pt.strip(), f"{path}.pt is empty"
        elif hasattr(type(obj), "model_fields"):
            for field_name in type(obj).model_fields:  # type: ignore[attr-defined]
                value = getattr(obj, field_name)
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        check_localized(item, f"{path}.{field_name}[{i}]")
                elif isinstance(value, dict):
                    for k, v in value.items():
                        check_localized(v, f"{path}.{field_name}.{k}")
                else:
                    check_localized(value, f"{path}.{field_name}")

    check_localized(config, "config")


def test_cache_behavior() -> None:
    """get_domain_config() returns the same instance on repeated calls."""
    first = get_domain_config()
    second = get_domain_config()
    assert first is second


def test_multi_domain_cache() -> None:
    """Multiple domains can be cached simultaneously."""
    br = get_domain_config("br_macro")
    demo = get_domain_config("test_demo")
    assert br is not demo
    assert br.domain.name != demo.domain.name
    # Both remain cached
    assert get_domain_config("br_macro") is br
    assert get_domain_config("test_demo") is demo


def test_reset_clears_all_caches() -> None:
    """reset_domain_config() clears all cached domains."""
    get_domain_config("br_macro")
    get_domain_config("test_demo")
    reset_domain_config()
    fresh = get_domain_config("br_macro")
    assert fresh.domain.id == "br_macro"


def test_invalid_yaml_raises() -> None:
    """Passing a nonexistent domain_id raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_domain_config("nonexistent_domain")


def test_extra_fields_rejected() -> None:
    """YAML with unknown fields raises ValidationError."""
    with pytest.raises(ValidationError):
        DomainConfig.model_validate({"domain": {}, "bogus_field": True})


def test_router_patterns_present() -> None:
    """Router config has direct lookup patterns."""
    config = load_domain_config("br_macro")
    assert len(config.router.direct_lookup_patterns) >= 3
    for pat in config.router.direct_lookup_patterns:
        assert pat.handler == "latest_value"
        assert "{keywords}" in pat.pattern


def test_app_config() -> None:
    """App config has correct values."""
    config = load_domain_config("br_macro")
    assert config.app.title == "Veredas"
    assert config.app.session_cookie_name == "veredas_session"
    assert "github.com" in config.app.github_url


def test_landing_features_count() -> None:
    """Landing page has 6 feature cards."""
    config = load_domain_config("br_macro")
    assert len(config.landing.features) == 6


# ---------------------------------------------------------------------------
# Second-domain (test_demo) validation
# ---------------------------------------------------------------------------


def test_second_domain_loads() -> None:
    """test_demo.yaml parses into a valid DomainConfig."""
    config = load_domain_config("test_demo")
    assert isinstance(config, DomainConfig)
    assert config.domain.name == "TestPlatform"
    assert config.domain.country == "Testland"
    assert len(config.series) == 2


def test_second_domain_prompts_use_config() -> None:
    """Prompts built with test_demo contain 'Testland', not 'Brazilian'."""
    config = load_domain_config("test_demo")

    from security.xml_fencing import build_insight_prompt

    system, _ = build_insight_prompt("test data", config=config)
    assert "Testland" in system
    assert "Brazilian" not in system


def test_second_domain_router_keywords() -> None:
    """test_demo keywords route correctly; br_macro keywords do not."""
    reset_domain_config()
    config = load_domain_config("test_demo")

    # Build keywords from test_demo config
    keywords: dict[str, str] = {}
    for sid, series in config.series.items():
        for kw in series.keywords:
            keywords[kw] = sid

    assert "rate1" in keywords
    assert keywords["rate1"] == "test_rate"
    assert "selic" not in keywords


def test_second_domain_series_config() -> None:
    """Series config built from test_demo has test_rate, not bcb_selic."""
    config = load_domain_config("test_demo")
    assert "test_rate" in config.series
    assert "test_index" in config.series
    assert "bcb_selic" not in config.series


def test_second_domain_safety_message() -> None:
    """Safety message mentions Testland scope."""
    config = load_domain_config("test_demo")
    scope = config.ai.scope_description.en
    indicators = config.ai.example_indicators.en
    msg = config.ai.safety_message.en.format(
        scope=scope, example_indicators=indicators,
    )
    assert "Testland" in msg
    assert "RATE1" in msg


def test_second_domain_data_sources() -> None:
    """test_demo data sources contain TDB, not BCB."""
    config = load_domain_config("test_demo")
    source_ids = [ds.id for ds in config.data_sources]
    assert "tdb" in source_ids
    assert "bcb" not in source_ids


def test_domains_are_isolated() -> None:
    """Loading different domains produces different prompts."""
    from security.xml_fencing import build_insight_prompt

    # Load br_macro
    br = load_domain_config("br_macro")
    system_br, _ = build_insight_prompt("data", config=br)
    assert "Brazilian" in system_br.lower() or br.ai.analyst_role.en in system_br

    # Reset and load test_demo
    reset_domain_config()
    demo = load_domain_config("test_demo")
    system_demo, _ = build_insight_prompt("data", config=demo)
    assert "Testland" in system_demo
    assert demo.ai.analyst_role.en in system_demo

    # Verify they differ
    assert system_br != system_demo
