"""Domain config loader with singleton caching."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from config.domain import DomainConfig

_config: DomainConfig | None = None
_CONFIG_DIR = Path(__file__).parent / "domains"


def load_domain_config(domain_id: str | None = None) -> DomainConfig:
    """Load and validate a domain config from YAML."""
    global _config  # noqa: PLW0603
    if domain_id is None:
        domain_id = os.environ.get("DOMAIN_ID", "br_macro")
    path = _CONFIG_DIR / f"{domain_id}.yaml"
    with open(path) as f:
        raw = yaml.safe_load(f)
    _config = DomainConfig.model_validate(raw)
    return _config


def get_domain_config() -> DomainConfig:
    """Return the cached config, loading br_macro if not yet loaded."""
    global _config  # noqa: PLW0603
    if _config is None:
        load_domain_config()
    assert _config is not None  # noqa: S101
    return _config


def reset_domain_config() -> None:
    """Clear the cached config (for testing)."""
    global _config  # noqa: PLW0603
    _config = None
