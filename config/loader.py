"""Domain config loader with multi-domain caching."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from config.domain import DomainConfig

_configs: dict[str, DomainConfig] = {}
_CONFIG_DIR = Path(__file__).parent / "domains"


def load_domain_config(domain_id: str | None = None) -> DomainConfig:
    """Load and validate a domain config from YAML."""
    if domain_id is None:
        domain_id = os.environ.get("DOMAIN_ID", "br_macro")
    path = _CONFIG_DIR / f"{domain_id}.yaml"
    with open(path) as f:
        raw = yaml.safe_load(f)
    config = DomainConfig.model_validate(raw)
    _configs[domain_id] = config
    return config


def get_domain_config(domain_id: str | None = None) -> DomainConfig:
    """Return the cached config for a domain, loading it if needed.

    When domain_id is None, uses the DOMAIN_ID env var (default: br_macro).
    Multiple domains can be cached simultaneously.
    """
    if domain_id is None:
        domain_id = os.environ.get("DOMAIN_ID", "br_macro")
    if domain_id not in _configs:
        load_domain_config(domain_id)
    return _configs[domain_id]


def reset_domain_config() -> None:
    """Clear all cached configs (for testing)."""
    _configs.clear()
