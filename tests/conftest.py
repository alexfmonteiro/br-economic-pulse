"""Shared test fixtures."""

from __future__ import annotations

from typing import Generator

import pytest

from config.domain import DomainConfig
from config.loader import load_domain_config, reset_domain_config


@pytest.fixture
def domain_config() -> Generator[DomainConfig, None, None]:
    """Load the br_macro domain config for tests."""
    reset_domain_config()
    config = load_domain_config("br_macro")
    yield config
    reset_domain_config()


@pytest.fixture
def test_domain_config() -> Generator[DomainConfig, None, None]:
    """Load the test_demo domain config for white-label tests."""
    reset_domain_config()
    config = load_domain_config("test_demo")
    yield config
    reset_domain_config()
