#!/usr/bin/env python3
"""
Download all fixture data for Veredas tests.

Covers Step 0.4 of the Execution Playbook:
- BCB series: SELIC (432), IPCA (433), USD/BRL (1)
- IBGE SIDRA: Unemployment / PNAD Contínua (table 6381)
- Tesouro Direto: Bond yields CSV

Usage:
    python scripts/download_fixtures.py
    # or
    uv run scripts/download_fixtures.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"

TIMEOUT = 30  # seconds per request
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds between retries


def download(url: str, description: str) -> httpx.Response:
    """Download a URL with retries."""
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            print(f"  [{attempt}/{RETRY_ATTEMPTS}] Fetching {description}...")
            resp = httpx.get(url, timeout=TIMEOUT, follow_redirects=True)
            resp.raise_for_status()
            return resp
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            last_exc = exc
            print(f"  Warning: attempt {attempt} failed: {exc}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
            else:
                raise
    raise RuntimeError(f"All {RETRY_ATTEMPTS} attempts failed for {description}: {last_exc}")


def download_bcb_series() -> None:
    """Download BCB (Banco Central) series fixtures."""
    series: dict[str, dict[str, int | str]] = {
        "bcb_selic.json": {
            "code": 432,
            "description": "SELIC rate",
        },
        "bcb_ipca.json": {
            "code": 433,
            "description": "IPCA (inflation)",
        },
        "bcb_usd_brl.json": {
            "code": 1,
            "description": "USD/BRL exchange rate",
        },
    }

    for filename, info in series.items():
        url = (
            f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{info['code']}"
            f"/dados/ultimos/10?formato=json"
        )
        print(f"\n-> BCB {info['description']} (series {info['code']})")
        resp = download(url, str(info["description"]))
        data = resp.json()
        out = FIXTURES_DIR / filename
        out.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        print(f"   Saved {out} ({len(data)} records)")


def download_ibge_sidra() -> None:
    """Download IBGE SIDRA unemployment data (PNAD Contínua, table 6381)."""
    # Table 6381: Taxa de desocupação
    # v/4099 = taxa de desocupação (%)
    # n1/all = Brasil
    # p/last 10 = last 10 periods
    url = (
        "https://apisidra.ibge.gov.br/values"
        "/t/6381/n1/all/v/4099/p/last%2010/d/v4099%201"
    )
    print("\n-> IBGE SIDRA: Unemployment / PNAD Contínua (table 6381)")
    resp = download(url, "IBGE unemployment")
    data = resp.json()
    out = FIXTURES_DIR / "ibge_sample.json"
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    # First row is header metadata in SIDRA API
    record_count = max(0, len(data) - 1)
    print(f"   Saved {out} ({record_count} data records + 1 header)")


def download_tesouro_direto() -> None:
    """Download Tesouro Direto bond yields CSV.

    Domain migrated from tesourotransparente.com.br to .gov.br (2025).
    Uses the CKAN API to dynamically resolve the CSV URL as a fallback.
    """
    # Primary: direct download from Tesouro Transparente CKAN (.gov.br domain)
    url = (
        "https://www.tesourotransparente.gov.br/ckan/dataset/"
        "df56aa42-484a-4a59-8184-7676580c81e3/resource/"
        "796d2059-14e9-44e3-80c9-2d9e30b405c1/download/"
        "PrecoTaxaTesouroDireto.csv"
    )
    print("\n-> Tesouro Direto: Bond yields CSV")
    try:
        resp = download(url, "Tesouro Direto yields")
        out = FIXTURES_DIR / "tesouro_sample.csv"
        out.write_bytes(resp.content)
        lines = resp.text.strip().split("\n")
        print(f"   Saved {out} ({len(lines)} lines)")
    except Exception as exc:
        print(f"   Warning: Direct download failed ({exc})")
        print("   Trying CKAN API to resolve current download URL...")
        try:
            api_url = (
                "https://www.tesourotransparente.gov.br/ckan/api/3/action/"
                "package_show?id=taxas-dos-titulos-ofertados-pelo-tesouro-direto"
            )
            api_resp = download(api_url, "CKAN metadata API")
            metadata = api_resp.json()
            resources = metadata.get("result", {}).get("resources", [])
            csv_url = None
            for res in resources:
                if res.get("format", "").upper() == "CSV":
                    csv_url = res.get("url")
                    break
            if not csv_url:
                raise ValueError("No CSV resource found in CKAN metadata")
            print(f"   Resolved CSV URL: {csv_url}")
            resp = download(csv_url, "Tesouro Direto yields (via CKAN API)")
            out = FIXTURES_DIR / "tesouro_sample.csv"
            out.write_bytes(resp.content)
            lines = resp.text.strip().split("\n")
            print(f"   Saved {out} ({len(lines)} lines)")
        except Exception as exc2:
            print(f"   Error: CKAN API fallback also failed ({exc2})")
            print(
                "   Please download manually from:\n"
                "   https://www.tesourotransparente.gov.br/ckan/dataset/"
                "taxas-dos-titulos-ofertados-pelo-tesouro-direto\n"
                "   and save as tests/fixtures/tesouro_sample.csv"
            )


def main() -> int:
    print(f"Fixture directory: {FIXTURES_DIR}")
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    errors: list[str] = []

    # BCB series
    try:
        download_bcb_series()
    except Exception as exc:
        errors.append(f"BCB download failed: {exc}")
        print(f"\n   ERROR: {exc}")

    # IBGE SIDRA
    try:
        download_ibge_sidra()
    except Exception as exc:
        errors.append(f"IBGE download failed: {exc}")
        print(f"\n   ERROR: {exc}")

    # Tesouro Direto
    try:
        download_tesouro_direto()
    except Exception as exc:
        errors.append(f"Tesouro download failed: {exc}")
        print(f"\n   ERROR: {exc}")

    # Summary
    print("\n" + "=" * 50)
    fixtures = list(FIXTURES_DIR.iterdir())
    print(f"Fixtures downloaded: {len(fixtures)}")
    for f in sorted(fixtures):
        size = f.stat().st_size
        print(f"  {f.name:30s} {size:>10,} bytes")

    if errors:
        print(f"\nWarnings ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("\nAll fixtures downloaded successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
