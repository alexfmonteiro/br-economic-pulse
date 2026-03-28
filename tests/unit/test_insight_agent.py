"""Tests for InsightAgent — agents/insight/agent.py."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import anthropic.types as atypes
import pytest

from agents.insight.agent import (
    InsightAgent,
    _build_series_descriptions,
    _compute_anomaly_hash,
    _compute_z_scores,
    _detect_anomalies,
    _extract_z_score,
    _format_anomaly_prompt_data,
    _format_gold_summary,
    _parse_insight_sections,
)


# ---------------------------------------------------------------------------
# _compute_z_scores
# ---------------------------------------------------------------------------


class TestComputeZScores:
    def test_normal_data(self) -> None:
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        z = _compute_z_scores(values)
        assert len(z) == 5
        # Mean=30, std~=14.14 → z[0] ~ -1.41, z[4] ~ +1.41
        assert z[0] < 0
        assert z[4] > 0
        # Middle value should be near zero
        assert abs(z[2]) < 1e-10

    def test_empty_list(self) -> None:
        assert _compute_z_scores([]) == []

    def test_single_value(self) -> None:
        assert _compute_z_scores([42.0]) == []

    def test_all_same_values(self) -> None:
        values = [5.0, 5.0, 5.0, 5.0]
        z = _compute_z_scores(values)
        assert len(z) == 4
        # std=0 → all z-scores should be 0.0
        assert all(score == 0.0 for score in z)

    def test_two_values(self) -> None:
        values = [0.0, 10.0]
        z = _compute_z_scores(values)
        assert len(z) == 2
        # Symmetric around mean=5
        assert z[0] == pytest.approx(-z[1])


# ---------------------------------------------------------------------------
# _detect_anomalies
# ---------------------------------------------------------------------------


class TestDetectAnomalies:
    def test_detects_anomaly_with_high_z_score(self) -> None:
        """A single outlier should be flagged when |z| > 2.0."""
        # 9 values near 10, one huge outlier
        rows: list[dict[str, Any]] = [
            {"date": f"2024-0{i+1}-01", "value": 10.0} for i in range(9)
        ]
        rows.append({"date": "2024-10-01", "value": 100.0})
        series_data = {"bcb_selic": rows}
        anomalies = _detect_anomalies(series_data)
        assert len(anomalies) >= 1
        assert "bcb_selic" in anomalies[0]
        assert "100.0" in anomalies[0]

    def test_no_anomalies_in_uniform_data(self) -> None:
        rows: list[dict[str, Any]] = [
            {"date": f"2024-0{i+1}-01", "value": 10.0} for i in range(5)
        ]
        series_data = {"bcb_selic": rows}
        anomalies = _detect_anomalies(series_data)
        assert anomalies == []

    def test_skips_series_with_fewer_than_two_values(self) -> None:
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_usd_brl": [{"date": "2024-01-01", "value": 5.0}],
        }
        anomalies = _detect_anomalies(series_data)
        assert anomalies == []

    def test_multiple_series(self) -> None:
        normal_rows: list[dict[str, Any]] = [
            {"date": f"2024-0{i+1}-01", "value": 10.0} for i in range(5)
        ]
        outlier_rows: list[dict[str, Any]] = [
            {"date": f"2024-0{i+1}-01", "value": 10.0} for i in range(9)
        ]
        outlier_rows.append({"date": "2024-10-01", "value": 200.0})
        series_data = {
            "bcb_selic": normal_rows,
            "bcb_ipca": outlier_rows,
        }
        anomalies = _detect_anomalies(series_data)
        assert any("bcb_ipca" in a for a in anomalies)

    def test_skips_none_values(self) -> None:
        rows: list[dict[str, Any]] = [
            {"date": "2024-01-01", "value": 10.0},
            {"date": "2024-02-01", "value": None},
            {"date": "2024-03-01", "value": 10.0},
        ]
        series_data = {"bcb_usd_brl": rows}
        # Should not raise — None values are filtered out
        anomalies = _detect_anomalies(series_data)
        assert isinstance(anomalies, list)


# ---------------------------------------------------------------------------
# _format_gold_summary
# ---------------------------------------------------------------------------


class TestFormatGoldSummary:
    def test_basic_formatting(self) -> None:
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [
                {"date": "2024-01-01", "value": 11.75},
                {"date": "2024-03-01", "value": 12.25},
            ],
        }
        result = _format_gold_summary(series_data, anomalies=[])
        assert "bcb_selic" in result
        assert "2 data points" in result
        assert "2024-01-01" in result
        assert "2024-03-01" in result
        assert "12.25" in result

    def test_empty_series(self) -> None:
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_usd_brl": [],
        }
        result = _format_gold_summary(series_data, anomalies=[])
        assert "bcb_usd_brl: no data available" in result

    def test_includes_anomalies(self) -> None:
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [{"date": "2024-01-01", "value": 11.0}],
        }
        anomalies = ["bcb_selic on 2024-01-01: value=11.0, z-score=2.50"]
        result = _format_gold_summary(series_data, anomalies)
        assert "Anomalies detected" in result
        assert "z-score=2.50" in result

    def test_no_anomalies_omits_section(self) -> None:
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [{"date": "2024-01-01", "value": 11.0}],
        }
        result = _format_gold_summary(series_data, anomalies=[])
        assert "Anomalies" not in result


# ---------------------------------------------------------------------------
# _parse_insight_sections
# ---------------------------------------------------------------------------


class TestParseInsightSections:
    def test_pt_and_en_sections(self) -> None:
        raw = "<pt>Resumo em portugues</pt>\n<en>Summary in English</en>"
        now = datetime.now(timezone.utc)
        records = _parse_insight_sections(
            raw, run_id="abc123", generated_at=now,
            metric_refs=["bcb_selic"], confidence_flag=True,
        )
        assert len(records) == 2
        assert records[0].language == "pt"
        assert records[0].content == "Resumo em portugues"
        assert records[1].language == "en"
        assert records[1].content == "Summary in English"
        assert records[0].run_id == "abc123"
        assert records[0].metric_refs == ["bcb_selic"]

    def test_fallback_to_en_only(self) -> None:
        raw = "Just some plain text without tags."
        now = datetime.now(timezone.utc)
        records = _parse_insight_sections(
            raw, run_id="xyz", generated_at=now,
            metric_refs=[], confidence_flag=False,
        )
        assert len(records) == 1
        assert records[0].language == "en"
        assert records[0].content == "Just some plain text without tags."
        assert records[0].confidence_flag is False

    def test_only_pt_tag(self) -> None:
        raw = "<pt>Apenas portugues</pt>"
        now = datetime.now(timezone.utc)
        records = _parse_insight_sections(
            raw, run_id="r1", generated_at=now,
            metric_refs=[], confidence_flag=True,
        )
        assert len(records) == 1
        assert records[0].language == "pt"

    def test_empty_tags_fallback(self) -> None:
        raw = "<pt></pt>\n<en></en>"
        now = datetime.now(timezone.utc)
        records = _parse_insight_sections(
            raw, run_id="r2", generated_at=now,
            metric_refs=[], confidence_flag=True,
        )
        # Empty tags → empty content → skipped → fallback to entire text as EN
        assert len(records) == 1
        assert records[0].language == "en"

    def test_model_version_set(self) -> None:
        raw = "<pt>Texto</pt><en>Text</en>"
        now = datetime.now(timezone.utc)
        records = _parse_insight_sections(
            raw, run_id="r3", generated_at=now,
            metric_refs=[], confidence_flag=True,
        )
        for rec in records:
            assert rec.model_version == "claude-sonnet-4-20250514"


# ---------------------------------------------------------------------------
# InsightAgent._execute — integration-style tests with all deps mocked
# ---------------------------------------------------------------------------


def _make_gold_rows(
    series: str, count: int = 3, base_value: float = 11.0,
) -> list[dict[str, Any]]:
    """Helper to create gold data rows."""
    return [
        {
            "date": datetime(2024, i + 1, 1),
            "value": base_value + i * 0.25,
            "series": series,
        }
        for i in range(count)
    ]


class TestInsightAgentExecute:
    @pytest.mark.asyncio
    async def test_successful_run_with_pt_en(self) -> None:
        """Full happy-path: gold data available, Claude returns PT+EN, Postgres stores."""
        gold_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": _make_gold_rows("bcb_selic"),
            "bcb_ipca": _make_gold_rows("bcb_ipca"),
            "bcb_usd_brl": _make_gold_rows("bcb_usd_brl"),
            "ibge_pnad": _make_gold_rows("ibge_pnad"),
            "ibge_gdp": _make_gold_rows("ibge_gdp"),
            "tesouro_prefixado_curto": _make_gold_rows("tesouro_prefixado_curto"),
            "tesouro_prefixado_longo": _make_gold_rows("tesouro_prefixado_longo"),
            "tesouro_ipca": _make_gold_rows("tesouro_ipca"),
        }

        async def mock_query_gold(series: str, after: str | None = None) -> list[dict[str, Any]]:
            return gold_data.get(series, [])

        # Mock Claude response
        mock_text_block = atypes.TextBlock(
            type="text",
            text="<pt>Resumo em PT</pt>\n<en>Summary in EN</en>",
        )
        mock_response = MagicMock()
        mock_response.content = [mock_text_block]

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        # Mock asyncpg
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        agent = InsightAgent()

        with (
            patch("agents.insight.agent.query_gold_series", side_effect=mock_query_gold),
            patch("agents.insight.agent.anthropic.AsyncAnthropic", return_value=mock_client),
            patch("asyncpg.connect", AsyncMock(return_value=mock_conn)),
            patch.dict(os.environ, {
                "DATABASE_URL": "postgresql://test:test@localhost/test",
                "ANTHROPIC_API_KEY": "test-key",
            }),
        ):
            result = await agent._execute()

        assert result.success is True
        assert result.agent_name == "insight"
        assert result.rows_processed > 0
        assert result.errors == []
        # Verify Claude was called
        mock_client.messages.create.assert_awaited_once()
        # Verify Postgres storage (CREATE TABLE + 2 INSERTs for PT and EN)
        assert mock_conn.execute.await_count >= 3

    @pytest.mark.asyncio
    async def test_no_gold_data_returns_failure(self) -> None:
        """When no gold data is available, agent should return success=False."""
        async def mock_query_empty(series: str, after: str | None = None) -> list[dict[str, Any]]:
            return []

        agent = InsightAgent()

        with patch("agents.insight.agent.query_gold_series", side_effect=mock_query_empty):
            result = await agent._execute()

        assert result.success is False
        assert any("No gold data" in e for e in result.errors)

    @pytest.mark.asyncio
    async def test_claude_api_error_returns_failure(self) -> None:
        """When Claude API raises an exception, agent should return success=False."""
        gold_data = {"bcb_selic": _make_gold_rows("bcb_selic")}

        async def mock_query_gold(series: str, after: str | None = None) -> list[dict[str, Any]]:
            return gold_data.get(series, [])

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(
            side_effect=RuntimeError("API rate limit exceeded")
        )

        agent = InsightAgent()

        with (
            patch("agents.insight.agent.query_gold_series", side_effect=mock_query_gold),
            patch("agents.insight.agent.anthropic.AsyncAnthropic", return_value=mock_client),
            patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}),
        ):
            result = await agent._execute()

        assert result.success is False
        assert any("Claude API call failed" in e for e in result.errors)

    @pytest.mark.asyncio
    async def test_postgres_storage_error_returns_failure(self) -> None:
        """When Postgres storage fails, agent should return success=False."""
        gold_data = {"bcb_selic": _make_gold_rows("bcb_selic")}

        async def mock_query_gold(series: str, after: str | None = None) -> list[dict[str, Any]]:
            return gold_data.get(series, [])

        # Mock Claude response (success)
        mock_text_block = atypes.TextBlock(
            type="text",
            text="<pt>Resumo</pt>\n<en>Summary</en>",
        )
        mock_response = MagicMock()
        mock_response.content = [mock_text_block]

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        # Mock asyncpg to fail
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(side_effect=RuntimeError("Connection refused"))
        mock_conn.close = AsyncMock()

        agent = InsightAgent()

        with (
            patch("agents.insight.agent.query_gold_series", side_effect=mock_query_gold),
            patch("agents.insight.agent.anthropic.AsyncAnthropic", return_value=mock_client),
            patch("asyncpg.connect", AsyncMock(return_value=mock_conn)),
            patch.dict(os.environ, {
                "DATABASE_URL": "postgresql://test:test@localhost/test",
                "ANTHROPIC_API_KEY": "test-key",
            }),
        ):
            result = await agent._execute()

        assert result.success is False
        assert any("Postgres storage failed" in e for e in result.errors)

    @pytest.mark.asyncio
    async def test_partial_gold_data_with_warnings(self) -> None:
        """When some series fail to read, agent should still proceed with warnings."""
        call_count = 0

        async def mock_query_gold(series: str, after: str | None = None) -> list[dict[str, Any]]:
            nonlocal call_count
            call_count += 1
            if series == "bcb_usd_brl":
                raise RuntimeError("Read error for bcb_usd_brl")
            if series == "bcb_selic":
                return _make_gold_rows("bcb_selic")
            return []

        mock_text_block = atypes.TextBlock(
            type="text", text="<en>Summary</en>",
        )
        mock_response = MagicMock()
        mock_response.content = [mock_text_block]

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()
        mock_conn.close = AsyncMock()

        agent = InsightAgent()

        with (
            patch("agents.insight.agent.query_gold_series", side_effect=mock_query_gold),
            patch("agents.insight.agent.anthropic.AsyncAnthropic", return_value=mock_client),
            patch("asyncpg.connect", AsyncMock(return_value=mock_conn)),
            patch.dict(os.environ, {
                "DATABASE_URL": "postgresql://test:test@localhost/test",
                "ANTHROPIC_API_KEY": "test-key",
            }),
        ):
            result = await agent._execute()

        assert result.success is True
        assert any("bcb_usd_brl" in w for w in result.warnings)

    @pytest.mark.asyncio
    async def test_database_url_missing_raises(self) -> None:
        """When DATABASE_URL is missing, _store_insights should raise ValueError."""
        gold_data = {"bcb_selic": _make_gold_rows("bcb_selic")}

        async def mock_query_gold(series: str, after: str | None = None) -> list[dict[str, Any]]:
            return gold_data.get(series, [])

        mock_text_block = atypes.TextBlock(
            type="text", text="<en>Summary</en>",
        )
        mock_response = MagicMock()
        mock_response.content = [mock_text_block]

        mock_client = MagicMock()
        mock_client.messages.create = AsyncMock(return_value=mock_response)

        agent = InsightAgent()

        with (
            patch("agents.insight.agent.query_gold_series", side_effect=mock_query_gold),
            patch("agents.insight.agent.anthropic.AsyncAnthropic", return_value=mock_client),
            patch.dict(os.environ, {
                "DATABASE_URL": "",
                "ANTHROPIC_API_KEY": "test-key",
            }, clear=False),
        ):
            result = await agent._execute()

        assert result.success is False
        assert any("Postgres storage failed" in e or "DATABASE_URL" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Anomaly analysis helpers
# ---------------------------------------------------------------------------


class TestComputeAnomalyHash:
    def test_deterministic(self) -> None:
        anomalies = ["bcb_selic on 2024-01-01: value=10, z-score=2.50", "bcb_usd_brl on 2024-02-01: value=6, z-score=2.10"]
        assert _compute_anomaly_hash(anomalies) == _compute_anomaly_hash(anomalies)

    def test_order_independent(self) -> None:
        a = ["first anomaly", "second anomaly"]
        b = ["second anomaly", "first anomaly"]
        assert _compute_anomaly_hash(a) == _compute_anomaly_hash(b)

    def test_different_inputs_different_hashes(self) -> None:
        a = ["bcb_selic on 2024-01-01: value=10, z-score=2.50"]
        b = ["bcb_ipca on 2024-01-01: value=10, z-score=2.50"]
        assert _compute_anomaly_hash(a) != _compute_anomaly_hash(b)

    def test_returns_16_char_hex(self) -> None:
        h = _compute_anomaly_hash(["test"])
        assert len(h) == 16
        int(h, 16)  # Should be valid hex


class TestExtractZScore:
    def test_extracts_positive(self) -> None:
        assert _extract_z_score("bcb_selic on 2024-01-01: value=10, z-score=3.14") == pytest.approx(3.14)

    def test_extracts_negative(self) -> None:
        assert _extract_z_score("bcb_usd_brl on 2020-01-01: value=2.8, z-score=-2.45") == pytest.approx(2.45)

    def test_malformed_returns_zero(self) -> None:
        assert _extract_z_score("no z-score here") == 0.0


class TestFormatAnomalyPromptData:
    def test_groups_by_series(self) -> None:
        anomalies = [
            "bcb_selic on 2024-01-01: value=10, z-score=2.50",
            "bcb_usd_brl on 2024-02-01: value=6, z-score=2.10",
            "bcb_selic on 2024-02-01: value=11, z-score=2.60",
        ]
        result = _format_anomaly_prompt_data(anomalies)
        assert "## bcb_usd_brl" in result
        assert "## bcb_selic" in result
        assert "Total anomalies detected: 3" in result

    def test_empty_anomalies(self) -> None:
        result = _format_anomaly_prompt_data([])
        assert "Total anomalies detected: 0" in result


class TestBuildSeriesDescriptions:
    def test_includes_all_series(self, domain_config: object) -> None:
        """Series descriptions should include all series from config."""
        from config import get_domain_config

        cfg = get_domain_config()
        result = _build_series_descriptions()
        for sid, series in cfg.series.items():
            assert series.label.en in result, f"Missing label: {series.label.en}"


class TestBuildAnomalyPrompt:
    def test_xml_fencing_structure(self, domain_config: object) -> None:
        from config.domain import DomainConfig
        from security.xml_fencing import build_anomaly_prompt

        cfg = DomainConfig.model_validate(domain_config) if not isinstance(domain_config, DomainConfig) else domain_config
        system, user = build_anomaly_prompt("anomaly data here", "series desc here", config=cfg)
        assert "<anomaly-data" in system
        assert "anomaly data here" in system
        assert "<series-descriptions>" in system
        assert "series desc here" in system
        assert "pt" in user
        assert "en" in user
