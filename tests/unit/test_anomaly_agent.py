"""Tests for AnomalyAgent — agents/anomaly/agent.py."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import anthropic.types as atypes
import pytest

from agents.anomaly.agent import (
    AnomalyAgent,
    _compute_anomaly_hash,
    _detect_anomalies_from_gold,
    _format_anomaly_descriptions,
    _format_anomaly_prompt_data,
    _parse_anomaly_sections,
)


# ---------------------------------------------------------------------------
# _detect_anomalies_from_gold
# ---------------------------------------------------------------------------


class TestDetectAnomaliesFromGold:
    def test_detects_high_z_score(self) -> None:
        """Rows with |z_score| > 2.0 should be flagged."""
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [
                {"date": "2024-01-01", "value": 13.75, "z_score": 0.5},
                {"date": "2024-02-01", "value": 13.75, "z_score": 2.5},
                {"date": "2024-03-01", "value": 13.75, "z_score": -2.1},
            ],
        }
        anomalies = _detect_anomalies_from_gold(series_data)
        assert len(anomalies) == 2
        assert anomalies[0]["z_score"] == 2.5
        assert anomalies[1]["z_score"] == -2.1

    def test_no_anomalies_below_threshold(self) -> None:
        """Rows with |z_score| <= 2.0 should not be flagged."""
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [
                {"date": "2024-01-01", "value": 13.75, "z_score": 0.5},
                {"date": "2024-02-01", "value": 13.75, "z_score": 1.9},
                {"date": "2024-03-01", "value": 13.75, "z_score": -1.8},
            ],
        }
        anomalies = _detect_anomalies_from_gold(series_data)
        assert len(anomalies) == 0

    def test_null_z_score_ignored(self) -> None:
        """Rows with None z_score should be skipped."""
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [
                {"date": "2024-01-01", "value": 13.75, "z_score": None},
                {"date": "2024-02-01", "value": 13.75},
            ],
        }
        anomalies = _detect_anomalies_from_gold(series_data)
        assert len(anomalies) == 0

    def test_multiple_series(self) -> None:
        """Anomalies across multiple series are collected."""
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [
                {"date": "2024-01-01", "value": 13.75, "z_score": 3.0},
            ],
            "bcb_ipca": [
                {"date": "2024-01-01", "value": 0.5, "z_score": 2.5},
            ],
            "bcb_usd_brl": [
                {"date": "2024-01-01", "value": 5.0, "z_score": 1.0},
            ],
        }
        anomalies = _detect_anomalies_from_gold(series_data)
        assert len(anomalies) == 2
        series_names = {a["series"] for a in anomalies}
        assert "bcb_selic" in series_names
        assert "bcb_ipca" in series_names

    def test_exactly_at_threshold_not_flagged(self) -> None:
        """z_score of exactly 2.0 should NOT be flagged (> not >=)."""
        series_data: dict[str, list[dict[str, Any]]] = {
            "bcb_selic": [
                {"date": "2024-01-01", "value": 13.75, "z_score": 2.0},
                {"date": "2024-02-01", "value": 13.75, "z_score": -2.0},
            ],
        }
        anomalies = _detect_anomalies_from_gold(series_data)
        assert len(anomalies) == 0


# ---------------------------------------------------------------------------
# _format_anomaly_descriptions
# ---------------------------------------------------------------------------


class TestFormatAnomalyDescriptions:
    def test_formats_correctly(self) -> None:
        anomalies = [
            {"series": "bcb_selic", "date": "2024-01-01", "value": 13.75, "z_score": 3.0},
        ]
        descriptions = _format_anomaly_descriptions(anomalies)
        assert len(descriptions) == 1
        assert "bcb_selic" in descriptions[0]
        assert "3.00" in descriptions[0]


# ---------------------------------------------------------------------------
# _compute_anomaly_hash
# ---------------------------------------------------------------------------


class TestComputeAnomalyHash:
    def test_deterministic(self) -> None:
        descs = ["a on 2024-01-01: value=1, z-score=2.5", "b on 2024-02-01: value=2, z-score=3.0"]
        h1 = _compute_anomaly_hash(descs)
        h2 = _compute_anomaly_hash(descs)
        assert h1 == h2

    def test_order_independent(self) -> None:
        """Hash should be the same regardless of input order."""
        descs_a = ["a: z=2.5", "b: z=3.0"]
        descs_b = ["b: z=3.0", "a: z=2.5"]
        assert _compute_anomaly_hash(descs_a) == _compute_anomaly_hash(descs_b)

    def test_different_input_different_hash(self) -> None:
        h1 = _compute_anomaly_hash(["a: z=2.5"])
        h2 = _compute_anomaly_hash(["b: z=3.0"])
        assert h1 != h2


# ---------------------------------------------------------------------------
# _format_anomaly_prompt_data
# ---------------------------------------------------------------------------


class TestFormatAnomalyPromptData:
    def test_groups_by_series(self) -> None:
        anomalies = [
            {"series": "bcb_selic", "date": "2024-01-01", "value": 13.75, "z_score": 3.0},
            {"series": "bcb_selic", "date": "2024-02-01", "value": 14.0, "z_score": 2.5},
            {"series": "bcb_ipca", "date": "2024-01-01", "value": 0.8, "z_score": 2.1},
        ]
        text = _format_anomaly_prompt_data(anomalies)
        assert "## bcb_selic" in text
        assert "## bcb_ipca" in text
        assert "Total anomalies detected: 3" in text


# ---------------------------------------------------------------------------
# _parse_anomaly_sections
# ---------------------------------------------------------------------------


class TestParseAnomalySections:
    def test_parses_bilingual_sections(self) -> None:
        raw = "<pt>Análise em português</pt>\n<en>Analysis in English</en>"
        now = datetime.now(timezone.utc)
        records = _parse_anomaly_sections(raw, "run-1", now, ["bcb_selic"], "hash123")
        assert len(records) == 2
        pt = [r for r in records if r.language == "pt"][0]
        en = [r for r in records if r.language == "en"][0]
        assert "português" in pt.content
        assert "English" in en.content
        assert pt.insight_type == "anomaly"
        assert pt.anomaly_hash == "hash123"

    def test_fallback_to_en_when_no_tags(self) -> None:
        raw = "Just plain text analysis"
        now = datetime.now(timezone.utc)
        records = _parse_anomaly_sections(raw, "run-1", now, [], "hash")
        assert len(records) == 1
        assert records[0].language == "en"
        assert records[0].insight_type == "anomaly"


# ---------------------------------------------------------------------------
# AnomalyAgent integration tests
# ---------------------------------------------------------------------------


class TestAnomalyAgent:
    @pytest.mark.asyncio()
    async def test_skips_when_no_anomalies(self) -> None:
        """Agent returns success with no Claude call when z_scores are low."""
        agent = AnomalyAgent()

        mock_rows: list[dict[str, Any]] = [
            {"date": "2024-01-01", "value": 10.0, "z_score": 0.5},
            {"date": "2024-02-01", "value": 10.5, "z_score": 0.3},
        ]

        with (
            patch(
                "agents.anomaly.agent.get_all_series_ids",
                return_value=["bcb_selic"],
            ),
            patch(
                "agents.anomaly.agent.query_gold_series",
                new_callable=AsyncMock,
                return_value=mock_rows,
            ),
        ):
            result = await agent.run()

        assert result.success
        assert any("No anomalies" in w for w in result.warnings)

    @pytest.mark.asyncio()
    async def test_skips_when_no_gold_data(self) -> None:
        """Agent returns success when no gold data available."""
        agent = AnomalyAgent()

        with (
            patch(
                "agents.anomaly.agent.get_all_series_ids",
                return_value=["bcb_selic"],
            ),
            patch(
                "agents.anomaly.agent.query_gold_series",
                new_callable=AsyncMock,
                return_value=[],
            ),
        ):
            result = await agent.run()

        assert result.success
        assert any("No gold data" in w for w in result.warnings)

    @pytest.mark.asyncio()
    async def test_detects_and_skips_dedup(self) -> None:
        """Agent detects anomalies but skips due to dedup hash match."""
        agent = AnomalyAgent()

        mock_rows: list[dict[str, Any]] = [
            {"date": "2024-01-01", "value": 10.0, "z_score": 3.0},
            {"date": "2024-02-01", "value": 10.5, "z_score": 0.3},
        ]

        with (
            patch(
                "agents.anomaly.agent.get_all_series_ids",
                return_value=["bcb_selic"],
            ),
            patch(
                "agents.anomaly.agent.query_gold_series",
                new_callable=AsyncMock,
                return_value=mock_rows,
            ),
            patch.object(
                agent, "_anomaly_hash_exists",
                new_callable=AsyncMock,
                return_value=True,
            ),
        ):
            result = await agent.run()

        assert result.success
        assert any("already analyzed" in w for w in result.warnings)

    @pytest.mark.asyncio()
    async def test_calls_claude_and_stores(self) -> None:
        """Agent calls Claude and stores results when anomalies found."""
        agent = AnomalyAgent()

        mock_rows: list[dict[str, Any]] = [
            {"date": "2024-01-01", "value": 10.0, "z_score": 3.5},
        ]

        mock_response = MagicMock()
        mock_response.content = [
            atypes.TextBlock(
                type="text",
                text="<pt>Anomalia detectada</pt>\n<en>Anomaly detected</en>",
            )
        ]

        with (
            patch(
                "agents.anomaly.agent.get_all_series_ids",
                return_value=["bcb_selic"],
            ),
            patch(
                "agents.anomaly.agent.query_gold_series",
                new_callable=AsyncMock,
                return_value=mock_rows,
            ),
            patch.object(
                agent, "_anomaly_hash_exists",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}),
            patch(
                "agents.anomaly.agent.anthropic.AsyncAnthropic",
            ) as mock_anthropic_cls,
            patch.object(
                agent, "_store_insights",
                new_callable=AsyncMock,
            ) as mock_store,
        ):
            mock_client = AsyncMock()
            mock_client.messages.create = AsyncMock(return_value=mock_response)
            mock_anthropic_cls.return_value = mock_client

            result = await agent.run()

        assert result.success
        mock_store.assert_awaited_once()
        stored_records = mock_store.call_args[0][0]
        assert len(stored_records) == 2  # PT + EN
        assert all(r.insight_type == "anomaly" for r in stored_records)

    @pytest.mark.asyncio()
    async def test_fails_without_api_key(self) -> None:
        """Agent fails gracefully when ANTHROPIC_API_KEY is not set."""
        agent = AnomalyAgent()

        mock_rows: list[dict[str, Any]] = [
            {"date": "2024-01-01", "value": 10.0, "z_score": 3.5},
        ]

        with (
            patch(
                "agents.anomaly.agent.get_all_series_ids",
                return_value=["bcb_selic"],
            ),
            patch(
                "agents.anomaly.agent.query_gold_series",
                new_callable=AsyncMock,
                return_value=mock_rows,
            ),
            patch.object(
                agent, "_anomaly_hash_exists",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False),
        ):
            result = await agent.run()

        assert not result.success
        assert any("ANTHROPIC_API_KEY" in e for e in result.errors)

    @pytest.mark.asyncio()
    async def test_agent_name(self) -> None:
        agent = AnomalyAgent()
        assert agent.agent_name == "anomaly"
