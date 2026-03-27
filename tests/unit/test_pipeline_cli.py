"""Tests for pipeline CLI argument parsing and stage/feed filtering."""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import patch

import pytest

from pipeline.__main__ import VALID_STAGES, build_stages, filter_feeds, parse_args


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_defaults(self) -> None:
        with patch.object(sys, "argv", ["pipeline"]):
            args = parse_args()
        assert args.stage == "all"
        assert args.feed == ""
        assert args.backfill is False
        assert args.list_feeds is False
        assert args.log_level == "debug"

    def test_stage_ingest(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "ingest"]):
            args = parse_args()
        assert args.stage == "ingest"

    def test_stage_transform(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "transform"]):
            args = parse_args()
        assert args.stage == "transform"

    def test_stage_quality_bronze(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "quality-bronze"]):
            args = parse_args()
        assert args.stage == "quality-bronze"

    def test_stage_quality_gold(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "quality-gold"]):
            args = parse_args()
        assert args.stage == "quality-gold"

    def test_stage_insight(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "insight"]):
            args = parse_args()
        assert args.stage == "insight"

    def test_invalid_stage_rejected(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "bogus"]):
            with pytest.raises(SystemExit):
                parse_args()

    def test_feed_single(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--feed", "bcb_selic"]):
            args = parse_args()
        assert args.feed == "bcb_selic"

    def test_feed_multiple(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--feed", "bcb_selic,ibge_pim"]):
            args = parse_args()
        assert args.feed == "bcb_selic,ibge_pim"

    def test_combined_stage_and_feed(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--stage", "ingest", "--feed", "bcb_selic"]):
            args = parse_args()
        assert args.stage == "ingest"
        assert args.feed == "bcb_selic"

    def test_list_feeds_flag(self) -> None:
        with patch.object(sys, "argv", ["pipeline", "--list-feeds"]):
            args = parse_args()
        assert args.list_feeds is True

    def test_all_valid_stages_accepted(self) -> None:
        for stage in VALID_STAGES:
            with patch.object(sys, "argv", ["pipeline", "--stage", stage]):
                args = parse_args()
            assert args.stage == stage


# ---------------------------------------------------------------------------
# filter_feeds
# ---------------------------------------------------------------------------

class TestFilterFeeds:
    def _sample_configs(self) -> dict[str, Any]:
        return {"bcb_selic": "cfg1", "bcb_cdi": "cfg2", "ibge_pim": "cfg3"}

    def test_empty_feed_arg_returns_all(self) -> None:
        configs = self._sample_configs()
        assert filter_feeds(configs, "") == configs

    def test_single_feed(self) -> None:
        configs = self._sample_configs()
        result = filter_feeds(configs, "bcb_selic")
        assert list(result.keys()) == ["bcb_selic"]

    def test_multiple_feeds(self) -> None:
        configs = self._sample_configs()
        result = filter_feeds(configs, "bcb_selic,ibge_pim")
        assert set(result.keys()) == {"bcb_selic", "ibge_pim"}

    def test_whitespace_trimmed(self) -> None:
        configs = self._sample_configs()
        result = filter_feeds(configs, " bcb_selic , ibge_pim ")
        assert set(result.keys()) == {"bcb_selic", "ibge_pim"}

    def test_unknown_feed_exits(self) -> None:
        configs = self._sample_configs()
        with pytest.raises(SystemExit):
            filter_feeds(configs, "nonexistent_feed")

    def test_partial_unknown_exits(self) -> None:
        configs = self._sample_configs()
        with pytest.raises(SystemExit):
            filter_feeds(configs, "bcb_selic,nonexistent_feed")


# ---------------------------------------------------------------------------
# build_stages
# ---------------------------------------------------------------------------

class TestBuildStages:
    def test_all_returns_five_stages(self) -> None:
        stages = build_stages("all", None, {}, "run-1", False)
        assert len(stages) == 5

    def test_ingest_returns_one_stage(self) -> None:
        stages = build_stages("ingest", None, {}, "run-1", False)
        assert len(stages) == 1
        assert stages[0].task_name == "ingestion"  # type: ignore[union-attr]

    def test_quality_bronze_returns_one_stage(self) -> None:
        stages = build_stages("quality-bronze", None, {}, "run-1", False)
        assert len(stages) == 1
        assert stages[0].task_name == "quality"  # type: ignore[union-attr]

    def test_transform_returns_one_stage(self) -> None:
        stages = build_stages("transform", None, {}, "run-1", False)
        assert len(stages) == 1
        assert stages[0].task_name == "transformation"  # type: ignore[union-attr]

    def test_quality_gold_returns_one_stage(self) -> None:
        stages = build_stages("quality-gold", None, {}, "run-1", False)
        assert len(stages) == 1
        assert stages[0].task_name == "quality"  # type: ignore[union-attr]

    def test_insight_returns_one_stage(self) -> None:
        stages = build_stages("insight", None, {}, "run-1", False)
        assert len(stages) == 1
        assert stages[0].agent_name == "insight"  # type: ignore[union-attr]

    def test_all_stages_order(self) -> None:
        stages = build_stages("all", None, {}, "run-1", False)
        names = []
        for s in stages:
            if hasattr(s, "task_name"):
                names.append(s.task_name)
            else:
                names.append(s.agent_name)  # type: ignore[union-attr]
        assert names == ["ingestion", "quality", "transformation", "quality", "insight"]

    def test_backfill_propagated_to_ingestion(self) -> None:
        stages = build_stages("ingest", None, {}, "run-1", True)
        assert stages[0]._backfill is True  # type: ignore[union-attr]

    def test_feed_configs_propagated(self) -> None:
        configs: dict[str, Any] = {"bcb_selic": "cfg"}
        stages = build_stages("ingest", None, configs, "run-1", False)
        assert stages[0]._feed_configs == configs  # type: ignore[union-attr]
