"""Entry point for running the pipeline: uv run python -m pipeline.flow"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import Any

import structlog
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from api.models import PipelineStage  # noqa: E402
from pipeline.feed_config import load_feed_configs  # noqa: E402
from pipeline.flow import PipelineFlow  # noqa: E402
from storage import get_storage_backend  # noqa: E402
from agents.base import BaseAgent  # noqa: E402
from agents.anomaly.agent import AnomalyAgent  # noqa: E402
from agents.insight.agent import InsightAgent  # noqa: E402
from tasks.base import BaseTask  # noqa: E402
from tasks.ingestion.task import IngestionTask  # noqa: E402
from tasks.quality.task import QualityTask  # noqa: E402
from tasks.cross_series.task import CrossSeriesTask  # noqa: E402
from tasks.transformation.task import TransformationTask  # noqa: E402

logger = structlog.get_logger()

VALID_STAGES = [
    "ingest", "quality-bronze", "transform", "cross-series",
    "quality-gold", "insight", "anomaly", "all",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Veredas pipeline")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Fetch full historical data instead of daily delta",
    )
    parser.add_argument(
        "--log-level",
        default="debug",
        choices=["debug", "info", "warning", "error"],
        help="Minimum log level (default: debug)",
    )
    parser.add_argument(
        "--stage",
        default="all",
        choices=VALID_STAGES,
        help="Pipeline stage to run (default: all)",
    )
    parser.add_argument(
        "--feed",
        default="",
        help="Comma-separated feed IDs to process (default: all active feeds)",
    )
    parser.add_argument(
        "--list-feeds",
        action="store_true",
        help="List all active feed IDs and exit",
    )
    return parser.parse_args()


def filter_feeds(
    feed_configs: dict[str, Any],
    feed_arg: str,
) -> dict[str, Any]:
    """Filter feed_configs to only the requested feed IDs.

    Raises SystemExit if any requested feed ID is not found.
    """
    if not feed_arg:
        return feed_configs

    requested = [f.strip() for f in feed_arg.split(",") if f.strip()]
    unknown = [f for f in requested if f not in feed_configs]
    if unknown:
        print(f"Error: unknown feed IDs: {', '.join(unknown)}", file=sys.stderr)
        print(f"Available: {', '.join(sorted(feed_configs.keys()))}", file=sys.stderr)
        sys.exit(1)

    return {k: v for k, v in feed_configs.items() if k in requested}


def build_stages(
    stage: str,
    storage: Any,
    feed_configs: dict[str, Any],
    run_id: str,
    backfill: bool,
) -> list[BaseTask | BaseAgent]:
    """Build the list of pipeline stages based on --stage flag."""
    all_stages: dict[str, BaseTask | BaseAgent] = {
        "ingest": IngestionTask(
            storage=storage,
            feed_configs=feed_configs,
            run_id=run_id,
            backfill=backfill,
        ),
        "quality-bronze": QualityTask(
            storage=storage,
            stage=PipelineStage.POST_INGESTION,
            feed_configs=feed_configs,
        ),
        "transform": TransformationTask(
            storage=storage,
            feed_configs=feed_configs,
        ),
        "cross-series": CrossSeriesTask(storage=storage),
        "quality-gold": QualityTask(
            storage=storage,
            stage=PipelineStage.POST_TRANSFORMATION,
            feed_configs=feed_configs,
        ),
        "insight": InsightAgent(),
        "anomaly": AnomalyAgent(),
    }

    if stage == "all":
        return list(all_stages.values())

    return [all_stages[stage]]


async def main() -> int:
    """Run the pipeline with optional stage and feed filtering."""
    args = parse_args()

    level = getattr(logging, args.log_level.upper())
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(level))

    feed_configs = load_feed_configs()

    if not feed_configs:
        logger.error("no_feed_configs", msg="No active feed configs found")
        return 1

    if args.list_feeds:
        for feed_id in sorted(feed_configs.keys()):
            print(feed_id)
        return 0

    feed_configs = filter_feeds(feed_configs, args.feed)

    storage = get_storage_backend()
    run_id = f"pipeline-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    if args.backfill:
        logger.info("pipeline_backfill_mode", feeds=list(feed_configs.keys()))

    if args.feed:
        logger.info("pipeline_feed_filter", feeds=list(feed_configs.keys()))
    if args.stage != "all":
        logger.info("pipeline_stage_filter", stage=args.stage)

    stages = build_stages(args.stage, storage, feed_configs, run_id, args.backfill)

    flow = PipelineFlow(storage=storage, trigger="local")
    result = await flow.run(stages)

    print(f"\nPipeline run: {result.run_id}")
    print(f"Success: {result.success}")
    print(f"Duration: {result.total_duration_ms:.0f}ms")
    print(f"Stages completed: {result.stages_completed}")
    if result.stages_failed:
        print(f"Stages failed: {result.stages_failed}")
    for r in result.results:
        name = r.task_name if hasattr(r, "task_name") else r.agent_name  # type: ignore[union-attr]
        print(f"  {name}: rows={r.rows_processed}, success={r.success}")
        if r.warnings:
            for w in r.warnings:
                print(f"    warn: {w}")
        if r.errors:
            for e in r.errors:
                print(f"    error: {e}")

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
