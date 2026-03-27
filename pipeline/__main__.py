"""Entry point for running the pipeline: uv run python -m pipeline.flow"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timezone

import structlog
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

from api.models import PipelineStage  # noqa: E402
from pipeline.feed_config import load_feed_configs  # noqa: E402
from pipeline.flow import PipelineFlow  # noqa: E402
from storage import get_storage_backend  # noqa: E402
from agents.base import BaseAgent  # noqa: E402
from agents.insight.agent import InsightAgent  # noqa: E402
from tasks.base import BaseTask  # noqa: E402
from tasks.ingestion.task import IngestionTask  # noqa: E402
from tasks.quality.task import QualityTask  # noqa: E402
from tasks.transformation.task import TransformationTask  # noqa: E402

logger = structlog.get_logger()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Veredas pipeline")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Fetch full historical data instead of daily delta",
    )
    return parser.parse_args()


async def main() -> int:
    """Run the full pipeline: Ingest → Quality → Transform → Quality → Insight."""
    args = parse_args()
    storage = get_storage_backend()
    feed_configs = load_feed_configs()
    run_id = f"pipeline-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    if not feed_configs:
        logger.error("no_feed_configs", msg="No active feed configs found")
        return 1

    if args.backfill:
        logger.info("pipeline_backfill_mode", feeds=list(feed_configs.keys()))

    stages: list[BaseTask | BaseAgent] = [
        IngestionTask(
            storage=storage,
            feed_configs=feed_configs,
            run_id=run_id,
            backfill=args.backfill,
        ),
        QualityTask(storage=storage, stage=PipelineStage.POST_INGESTION, feed_configs=feed_configs),
        TransformationTask(storage=storage, feed_configs=feed_configs),
        QualityTask(storage=storage, stage=PipelineStage.POST_TRANSFORMATION, feed_configs=feed_configs),
        InsightAgent(),
    ]

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
