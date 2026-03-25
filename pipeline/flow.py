"""PipelineFlow — orchestrates Tasks and Agents in sequence."""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import structlog

from agents.base import BaseAgent
from api.models import (
    AgentResult,
    PipelineRunResult,
    RunManifest,
    StageDetail,
    TaskResult,
)
from tasks.base import BaseTask

if TYPE_CHECKING:
    from storage.protocol import StorageBackend

logger = structlog.get_logger()


class PipelineFlow:
    """Runs Tasks and Agents in sequence. Halts on failure."""

    def __init__(
        self,
        storage: StorageBackend | None = None,
        trigger: str = "local",
    ) -> None:
        self._storage = storage
        self._trigger = trigger

    async def run(self, stages: Sequence[BaseTask | BaseAgent]) -> PipelineRunResult:
        run_id = f"pipeline-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
        started_at = datetime.now(timezone.utc)
        start = time.perf_counter()
        completed: list[str] = []
        failed: list[str] = []
        results: list[TaskResult | AgentResult] = []
        stage_details: list[StageDetail] = []

        logger.info("pipeline_started", run_id=run_id, total_stages=len(stages))

        for stage in stages:
            name = stage.task_name if isinstance(stage, BaseTask) else stage.agent_name
            try:
                result = await stage.run()
                results.append(result)

                # Collect reconciliation data if available
                recon = getattr(result, "series_reconciliation", [])
                stage_details.append(StageDetail(
                    stage_name=name,
                    duration_ms=result.duration_ms,
                    rows_written=result.rows_processed,
                    errors=result.errors,
                    series_reconciliation=recon,
                ))

                if result.success:
                    completed.append(name)
                else:
                    failed.append(name)
                    logger.error(
                        "pipeline_stage_failed",
                        run_id=run_id,
                        stage=name,
                        errors=result.errors,
                    )
                    break  # Halt pipeline
            except Exception as exc:
                logger.error(
                    "pipeline_stage_exception",
                    run_id=run_id,
                    stage=name,
                    error=str(exc),
                )
                failed.append(name)
                stage_details.append(StageDetail(
                    stage_name=name,
                    errors=[str(exc)],
                ))
                break

        total_ms = round((time.perf_counter() - start) * 1000, 2)
        success = len(failed) == 0
        finished_at = datetime.now(timezone.utc)

        # Determine manifest status
        if success:
            status = "success"
        elif completed:
            status = "partial"
        else:
            status = "failed"

        # Write manifest to storage
        if self._storage is not None:
            try:
                manifest = RunManifest(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    status=status,
                    trigger=self._trigger,
                    stages=stage_details,
                )
                await self._storage.write(
                    f"runs/{run_id}/manifest.json",
                    manifest.model_dump_json(indent=2).encode(),
                )
                logger.info("manifest_written", run_id=run_id, status=status)
            except Exception as exc:
                logger.warning("manifest_write_error", run_id=run_id, error=str(exc))

        logger.info(
            "pipeline_completed",
            run_id=run_id,
            success=success,
            completed=completed,
            failed=failed,
            total_duration_ms=total_ms,
        )

        return PipelineRunResult(
            run_id=run_id,
            success=success,
            stages_completed=completed,
            stages_failed=failed,
            total_duration_ms=total_ms,
            results=results,
        )


if __name__ == "__main__":
    # Support `python -m pipeline.flow` by delegating to __main__
    from pipeline.__main__ import main as _main

    import asyncio
    import sys

    sys.exit(asyncio.run(_main()))
