"""Tests for PipelineFlow."""

from __future__ import annotations

from pathlib import Path

import pytest

from api.models import AgentResult, RunManifest, TaskResult
from pipeline.flow import PipelineFlow
from agents.base import BaseAgent
from storage.local import LocalStorageBackend
from tasks.base import BaseTask


class MockTask(BaseTask):
    def __init__(self, name: str = "mock_task", should_fail: bool = False) -> None:
        self._name = name
        self._should_fail = should_fail

    @property
    def task_name(self) -> str:
        return self._name

    async def _execute(self) -> TaskResult:
        if self._should_fail:
            raise RuntimeError(f"{self._name} intentional failure")
        return TaskResult(
            success=True,
            task_name=self._name,
            duration_ms=0.0,
            rows_processed=10,
        )


class MockAgent(BaseAgent):
    def __init__(self, name: str = "mock_agent", should_fail: bool = False) -> None:
        self._name = name
        self._should_fail = should_fail

    @property
    def agent_name(self) -> str:
        return self._name

    async def _execute(self) -> AgentResult:
        if self._should_fail:
            raise RuntimeError(f"{self._name} intentional failure")
        return AgentResult(
            success=True,
            agent_name=self._name,
            duration_ms=0.0,
            rows_processed=5,
        )


@pytest.mark.asyncio
async def test_pipeline_all_stages_succeed() -> None:
    """Pipeline should run all stages when all succeed."""
    flow = PipelineFlow()
    result = await flow.run([
        MockTask("task_1"),
        MockTask("task_2"),
        MockAgent("agent_1"),
    ])

    assert result.success is True
    assert len(result.stages_completed) == 3
    assert len(result.stages_failed) == 0
    assert result.total_duration_ms > 0


@pytest.mark.asyncio
async def test_pipeline_halts_on_failure() -> None:
    """Pipeline should halt on task failure and not run subsequent stages."""
    flow = PipelineFlow()
    result = await flow.run([
        MockTask("task_1"),
        MockTask("task_2", should_fail=True),
        MockTask("task_3"),  # Should NOT run
    ])

    assert result.success is False
    assert "task_1" in result.stages_completed
    assert "task_2" in result.stages_failed
    assert "task_3" not in result.stages_completed


@pytest.mark.asyncio
async def test_pipeline_empty_stages() -> None:
    """Pipeline with no stages should succeed."""
    flow = PipelineFlow()
    result = await flow.run([])

    assert result.success is True
    assert len(result.stages_completed) == 0


@pytest.mark.asyncio
async def test_pipeline_records_all_results() -> None:
    """Pipeline should record results from all executed stages."""
    flow = PipelineFlow()
    result = await flow.run([
        MockTask("task_1"),
        MockAgent("agent_1"),
    ])

    assert len(result.results) == 2


@pytest.mark.asyncio
async def test_pipeline_run_id_generated() -> None:
    """Pipeline should generate a unique run_id."""
    flow = PipelineFlow()
    result = await flow.run([MockTask("task_1")])

    assert result.run_id.startswith("pipeline-")
    assert len(result.run_id) > len("pipeline-")


@pytest.mark.asyncio
async def test_pipeline_writes_manifest(tmp_path: Path) -> None:
    """Pipeline with storage writes manifest.json to runs/{run_id}/."""
    storage = LocalStorageBackend(tmp_path)
    flow = PipelineFlow(storage=storage, trigger="local")
    result = await flow.run([MockTask("task_1"), MockAgent("agent_1")])

    manifest_key = f"runs/{result.run_id}/manifest.json"
    assert await storage.exists(manifest_key)

    raw = await storage.read(manifest_key)
    manifest = RunManifest.model_validate_json(raw)
    assert manifest.run_id == result.run_id
    assert manifest.status == "success"
    assert manifest.trigger == "local"
    assert len(manifest.stages) == 2
    assert manifest.stages[0].stage_name == "task_1"
    assert manifest.stages[0].rows_written == 10
    assert manifest.stages[1].stage_name == "agent_1"


@pytest.mark.asyncio
async def test_pipeline_manifest_on_failure(tmp_path: Path) -> None:
    """Pipeline failure still writes manifest with status 'partial'."""
    storage = LocalStorageBackend(tmp_path)
    flow = PipelineFlow(storage=storage)
    result = await flow.run([
        MockTask("task_1"),
        MockTask("task_2", should_fail=True),
        MockTask("task_3"),
    ])

    manifest_key = f"runs/{result.run_id}/manifest.json"
    assert await storage.exists(manifest_key)

    raw = await storage.read(manifest_key)
    manifest = RunManifest.model_validate_json(raw)
    assert manifest.status == "partial"
    # task_1 succeeded, task_2 exception, task_3 never ran
    assert len(manifest.stages) == 2
    assert manifest.stages[1].stage_name == "task_2"
    assert len(manifest.stages[1].errors) > 0


@pytest.mark.asyncio
async def test_pipeline_no_storage_no_crash() -> None:
    """Pipeline without storage should still work (backward compat)."""
    flow = PipelineFlow()  # no storage
    result = await flow.run([MockTask("task_1")])
    assert result.success is True
