"""Pydantic models for all API request/response schemas."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


# --- Enums ---


class TaskStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class QualityLevel(str, Enum):
    PASSED = "passed"
    WARNING = "warning"
    CRITICAL = "critical"


class FreshnessStatus(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    CRITICAL = "critical"


class PipelineStage(str, Enum):
    POST_INGESTION = "post_ingestion"
    POST_TRANSFORMATION = "post_transformation"


# --- Task / Agent Result Models ---


class TaskResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    success: bool
    task_name: str
    duration_ms: float
    rows_processed: int = 0
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class AgentResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    success: bool
    agent_name: str
    duration_ms: float
    rows_processed: int = 0
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


# --- Quality Models ---


class QualityCheckResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    check_name: str
    passed: bool
    metric_value: float | None = None
    threshold: float | None = None
    message: str = ""


class SeriesFreshness(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    series: str
    last_updated: datetime | None = None
    status: FreshnessStatus = FreshnessStatus.CRITICAL
    hours_since_update: float | None = None


class QualityReport(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    run_id: str
    stage: PipelineStage
    timestamp: datetime
    overall_status: QualityLevel
    checks: list[QualityCheckResult] = Field(default_factory=list)
    series_freshness: list[SeriesFreshness] = Field(default_factory=list)
    critical_failures: list[str] = Field(default_factory=list)


# --- API Response Models ---


class HealthResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    status: str
    timestamp: datetime
    sync: SyncInfo | None = None
    data_freshness: dict[str, str] = Field(default_factory=dict)


class SyncInfo(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    last_sync_at: datetime | None = None
    run_id: str | None = None
    files_synced: int = 0
    sync_duration_ms: float = 0.0
    source: str = ""


class SyncStatusResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    last_sync_at: datetime | None = None
    run_id: str | None = None
    files_synced: int = 0
    sync_duration_ms: float = 0.0
    source: str = ""
    seconds_since_sync: float | None = None
    sync_health: FreshnessStatus = FreshnessStatus.CRITICAL


class MetricDataPoint(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    date: datetime
    value: float
    series: str
    unit: str = ""


class MetricsResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    series: str
    data_points: list[MetricDataPoint] = Field(default_factory=list)
    last_updated: datetime | None = None


# --- Sync Webhook ---


class SyncResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    success: bool
    files_synced: int = 0
    sync_duration_ms: float = 0.0
    errors: list[str] = Field(default_factory=list)


# --- Pipeline Models ---


class PipelineRunResult(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    run_id: str
    success: bool
    stages_completed: list[str] = Field(default_factory=list)
    stages_failed: list[str] = Field(default_factory=list)
    total_duration_ms: float = 0.0
    results: list[TaskResult | AgentResult] = Field(default_factory=list)


# --- Feed Config Models (Data Contracts) ---


class FeedStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"


class SourceFormat(str, Enum):
    JSON = "json"
    CSV = "csv"


class SilverProcessingType(str, Enum):
    APPEND = "append"
    MERGE_BY_KEY = "merge_by_key"
    LATEST_ONLY = "latest_only"


class FeedSourceConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    type: str = "api"
    url: str
    format: SourceFormat
    auth_method: str = "none"
    rate_limit_rpm: int = 30
    json_data_path: str | None = None
    csv_separator: str = ";"
    csv_header_row: int = 0
    skip_rows: int = 0
    backfill_url: str | None = None
    backfill_window_years: int | None = None
    backfill_start_date: str | None = None


class FeedFieldDefinition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str
    source_field: str
    type: str = "string"
    required: bool = False
    description: str = ""
    silver_type: str | None = None
    silver_expression: str | None = None


class FeedScheduleConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    cron: str = "0 6 * * *"
    timezone: str = "UTC"
    retry_attempts: int = 3
    retry_delay_seconds: int = 60


class BronzeProcessingConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    write_mode: Literal["append"] = "append"
    rescued_data_column: str = "_rescued_data"


class SilverProcessingConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    processing_type: SilverProcessingType = SilverProcessingType.LATEST_ONLY
    primary_key: list[str] = Field(default_factory=list)
    dedup_columns: list[str] = Field(default_factory=list)
    dedup_order_by: str = "_ingested_at DESC"
    pre_filter: str | None = None


class FeedProcessingConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    bronze: BronzeProcessingConfig = Field(default_factory=BronzeProcessingConfig)
    silver: SilverProcessingConfig = Field(default_factory=SilverProcessingConfig)


class QualityRuleConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    max_null_rate: float = 0.02
    min_row_count: int = 1
    max_rescued_data_rate: float | None = None
    value_range_min: float | None = None
    value_range_max: float | None = None
    freshness_hours: float | None = None


class FeedQualityConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    bronze: QualityRuleConfig = Field(default_factory=QualityRuleConfig)
    silver: QualityRuleConfig | None = None
    gold: QualityRuleConfig | None = None


class FeedMetadataConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    unit: str = ""
    tags: list[str] = Field(default_factory=list)
    domain: str = ""


class FeedConfig(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    feed_id: str
    name: str
    version: str = "1.0.0"
    status: FeedStatus = FeedStatus.ACTIVE
    source: FeedSourceConfig
    schedule: FeedScheduleConfig = Field(default_factory=FeedScheduleConfig)
    schema_fields: list[FeedFieldDefinition] = Field(default_factory=list)
    processing: FeedProcessingConfig = Field(default_factory=FeedProcessingConfig)
    quality: FeedQualityConfig = Field(default_factory=FeedQualityConfig)
    metadata: FeedMetadataConfig = Field(default_factory=FeedMetadataConfig)


# --- Query Models ---


class QueryTier(str, Enum):
    DIRECT_LOOKUP = "direct_lookup"
    FULL_LLM = "full_llm"


class DataPoint(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    series: str
    value: float
    date: datetime


class QueryRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    question: str = Field(..., max_length=500)
    language: str = "en"


class QueryResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    answer: str
    data_points: list[DataPoint] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    tier_used: QueryTier = QueryTier.FULL_LLM
    llm_tokens_used: int = 0
    llm_input_tokens: int = 0
    llm_output_tokens: int = 0


# --- Insight Models ---


class InsightRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    content: str
    language: str
    metric_refs: list[str] = Field(default_factory=list)
    model_version: str = ""
    run_id: str = ""
    generated_at: datetime
    confidence_flag: bool = True


class InsightResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    insights: list[InsightRecord] = Field(default_factory=list)
    latest_run_id: str | None = None


# --- Silver Watermark ---


class SilverWatermark(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    last_processed_key: str
    last_processed_at: datetime


# Rebuild HealthResponse to resolve forward reference
HealthResponse.model_rebuild()
