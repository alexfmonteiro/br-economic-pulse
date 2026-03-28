const API_BASE = import.meta.env.VITE_API_URL ?? '';

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
  return res.json() as Promise<T>;
}

// --- Types ---

export interface MetricDataPoint {
  date: string;
  value: number;
  series: string;
  unit?: string;
}

export interface MetricsResponse {
  series: string;
  data_points: MetricDataPoint[];
  last_updated: string | null;
}

export interface HealthResponse {
  status: string;
  timestamp: string;
  sync: {
    last_sync_at: string | null;
    sync_health: string;
  } | null;
  data_freshness: Record<string, string>;
}

export interface QualityCheckResult {
  check_name: string;
  passed: boolean;
  metric_value: number | null;
  threshold: number | null;
  message: string;
}

export interface SeriesFreshnessData {
  series: string;
  last_updated: string | null;
  status: string;
  hours_since_update: number | null;
  last_ingested_at: string | null;
}

export interface QualityReportData {
  run_id: string;
  stage: string;
  timestamp: string;
  overall_status: string;
  checks: QualityCheckResult[];
  series_freshness: SeriesFreshnessData[];
  critical_failures: string[];
}

export interface QualityLatest {
  status: string;
  sync_health: string;
  last_sync: {
    last_sync_at: string;
    run_id: string | null;
    files_synced: number;
  } | null;
  report: QualityReportData | null;
  series_freshness: SeriesFreshnessData[];
}

// --- Time Ranges ---

export type TimeRange = 'MTD' | 'YTD' | '1Y' | '2Y' | '5Y' | '10Y' | '20Y' | 'ALL';

export function getAfterDate(range: TimeRange): string | null {
  if (range === 'ALL') return null;

  const now = new Date();
  let d: Date;

  switch (range) {
    case 'MTD':
      d = new Date(now.getFullYear(), now.getMonth(), 1);
      break;
    case 'YTD':
      d = new Date(now.getFullYear(), 0, 1);
      break;
    case '1Y':
      d = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
      break;
    case '2Y':
      d = new Date(now.getFullYear() - 2, now.getMonth(), now.getDate());
      break;
    case '5Y':
      d = new Date(now.getFullYear() - 5, now.getMonth(), now.getDate());
      break;
    case '10Y':
      d = new Date(now.getFullYear() - 10, now.getMonth(), now.getDate());
      break;
    case '20Y':
      d = new Date(now.getFullYear() - 20, now.getMonth(), now.getDate());
      break;
  }

  return d.toISOString().split('T')[0];
}

export const TIME_RANGES: { value: TimeRange; label: string }[] = [
  { value: 'MTD', label: 'MTD' },
  { value: 'YTD', label: 'YTD' },
  { value: '1Y', label: '1Y' },
  { value: '2Y', label: '2Y' },
  { value: '5Y', label: '5Y' },
  { value: '10Y', label: '10Y' },
  { value: '20Y', label: '20Y' },
  { value: 'ALL', label: 'All' },
];

// --- Query types ---

export interface QueryRequest {
  question: string;
}

export interface DataPointResponse {
  series: string;
  value: number;
  date: string;
}

export interface QueryResponse {
  answer: string;
  data_points: DataPointResponse[];
  sources: string[];
  tier_used: 'direct_lookup' | 'full_llm';
  llm_tokens_used: number;
}

// --- Insight types ---

export interface InsightRecord {
  content: string;
  language: string;
  metric_refs: string[];
  model_version: string;
  run_id: string;
  generated_at: string;
  confidence_flag: boolean;
  insight_type?: string;
  anomaly_hash?: string | null;
}

export interface InsightResponse {
  insights: InsightRecord[];
  latest_run_id: string | null;
}

// --- Sync status ---

export interface SyncStatusResponse {
  last_sync_at: string | null;
  run_id: string | null;
  files_synced: number;
  sync_duration_ms: number;
  source: string;
  seconds_since_sync: number | null;
  sync_health: string;
}

// --- Series label helper (fallback, prefer domain config) ---

export function getSeriesLabel(seriesId: string): string {
  return seriesId;
}

// --- Fetchers ---

export const fetchMetrics = (series: string, after?: string | null): Promise<MetricsResponse> => {
  const params = after ? `?after=${after}` : '';
  return fetchJSON(`/api/metrics/${series}${params}`);
};

export const fetchHealth = (): Promise<HealthResponse> =>
  fetchJSON('/api/health');

export const fetchQualityLatest = (): Promise<QualityLatest> =>
  fetchJSON('/api/quality/latest');

export const fetchInsightsLatest = (): Promise<InsightResponse> =>
  fetchJSON('/api/insights/latest');

export const fetchAnomalyInsights = (): Promise<InsightResponse> =>
  fetchJSON('/api/insights/anomalies');

export const fetchSyncStatus = (): Promise<SyncStatusResponse> =>
  fetchJSON('/api/quality/sync-status');

export async function postQuery(question: string, language: string = 'en'): Promise<QueryResponse> {
  const res = await fetch(`${API_BASE}/api/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify({ question, language }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as Record<string, string>).detail ?? `API ${res.status}`);
  }
  return res.json() as Promise<QueryResponse>;
}

// --- Series Config ---

export interface SeriesConfig {
  id: string;
  label: string;
  unit: string;
  source: string;
  color: string;
  /** Hours without a new data point before the series is considered stale.
   *  Stale = 1× threshold, Critical = 2× threshold. */
  freshnessHours: number;
}

// --- Run History types ---

export interface SeriesReconciliation {
  series_id: string;
  rows_in: number;
  rows_out: number;
  rows_quarantined: number;
  rows_rescued: number;
}

export interface StageDetail {
  stage_name: string;
  duration_ms: number;
  rows_read: number;
  rows_written: number;
  rows_quarantined: number;
  rows_rescued: number;
  errors: string[];
  series_reconciliation: SeriesReconciliation[];
}

export interface RunManifest {
  run_id: string;
  started_at: string;
  finished_at: string;
  status: string;
  trigger: string;
  stages: StageDetail[];
}

export interface RunHistoryResponse {
  runs: RunManifest[];
  total: number;
}

/** Build SERIES array from domain config. */
export function buildSeriesFromConfig(
  series: Record<string, { label: { en: string; pt: string }; unit: string; source: string; color: string; freshness_hours: number }>,
  language: 'en' | 'pt' = 'en',
): SeriesConfig[] {
  return Object.entries(series).map(([id, s]) => ({
    id,
    label: s.label[language],
    unit: s.unit,
    source: s.source,
    color: s.color,
    freshnessHours: s.freshness_hours,
  }));
}

// --- Query Usage types ---

export interface TierBreakdown {
  tier: string;
  count: number;
  tokens: number;
  cost_usd: number;
}

export interface QueryUsageResponse {
  total_queries: number;
  total_tokens: number;
  total_cost_usd: number;
  total_input_tokens: number;
  total_output_tokens: number;
  avg_tokens_per_query: number;
  avg_duration_ms: number;
  by_tier: TierBreakdown[];
  today: { queries: number; cost_usd: number };
  error?: string;
}

export const fetchQueryUsage = (): Promise<QueryUsageResponse> =>
  fetchJSON('/api/query/usage');

// --- Run History Fetchers ---

export const fetchRunHistory = (limit?: number): Promise<RunHistoryResponse> =>
  fetchJSON(`/api/runs${limit ? `?limit=${limit}` : ''}`);

export const fetchRunDetail = (runId: string): Promise<RunManifest> =>
  fetchJSON(`/api/runs/${runId}`);
