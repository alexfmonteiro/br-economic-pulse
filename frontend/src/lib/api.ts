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

export interface QualityLatest {
  status: string;
  sync_health: string;
  last_sync: {
    last_sync_at: string;
    run_id: string | null;
    files_synced: number;
  } | null;
}

// --- Time Ranges ---

export type TimeRange = 'MTD' | 'YTD' | '1Y' | '2Y' | '5Y' | 'ALL';

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
  }

  return d.toISOString().split('T')[0];
}

export const TIME_RANGES: { value: TimeRange; label: string }[] = [
  { value: 'MTD', label: 'MTD' },
  { value: 'YTD', label: 'YTD' },
  { value: '1Y', label: '1Y' },
  { value: '2Y', label: '2Y' },
  { value: '5Y', label: '5Y' },
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

// --- Series label helper ---

const SERIES_LABELS: Record<string, string> = {
  bcb_432: 'SELIC',
  bcb_433: 'IPCA',
  bcb_1: 'USD/BRL',
  ibge_pnad: 'Unemployment',
  ibge_gdp: 'GDP',
  tesouro: 'Tesouro Direto',
};

export function getSeriesLabel(seriesId: string): string {
  return SERIES_LABELS[seriesId] ?? seriesId;
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
}

export const SERIES: SeriesConfig[] = [
  { id: 'bcb_432', label: 'SELIC', unit: '% a.a.', source: 'BCB', color: '#3b82f6' },
  { id: 'bcb_433', label: 'IPCA', unit: '% a.m.', source: 'BCB', color: '#8b5cf6' },
  { id: 'bcb_1', label: 'USD/BRL', unit: 'R$', source: 'BCB', color: '#22c55e' },
  { id: 'ibge_pnad', label: 'Unemployment', unit: '%', source: 'IBGE', color: '#f59e0b' },
  { id: 'ibge_gdp', label: 'GDP', unit: 'R$ bi', source: 'IBGE', color: '#06b6d4' },
  { id: 'tesouro', label: 'Tesouro Direto', unit: '% a.a.', source: 'Tesouro', color: '#ec4899' },
];
