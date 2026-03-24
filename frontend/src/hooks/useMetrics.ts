import { useQuery } from '@tanstack/react-query';
import {
  fetchMetrics,
  fetchHealth,
  fetchInsightsLatest,
  fetchAnomalyInsights,
  fetchSyncStatus,
  fetchQualityLatest,
  getAfterDate,
} from '@/lib/api';
import type {
  MetricsResponse,
  HealthResponse,
  InsightResponse,
  SyncStatusResponse,
  QualityLatest,
  TimeRange,
} from '@/lib/api';

export function useMetrics(series: string, range: TimeRange = 'ALL') {
  const after = getAfterDate(range);

  return useQuery<MetricsResponse>({
    queryKey: ['metrics', series, range],
    queryFn: () => fetchMetrics(series, after),
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}

export function useHealth() {
  return useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: fetchHealth,
    staleTime: 60 * 1000,
    retry: 1,
  });
}

export function useInsights() {
  return useQuery<InsightResponse>({
    queryKey: ['insights', 'latest'],
    queryFn: fetchInsightsLatest,
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}

export function useAnomalyInsights() {
  return useQuery<InsightResponse>({
    queryKey: ['insights', 'anomalies'],
    queryFn: fetchAnomalyInsights,
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}

export function useSyncStatus() {
  return useQuery<SyncStatusResponse>({
    queryKey: ['sync-status'],
    queryFn: fetchSyncStatus,
    staleTime: 60 * 1000,
    retry: 1,
  });
}

export function useQualityLatest() {
  return useQuery<QualityLatest>({
    queryKey: ['quality', 'latest'],
    queryFn: fetchQualityLatest,
    staleTime: 60 * 1000,
    retry: 1,
  });
}
