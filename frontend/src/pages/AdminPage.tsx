import { useState } from 'react';
import { buildSeriesFromConfig } from '@/lib/api';
import type { RunManifest, StageDetail, SeriesFreshnessData } from '@/lib/api';
import {
  useHealth,
  useSyncStatus,
  useQualityLatest,
  useRunHistory,
  useQueryUsage,
} from '@/hooks/useMetrics';
import { useLanguage } from '@/lib/LanguageContext';
import { useDomain } from '@/lib/domain';

// --- Helpers ---

function formatTimestamp(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60_000).toFixed(1)}m`;
}

function formatLag(seconds: number | null): string {
  if (seconds === null) return '—';
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
}

const STATUS_BADGE: Record<string, string> = {
  success: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  failed: 'bg-red-500/20 text-red-400 border-red-500/30',
  partial: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
  fresh: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  stale: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
  critical: 'bg-red-500/20 text-red-400 border-red-500/30',
  healthy: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  degraded: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
};

function Badge({ value }: { value: string }) {
  const color = STATUS_BADGE[value] ?? 'bg-slate-500/20 text-slate-400 border-slate-500/30';
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${color}`}>
      {value}
    </span>
  );
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
        {title}
      </h3>
      {children}
    </div>
  );
}

function Skeleton({ rows = 3 }: { rows?: number }) {
  return (
    <div className="animate-pulse space-y-3">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="h-4 rounded bg-slate-700" />
      ))}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div>
      <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{label}</p>
      <p className="text-sm text-slate-300 font-medium">{value}</p>
    </div>
  );
}

// --- Sections ---

function StageRow({ stage }: { stage: StageDetail }) {
  return (
    <div className="grid grid-cols-6 gap-2 py-2 border-b border-slate-700/30 last:border-0 text-xs">
      <span className="text-slate-300 font-medium">{stage.stage_name}</span>
      <span className="text-slate-400 text-right">{formatDuration(stage.duration_ms)}</span>
      <span className="text-slate-400 text-right">{stage.rows_read}</span>
      <span className="text-slate-400 text-right">{stage.rows_written}</span>
      <span className="text-slate-400 text-right">{stage.rows_quarantined}</span>
      <span className="text-slate-400 text-right">{stage.rows_rescued}</span>
    </div>
  );
}

function RunRow({ run }: { run: RunManifest }) {
  const [expanded, setExpanded] = useState(false);
  const durationMs =
    run.started_at && run.finished_at
      ? new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()
      : 0;

  return (
    <div className="border-b border-slate-700/30 last:border-0">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full grid grid-cols-5 gap-3 py-3 text-left text-xs hover:bg-slate-700/20 transition-colors cursor-pointer"
      >
        <span className="text-slate-400 font-mono truncate" title={run.run_id}>{run.run_id.slice(0, 12)}</span>
        <span className="text-slate-300">{formatTimestamp(run.started_at)}</span>
        <span><Badge value={run.status} /></span>
        <span className="text-slate-400">{formatDuration(durationMs)}</span>
        <span className="text-slate-400">{run.stages.length} stages</span>
      </button>
      {expanded && run.stages.length > 0 && (
        <div className="pb-3 pl-4 pr-2">
          <div className="grid grid-cols-6 gap-2 text-[10px] text-slate-500 uppercase tracking-wider pb-1 border-b border-slate-700/30">
            <span>Stage</span>
            <span className="text-right">Duration</span>
            <span className="text-right">Read</span>
            <span className="text-right">Written</span>
            <span className="text-right">Quarantined</span>
            <span className="text-right">Rescued</span>
          </div>
          {run.stages.map((s) => (
            <StageRow key={s.stage_name} stage={s} />
          ))}
        </div>
      )}
    </div>
  );
}

function PipelineRunsSection() {
  const { data, isLoading, isError } = useRunHistory(20);

  if (isLoading) return <Card title="Pipeline Runs"><Skeleton rows={5} /></Card>;
  if (isError || !data) return <Card title="Pipeline Runs"><p className="text-sm text-slate-500">Unable to load run history</p></Card>;

  return (
    <Card title="Pipeline Runs">
      {data.runs.length === 0 ? (
        <p className="text-sm text-slate-500">No pipeline runs recorded yet</p>
      ) : (
        <>
          <div className="grid grid-cols-5 gap-3 text-[10px] text-slate-500 uppercase tracking-wider pb-2 border-b border-slate-700/30">
            <span>Run ID</span>
            <span>Started</span>
            <span>Status</span>
            <span>Duration</span>
            <span>Stages</span>
          </div>
          {data.runs.map((run) => (
            <RunRow key={run.run_id} run={run} />
          ))}
          <p className="text-[10px] text-slate-600 mt-2">
            Showing {data.runs.length} of {data.total} runs
          </p>
        </>
      )}
    </Card>
  );
}

function QualityReportsSection() {
  const { data, isLoading, isError } = useQualityLatest();

  if (isLoading) return <Card title="Quality Reports"><Skeleton /></Card>;
  if (isError || !data) return <Card title="Quality Reports"><p className="text-sm text-slate-500">Unable to load quality data</p></Card>;

  const report = data.report;
  if (!report) {
    return (
      <Card title="Quality Reports">
        <div className="flex items-center gap-2">
          <Badge value={data.status} />
          <span className="text-sm text-slate-400">No quality report available</span>
        </div>
      </Card>
    );
  }

  const passed = report.checks.filter((c) => c.passed).length;
  const failed = report.checks.filter((c) => !c.passed).length;

  return (
    <Card title="Quality Reports">
      <div className="flex items-center gap-3 mb-4">
        <Badge value={report.overall_status} />
        <span className="text-xs text-slate-400">{formatTimestamp(report.timestamp)}</span>
      </div>

      <div className="flex items-center gap-4 mb-4">
        <span className="text-emerald-400 text-sm font-medium">{passed} passed</span>
        {failed > 0 && <span className="text-red-400 text-sm font-medium">{failed} failed</span>}
      </div>

      {report.checks.length > 0 && (
        <div className="space-y-1">
          {report.checks.map((c, i) => (
            <div key={i} className="flex items-center gap-2 text-xs">
              <span className={c.passed ? 'text-emerald-400' : 'text-red-400'}>
                {c.passed ? '\u2713' : '\u2717'}
              </span>
              <span className="text-slate-300">{c.check_name}</span>
              <span className="text-slate-500 ml-auto">{c.message}</span>
            </div>
          ))}
        </div>
      )}

      {report.critical_failures.length > 0 && (
        <div className="mt-3 pt-3 border-t border-slate-700/30">
          <p className="text-[10px] text-red-400 uppercase tracking-wider mb-1">Critical Failures</p>
          {report.critical_failures.map((f, i) => (
            <p key={i} className="text-xs text-red-400/80">{f}</p>
          ))}
        </div>
      )}
    </Card>
  );
}

function SeriesFreshnessSection() {
  const { data, isLoading, isError } = useQualityLatest();
  const { language } = useLanguage();
  const cfg = useDomain();
  const SERIES = buildSeriesFromConfig(cfg.series, language);

  if (isLoading) return <Card title="Series Freshness"><Skeleton rows={8} /></Card>;
  if (isError || !data) return <Card title="Series Freshness"><p className="text-sm text-slate-500">Unable to load freshness data</p></Card>;

  const freshnessMap = new Map(data.series_freshness.map((f) => [f.series, f]));

  // Sort: critical first, then stale, then fresh
  const order: Record<string, number> = { critical: 0, stale: 1, fresh: 2 };
  const sortedSeries = [...SERIES].sort((a, b) => {
    const fa = freshnessMap.get(a.id);
    const fb = freshnessMap.get(b.id);
    return (order[fa?.status ?? 'critical'] ?? 3) - (order[fb?.status ?? 'critical'] ?? 3);
  });

  return (
    <Card title="Series Freshness">
      <div className="grid grid-cols-5 gap-2 text-[10px] text-slate-500 uppercase tracking-wider pb-2 border-b border-slate-700/30">
        <span>Series</span>
        <span>Last Updated</span>
        <span className="text-right">Hours Since</span>
        <span>Last Ingested</span>
        <span className="text-right">Status</span>
      </div>
      {sortedSeries.map((s) => {
        const f: SeriesFreshnessData | undefined = freshnessMap.get(s.id);
        if (!f) return null;
        const label = cfg.series[s.id] ? cfg.series[s.id].label[language] : s.label;
        return (
          <div key={s.id} className="grid grid-cols-5 gap-2 py-2 border-b border-slate-700/30 last:border-0 text-xs items-center">
            <span className="text-slate-300 font-medium">{label}</span>
            <span className="text-slate-400">{f.last_updated ? new Date(f.last_updated).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '—'}</span>
            <span className="text-slate-400 text-right">{f.hours_since_update != null ? `${Math.round(f.hours_since_update)}h` : '—'}</span>
            <span className="text-slate-400">{f.last_ingested_at ? formatTimestamp(f.last_ingested_at) : '—'}</span>
            <span className="text-right"><Badge value={f.status} /></span>
          </div>
        );
      })}
    </Card>
  );
}

function SyncStatusSection() {
  const { data, isLoading, isError } = useSyncStatus();

  if (isLoading) return <Card title="Sync Status"><Skeleton /></Card>;
  if (isError || !data) return <Card title="Sync Status"><p className="text-sm text-slate-500">Unable to load sync status</p></Card>;

  return (
    <Card title="Sync Status">
      <div className="flex items-center gap-3 mb-4">
        <Badge value={data.sync_health} />
        <span className="text-xs text-slate-400">Source: {data.source}</span>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <Stat label="Last Sync" value={formatTimestamp(data.last_sync_at)} />
        <Stat label="Sync Lag" value={formatLag(data.seconds_since_sync)} />
        <Stat label="Files Synced" value={data.files_synced} />
        <Stat label="Duration" value={formatDuration(data.sync_duration_ms)} />
      </div>
      {data.run_id && (
        <p className="text-[10px] text-slate-600 mt-3">Run ID: {data.run_id}</p>
      )}
    </Card>
  );
}

function QueryMetricsSection() {
  const { data, isLoading, isError } = useQueryUsage();

  if (isLoading) return <Card title="Query Metrics"><Skeleton /></Card>;
  if (isError || !data || data.error) {
    return (
      <Card title="Query Metrics">
        <p className="text-sm text-slate-500">{data?.error ?? 'Unable to load query metrics'}</p>
      </Card>
    );
  }

  return (
    <Card title="Query Metrics">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-4">
        <Stat label="Total Queries" value={data.total_queries} />
        <Stat label="Total Cost" value={`$${data.total_cost_usd.toFixed(4)}`} />
        <Stat label="Avg Latency" value={formatDuration(data.avg_duration_ms)} />
        <Stat label="Avg Tokens/Query" value={Math.round(data.avg_tokens_per_query)} />
      </div>

      {data.by_tier.length > 0 && (
        <div className="mb-4">
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-2">By Tier</p>
          <div className="space-y-1">
            {data.by_tier.map((t) => (
              <div key={t.tier} className="flex items-center gap-3 text-xs">
                <span className="text-slate-300 font-medium w-24">{t.tier}</span>
                <span className="text-slate-400">{t.count} queries</span>
                <span className="text-slate-400">{t.tokens.toLocaleString()} tokens</span>
                <span className="text-slate-500 ml-auto">${t.cost_usd.toFixed(4)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="pt-3 border-t border-slate-700/30">
        <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-2">Today</p>
        <div className="grid grid-cols-2 gap-4">
          <Stat label="Queries Today" value={data.today.queries} />
          <Stat label="Cost Today" value={`$${data.today.cost_usd.toFixed(4)}`} />
        </div>
      </div>
    </Card>
  );
}

function SystemInfoSection() {
  const { data: health, isLoading } = useHealth();

  if (isLoading) return <Card title="System Info"><Skeleton /></Card>;

  return (
    <Card title="System Info">
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
        <Stat label="Status" value={health?.status ?? '—'} />
        <Stat label="Timestamp" value={formatTimestamp(health?.timestamp ?? null)} />
        <Stat label="Sync Health" value={health?.sync?.sync_health ?? '—'} />
      </div>
    </Card>
  );
}

// --- Data Explorer ---

const MARIMO_URL = import.meta.env.VITE_MARIMO_URL ?? '';

function DataExplorerSection() {
  if (!MARIMO_URL) return null;

  return (
    <Card title="Data Explorer">
      <p className="text-sm text-slate-400 mb-4">
        Interactive DuckDB notebooks for inspecting bronze, silver, and gold
        parquet data directly from R2.
      </p>
      <a
        href={MARIMO_URL}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-2 rounded-lg bg-blue-600 hover:bg-blue-500 px-4 py-2 text-sm font-medium text-white transition-colors"
      >
        Open Notebooks
        <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 6H5.25A2.25 2.25 0 0 0 3 8.25v10.5A2.25 2.25 0 0 0 5.25 21h10.5A2.25 2.25 0 0 0 18 18.75V10.5m-10.5 6L21 3m0 0h-5.25M21 3v5.25" />
        </svg>
      </a>
    </Card>
  );
}

// --- Main ---

export function AdminPage() {
  return (
    <div className="min-h-[calc(100vh-3.5rem)] p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
      <header className="mb-8">
        <h1 className="text-2xl font-bold text-slate-100">Admin</h1>
        <p className="text-sm text-slate-500 mt-1">
          Pipeline operations, data quality, and system metrics.
        </p>
      </header>

      <div className="space-y-6">
        <DataExplorerSection />
        <PipelineRunsSection />

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <QualityReportsSection />
          <SyncStatusSection />
        </div>

        <SeriesFreshnessSection />

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <QueryMetricsSection />
          <SystemInfoSection />
        </div>
      </div>
    </div>
  );
}
