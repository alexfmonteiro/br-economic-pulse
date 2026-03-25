import { SERIES } from '@/lib/api';
import type { SeriesFreshnessData } from '@/lib/api';
import { useHealth, useSyncStatus, useQualityLatest } from '@/hooks/useMetrics';
import { useLanguage } from '@/lib/LanguageContext';

function formatTimestamp(iso: string | null): string {
  if (!iso) return 'Never';
  return new Date(iso).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatLag(seconds: number | null): string {
  if (seconds === null) return 'Unknown';
  if (seconds < 60) return `${Math.round(seconds)}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h ago`;
  return `${Math.round(seconds / 86400)}d ago`;
}

function SyncStatusPanel() {
  const { data, isLoading, isError } = useSyncStatus();
  const { t } = useLanguage();

  if (isLoading) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6 animate-pulse">
        <div className="h-4 w-32 rounded bg-slate-700 mb-4" />
        <div className="h-20 rounded bg-slate-700" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
          {t.quality.syncStatus}
        </h3>
        <p className="text-sm text-slate-500">{t.quality.unableToLoadSync}</p>
      </div>
    );
  }

  const healthColor =
    data.sync_health === 'fresh'
      ? 'text-emerald-400'
      : data.sync_health === 'stale'
        ? 'text-yellow-400'
        : 'text-red-400';

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          {t.quality.syncStatus}
        </h3>
        <span className={`text-sm font-semibold uppercase ${healthColor}`}>
          {data.sync_health}
        </span>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.lastSync}</p>
          <p className="text-sm text-slate-300">{formatTimestamp(data.last_sync_at)}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.syncLag}</p>
          <p className="text-sm text-slate-300">{formatLag(data.seconds_since_sync)}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.filesSynced}</p>
          <p className="text-sm text-slate-300">{data.files_synced}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.duration}</p>
          <p className="text-sm text-slate-300">{formatDuration(data.sync_duration_ms)}</p>
        </div>
      </div>

      {data.run_id && (
        <p className="text-[10px] text-slate-600 mt-3">
          {t.quality.runId}: {data.run_id} | {t.quality.source}: {data.source}
        </p>
      )}
    </div>
  );
}

function QualityStatusPanel() {
  const { data, isLoading, isError } = useQualityLatest();
  const { t } = useLanguage();

  if (isLoading) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6 animate-pulse">
        <div className="h-4 w-32 rounded bg-slate-700 mb-4" />
        <div className="h-12 rounded bg-slate-700" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
          {t.quality.pipelineQuality}
        </h3>
        <p className="text-sm text-slate-500">{t.quality.unableToLoadQuality}</p>
      </div>
    );
  }

  const statusColor =
    data.status === 'healthy'
      ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
      : data.status === 'degraded'
        ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
        : 'bg-red-500/20 text-red-400 border-red-500/30';

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          {t.quality.pipelineQuality}
        </h3>
        <span
          className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${statusColor}`}
        >
          {data.status}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.syncHealth}</p>
          <p className="text-sm text-slate-300 capitalize">{data.sync_health}</p>
        </div>
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.lastSync}</p>
          <p className="text-sm text-slate-300">
            {data.last_sync ? formatTimestamp(data.last_sync.last_sync_at) : 'N/A'}
          </p>
        </div>
      </div>

      {data.report && (
        <div className="mt-4 pt-3 border-t border-slate-700/30">
          <div className="flex items-center gap-3 mb-2">
            <span className="text-emerald-400 text-xs font-medium">
              {data.report.checks.filter(c => c.passed).length} passed
            </span>
            {data.report.checks.filter(c => !c.passed).length > 0 && (
              <span className="text-red-400 text-xs font-medium">
                {data.report.checks.filter(c => !c.passed).length} failed
              </span>
            )}
          </div>
          {data.report.critical_failures.length > 0 && (
            <div className="text-xs text-red-400/80 space-y-0.5">
              {data.report.critical_failures.map((f, i) => (
                <p key={i}>{f}</p>
              ))}
            </div>
          )}
        </div>
      )}

      {data.last_sync && !data.report && (
        <p className="text-[10px] text-slate-600 mt-3">
          {t.quality.filesSynced}: {data.last_sync.files_synced}
          {data.last_sync.run_id && ` | ${t.quality.runId}: ${data.last_sync.run_id}`}
        </p>
      )}
    </div>
  );
}

function formatShortDate(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

function formatShortTimestamp(iso: string | null): string {
  if (!iso) return '—';
  return new Date(iso).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

const STATUS_COLORS: Record<string, string> = {
  fresh: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  stale: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
  critical: 'bg-red-500/20 text-red-400 border-red-500/30',
};

function SeriesFreshnessRow({ label, freshness }: { label: string; freshness: SeriesFreshnessData }) {
  const badgeColor = STATUS_COLORS[freshness.status] ?? STATUS_COLORS.critical;

  return (
    <div className="flex items-center justify-between py-2.5 border-b border-slate-700/30 last:border-0">
      <span className="text-sm text-slate-300 font-medium">{label}</span>
      <div className="flex items-center gap-4">
        <div className="text-right">
          <p className="text-[10px] text-slate-500 uppercase">Latest data</p>
          <p className="text-xs text-slate-400">{formatShortDate(freshness.last_updated)}</p>
        </div>
        <div className="text-right">
          <p className="text-[10px] text-slate-500 uppercase">Last ingested</p>
          <p className="text-xs text-slate-400">{formatShortTimestamp(freshness.last_ingested_at)}</p>
        </div>
        <span
          className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${badgeColor}`}
        >
          {freshness.status}
        </span>
      </div>
    </div>
  );
}

function SeriesFreshnessPanel() {
  const { data, isLoading, isError } = useQualityLatest();
  const { t } = useLanguage();

  if (isLoading) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6 animate-pulse">
        <div className="h-4 w-32 rounded bg-slate-700 mb-4" />
        <div className="h-40 rounded bg-slate-700" />
      </div>
    );
  }

  if (isError || !data) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
        <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
          {t.quality.seriesFreshness}
        </h3>
        <p className="text-sm text-slate-500">Unable to load freshness data</p>
      </div>
    );
  }

  // Build lookup from API freshness data
  const freshnessMap = new Map(data.series_freshness.map(f => [f.series, f]));

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
        {t.quality.seriesFreshness}
      </h3>
      <div>
        {SERIES.map((s) => {
          const freshness = freshnessMap.get(s.id);
          if (!freshness) return null;
          const label = (t.seriesLabels as Record<string, string>)[s.id] ?? s.label;
          return <SeriesFreshnessRow key={s.id} label={label} freshness={freshness} />;
        })}
      </div>
    </div>
  );
}

export function QualityPage() {
  const { data: health } = useHealth();
  const { t } = useLanguage();

  return (
    <div className="min-h-[calc(100vh-3.5rem)] p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
      <header className="mb-8">
        <h1 className="text-2xl font-bold text-slate-100">{t.quality.title}</h1>
        <p className="text-sm text-slate-500 mt-1">
          {t.quality.subtitle}
        </p>
      </header>

      <div className="space-y-6">
        {/* Health overview */}
        <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.quality.systemHealth}
          </h3>
          {health ? (
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
              <div>
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.status}</p>
                <p className={`text-sm font-semibold ${
                  health.status === 'ok' ? 'text-emerald-400' : 'text-yellow-400'
                }`}>
                  {health.status}
                </p>
              </div>
              <div>
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.timestamp}</p>
                <p className="text-sm text-slate-300">
                  {formatTimestamp(health.timestamp)}
                </p>
              </div>
              <div>
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">{t.quality.syncHealth}</p>
                <p className="text-sm text-slate-300 capitalize">
                  {health.sync?.sync_health ?? 'Unknown'}
                </p>
              </div>
            </div>
          ) : (
            <div className="h-12 flex items-center">
              <div className="h-4 w-4 rounded-full border-2 border-brand-500 border-t-transparent animate-spin" />
            </div>
          )}
        </div>

        {/* Quality and Sync panels side by side */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <QualityStatusPanel />
          <SyncStatusPanel />
        </div>

        {/* Per-series freshness */}
        <SeriesFreshnessPanel />

        {/* Data freshness from health endpoint */}
        {health?.data_freshness && Object.keys(health.data_freshness).length > 0 && (
          <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
            <h3 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
              {t.quality.dataFreshnessHealth}
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {Object.entries(health.data_freshness).map(([key, value]) => (
                <div
                  key={key}
                  className="flex items-center justify-between rounded-lg border border-slate-700/30 bg-slate-900/30 px-3 py-2"
                >
                  <span className="text-xs text-slate-400">{key}</span>
                  <span className="text-xs text-slate-300 font-medium">{value}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
