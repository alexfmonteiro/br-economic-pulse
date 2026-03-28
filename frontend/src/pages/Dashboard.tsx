import { useState } from 'react';
import { Link } from 'react-router-dom';
import { buildSeriesFromConfig, type TimeRange } from '@/lib/api';
import { MetricCard } from '@/components/MetricCard';
import { InsightDigest } from '@/components/InsightDigest';
import { AnomalyCard } from '@/components/AnomalyCard';
import { RangeSelector } from '@/components/RangeSelector';
import { useHealth } from '@/hooks/useMetrics';
import { useLanguage } from '@/lib/LanguageContext';
import { useDomain, localize } from '@/lib/domain';

export function Dashboard() {
  const { data: health } = useHealth();
  const { language, t } = useLanguage();
  const cfg = useDomain();
  const syncHealth = health?.sync?.sync_health ?? 'unknown';
  const [range, setRange] = useState<TimeRange>('1Y');

  const SERIES = buildSeriesFromConfig(cfg.series, language);
  const dataSourceNames = cfg.data_sources.map((ds) => ds.name).join(', ');
  const dataSourceFooter = language === 'pt'
    ? `Dados provenientes de ${dataSourceNames}.`
    : `Data sourced from ${dataSourceNames}.`;

  return (
    <div className="min-h-screen p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
      {/* Header */}
      <header className="mb-8">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <div className="flex items-center gap-3 mb-1">
              <h1 className="text-2xl font-bold text-slate-100 tracking-tight">
                {t.hero.title}
              </h1>
              {syncHealth !== 'unknown' && (
                <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${
                  syncHealth === 'fresh'
                    ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
                    : syncHealth === 'stale'
                      ? 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30'
                      : 'bg-red-500/20 text-red-400 border-red-500/30'
                }`}>
                  {syncHealth}
                </span>
              )}
            </div>
            <p className="text-sm text-slate-500">
              {localize(cfg.landing.hero_title, language)}
            </p>
          </div>

          <RangeSelector value={range} onChange={setRange} />
        </div>
      </header>

      {/* Metric Grid */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
        {SERIES.map((s) => (
          <MetricCard key={s.id} config={s} range={range} />
        ))}
      </div>

      {/* Insight Digest */}
      <div className="space-y-4">
        <InsightDigest />
        <AnomalyCard />
      </div>

      {/* Footer */}
      <footer className="mt-12 py-6 border-t border-slate-800 text-center text-xs text-slate-600">
        <p>
          {dataSourceFooter}{' '}
          <Link to="/quality" className="text-brand-500 hover:text-brand-400">
            {t.hero.dataQualityLink}
          </Link>
        </p>
      </footer>
    </div>
  );
}
