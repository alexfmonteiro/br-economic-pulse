import * as Tooltip from '@radix-ui/react-tooltip';
import { useMetrics } from '@/hooks/useMetrics';
import type { SeriesConfig, TimeRange } from '@/lib/api';
import { useLanguage } from '@/lib/LanguageContext';
import { Sparkline } from './Sparkline';
import { DeltaIndicator } from './DeltaIndicator';
import { FreshnessBadge } from './FreshnessBadge';

interface MetricCardProps {
  config: SeriesConfig;
  range: TimeRange;
}

function formatValue(value: number, unit: string): string {
  if (unit === 'R$') return `R$ ${value.toFixed(2)}`;
  if (unit === 'R$ bi' || unit === 'R$ mi') return `R$ ${value.toLocaleString('en-US', { maximumFractionDigits: 1 })} ${unit.split(' ')[1]}`;
  return `${value.toFixed(2)}${unit ? ` ${unit}` : ''}`;
}

function formatDate(dateStr: string, locale: string): string {
  return new Date(dateStr).toLocaleDateString(locale, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

export function MetricCard({ config, range }: MetricCardProps) {
  const { language, t } = useLanguage();
  const locale = language === 'pt' ? 'pt-BR' : 'en-US';
  const hint = (t.seriesHints as Record<string, string>)[config.id] ?? '';
  const { data, isLoading, isError } = useMetrics(config.id, range);

  if (isLoading) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-5 animate-pulse">
        <div className="h-4 w-20 rounded bg-slate-700 mb-3" />
        <div className="h-8 w-28 rounded bg-slate-700 mb-2" />
        <div className="h-10 rounded bg-slate-700" />
      </div>
    );
  }

  if (isError || !data || data.data_points.length === 0) {
    return (
      <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-5">
        <div className="flex items-center justify-between mb-1">
          <h3 className="text-sm font-medium text-slate-400">{config.label}</h3>
          <span className="text-[10px] text-slate-600">{config.source}</span>
        </div>
        <p className="text-2xl font-semibold text-slate-500">--</p>
        <p className="text-xs text-slate-600 mt-2">No data available</p>
      </div>
    );
  }

  const points = data.data_points;
  const latest = points[points.length - 1];
  const first = points[0];
  const sparkData = points.map((p) => ({ value: p.value }));

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-5 hover:border-slate-600/50 transition-colors">
      <div className="flex items-center justify-between mb-1">
        {hint ? (
          <Tooltip.Provider delayDuration={300}>
            <Tooltip.Root>
              <Tooltip.Trigger asChild>
                <h3 className="text-sm font-medium text-slate-400 cursor-help border-b border-dotted border-slate-600">
                  {config.label}
                </h3>
              </Tooltip.Trigger>
              <Tooltip.Portal>
                <Tooltip.Content
                  side="bottom"
                  align="start"
                  sideOffset={6}
                  className="z-50 max-w-xs rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-xs text-slate-300 leading-relaxed shadow-lg animate-in fade-in-0 zoom-in-95"
                >
                  {hint}
                  <Tooltip.Arrow className="fill-slate-800" />
                </Tooltip.Content>
              </Tooltip.Portal>
            </Tooltip.Root>
          </Tooltip.Provider>
        ) : (
          <h3 className="text-sm font-medium text-slate-400">{config.label}</h3>
        )}
        <span className="text-[10px] text-slate-600 uppercase tracking-wider">{config.source}</span>
      </div>

      <div className="flex items-baseline gap-2 mb-1">
        <span className="text-2xl font-semibold text-slate-100">
          {formatValue(latest.value, config.unit)}
        </span>
      </div>

      <DeltaIndicator
        current={latest.value}
        previous={first.value}
      />

      <div className="mt-3">
        <Sparkline data={sparkData} color={config.color} />
      </div>

      <div className="flex items-center justify-between mt-3">
        <FreshnessBadge lastUpdated={data.last_updated} freshnessHours={config.freshnessHours} />
        <span className="text-[10px] text-slate-600">
          {points.length} pts {data.last_updated ? `\u00B7 ${formatDate(data.last_updated, locale)}` : ''}
        </span>
      </div>
    </div>
  );
}
