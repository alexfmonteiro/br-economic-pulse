import { useState, useCallback } from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { SERIES, type TimeRange, type SeriesConfig } from '@/lib/api';
import { useMetrics } from '@/hooks/useMetrics';
import { RangeSelector } from '@/components/RangeSelector';
import { useLanguage } from '@/lib/LanguageContext';
import type { Translations } from '@/lib/i18n';

function formatAxisDate(dateStr: string, locale: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString(locale, { month: 'short', year: '2-digit' });
}

function formatTooltipDate(dateStr: string, locale: string): string {
  return new Date(dateStr).toLocaleDateString(locale, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

function downloadCSV(config: SeriesConfig, data: { date: string; value: number }[]) {
  const header = 'date,value\n';
  const rows = data.map((d) => `${d.date},${d.value}`).join('\n');
  const blob = new Blob([header + rows], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${config.id}_${config.label.toLowerCase().replace(/\s+/g, '_')}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

interface ChartCardProps {
  config: SeriesConfig;
  range: TimeRange;
  t: Translations;
  locale: string;
}

function ChartCard({ config, range, t, locale }: ChartCardProps) {
  const label = (t.seriesLabels as Record<string, string>)[config.id] ?? config.label;
  const hint = (t.seriesHints as Record<string, string>)[config.id] ?? '';
  const { data, isLoading, isError } = useMetrics(config.id, range);

  const points = data?.data_points ?? [];
  const chartData = points.map((p) => ({
    date: p.date,
    value: p.value,
  }));

  const handleDownload = useCallback(() => {
    downloadCSV(config, chartData);
  }, [config, chartData]);

  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-5">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-semibold text-slate-200">{label}</h3>
          <p className="text-[10px] text-slate-500 uppercase tracking-wider">
            {config.source} | {config.unit}
          </p>
          {hint && (
            <p className="text-[11px] text-slate-500 mt-1 leading-snug max-w-md">
              {hint}
            </p>
          )}
        </div>
        <button
          onClick={handleDownload}
          disabled={chartData.length === 0}
          className="cursor-pointer text-[10px] text-slate-500 hover:text-slate-300 transition-colors border border-slate-700/50 rounded-md px-2 py-1 disabled:opacity-30 disabled:cursor-not-allowed focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none"
        >
          {t.analytics.exportCsv}
        </button>
      </div>

      {isLoading && (
        <div className="h-48 flex items-center justify-center">
          <div className="h-4 w-4 rounded-full border-2 border-brand-500 border-t-transparent animate-spin" />
        </div>
      )}

      {isError && (
        <div className="h-48 flex items-center justify-center text-sm text-slate-500">
          {t.analytics.failedToLoad}
        </div>
      )}

      {!isLoading && !isError && chartData.length === 0 && (
        <div className="h-48 flex items-center justify-center text-sm text-slate-500">
          {t.analytics.noData}
        </div>
      )}

      {!isLoading && !isError && chartData.length > 0 && (
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis
              dataKey="date"
              tickFormatter={(v) => formatAxisDate(v, locale)}
              tick={{ fontSize: 10, fill: '#64748b' }}
              stroke="#475569"
              tickLine={false}
              minTickGap={40}
            />
            <YAxis
              domain={['auto', 'auto']}
              tick={{ fontSize: 10, fill: '#64748b' }}
              stroke="#475569"
              tickLine={false}
              width={50}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1e293b',
                border: '1px solid #334155',
                borderRadius: '8px',
                fontSize: '12px',
              }}
              labelFormatter={(label) => formatTooltipDate(String(label), locale)}
              formatter={(value) => [
                Number(value).toLocaleString(locale, { maximumFractionDigits: 4 }),
                config.label,
              ]}
            />
            <Line
              type="monotone"
              dataKey="value"
              stroke={config.color}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: config.color }}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      )}

      {data?.last_updated && (
        <p className="text-[10px] text-slate-600 mt-2">
          {t.analytics.lastUpdated}: {new Date(data.last_updated).toLocaleDateString(locale)}
          {' | '}
          {points.length} {t.analytics.dataPoints}
        </p>
      )}
    </div>
  );
}

export function AnalyticsPage() {
  const [range, setRange] = useState<TimeRange>('2Y');
  const { language, t } = useLanguage();
  const locale = language === 'pt' ? 'pt-BR' : 'en-US';

  return (
    <div className="min-h-[calc(100vh-3.5rem)] p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
      <header className="mb-8">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-slate-100">{t.analytics.title}</h1>
            <p className="text-sm text-slate-500 mt-1">
              {t.analytics.subtitle}
            </p>
          </div>
          <RangeSelector value={range} onChange={setRange} />
        </div>
      </header>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {SERIES.map((s) => (
          <ChartCard key={s.id} config={s} range={range} t={t} locale={locale} />
        ))}
      </div>
    </div>
  );
}
