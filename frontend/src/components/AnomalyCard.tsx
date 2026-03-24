import Markdown from 'react-markdown';
import { useAnomalyInsights } from '@/hooks/useMetrics';
import { getSeriesLabel } from '@/lib/api';
import { useLanguage } from '@/lib/LanguageContext';

function formatTimestamp(iso: string, locale: string): string {
  return new Date(iso).toLocaleDateString(locale, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function AnomalyCard() {
  const { data, isLoading, isError } = useAnomalyInsights();
  const { language, t } = useLanguage();
  const locale = language === 'pt' ? 'pt-BR' : 'en-US';

  const insights = data?.insights ?? [];
  const insight = insights.find((i) => i.language === language) ?? insights.find((i) => i.language === 'en') ?? insights[0];

  // Don't render the card at all if there's no data and we're not loading
  if (!isLoading && !isError && !insight) {
    return null;
  }

  return (
    <section className="rounded-xl border border-amber-700/30 bg-slate-800/50 p-6">
      <div className="flex items-center gap-2 mb-4">
        <div className="h-2 w-2 rounded-full bg-amber-500" />
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          {t.anomaly.title}
        </h2>
      </div>

      {isLoading && (
        <div className="space-y-3 animate-pulse">
          <div className="h-4 w-3/4 rounded bg-slate-700" />
          <div className="h-4 w-1/2 rounded bg-slate-700" />
        </div>
      )}

      {isError && (
        <p className="text-sm text-slate-500">
          Unable to load anomaly analysis.
        </p>
      )}

      {!isLoading && !isError && insight && (
        <div className="space-y-3">
          <div className="text-sm text-slate-300 leading-relaxed prose prose-invert prose-sm max-w-none prose-p:my-1 prose-strong:text-slate-100 prose-ul:my-1 prose-li:my-0">
            <Markdown>{insight.content}</Markdown>
          </div>

          <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500">
            <span className="inline-flex items-center gap-1 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/30 px-2 py-0.5 font-semibold uppercase tracking-wider text-[10px]">
              {t.anomaly.badge}
            </span>

            <span>{formatTimestamp(insight.generated_at, locale)}</span>

            {insight.metric_refs.length > 0 && (
              <span className="text-slate-600">
                {insight.metric_refs.map(getSeriesLabel).join(', ')}
              </span>
            )}
          </div>

          <div className="flex items-center gap-2 text-xs text-slate-600">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-slate-600" />
            {t.anomaly.poweredBy} {insight.model_version}
          </div>
        </div>
      )}
    </section>
  );
}
