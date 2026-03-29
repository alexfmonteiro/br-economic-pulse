import Markdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useInsights } from '@/hooks/useMetrics';
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

export function InsightDigest() {
  const { data, isLoading, isError } = useInsights();
  const { language, t } = useLanguage();
  const locale = language === 'pt' ? 'pt-BR' : 'en-US';

  const insights = data?.insights ?? [];
  const insight = insights.find((i) => i.language === language) ?? insights.find((i) => i.language === 'en') ?? insights[0];

  return (
    <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
      <div className="flex items-center gap-2 mb-4">
        <div className="h-2 w-2 rounded-full bg-brand-500" />
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
          {t.insight.title}
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
          Unable to load insights. The API may be unavailable.
        </p>
      )}

      {!isLoading && !isError && !insight && (
        <div className="space-y-3 text-sm text-slate-400">
          <p>{t.insight.noData}</p>
          <div className="flex items-center gap-2 text-xs text-slate-600">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-slate-600" />
            {t.insight.poweredBy} Claude Sonnet
          </div>
        </div>
      )}

      {!isLoading && !isError && insight && (
        <div className="space-y-3">
          <div className="text-sm text-slate-300 leading-relaxed prose prose-invert prose-sm max-w-none prose-p:my-1 prose-strong:text-slate-100 prose-ul:my-1 prose-li:my-0">
            <Markdown remarkPlugins={[remarkGfm]}>{insight.content}</Markdown>
          </div>

          <div className="flex flex-wrap items-center gap-3 text-xs text-slate-500">
            <span>{formatTimestamp(insight.generated_at, locale)}</span>

            {insight.metric_refs.length > 0 && (
              <span className="text-slate-600">
                {t.insight.refs}: {insight.metric_refs.map(getSeriesLabel).join(', ')}
              </span>
            )}
          </div>

          <div className="flex items-center gap-2 text-xs text-slate-600">
            <span className="inline-block h-1.5 w-1.5 rounded-full bg-slate-600" />
            {t.insight.poweredBy} {insight.model_version}
          </div>
        </div>
      )}
    </section>
  );
}
