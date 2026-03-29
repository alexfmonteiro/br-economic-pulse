import { useState } from 'react';
import { useLanguage } from '@/lib/LanguageContext';
import { useDomain, localize } from '@/lib/domain';

export function AboutPage() {
  const { language, t } = useLanguage();
  const cfg = useDomain();
  const [showDevDetails, setShowDevDetails] = useState(false);

  const aboutTitle = language === 'pt'
    ? `Sobre o ${cfg.app.title}`
    : `About ${cfg.app.title}`;

  return (
    <div className="min-h-[calc(100vh-3.5rem)] p-4 sm:p-6 lg:p-8 max-w-4xl mx-auto">
      <header className="mb-8">
        <h1 className="text-2xl font-bold text-slate-100">{aboutTitle}</h1>
      </header>

      <div className="space-y-8">
        {/* What is Veredas */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.whatIsTitle}
          </h2>
          <p className="text-sm text-slate-300 leading-relaxed">
            {t.about.whatIsDesc}
          </p>
        </section>

        {/* Data Sources */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.dataSources}
          </h2>
          <div className="space-y-4">
            {cfg.data_sources.map((ds) => (
              <div key={ds.id} className="flex items-start gap-4 rounded-lg border border-slate-700/30 bg-slate-900/30 p-4">
                <div className="flex-1">
                  <h3 className="text-sm font-semibold text-slate-200">{ds.name}</h3>
                  <p className="text-xs text-slate-400 mt-1">
                    {localize(ds.description, language)}
                  </p>
                  <a
                    href={ds.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-brand-400 hover:text-brand-300 mt-1 inline-block"
                  >
                    {ds.url.replace(/^https?:\/\/(www\.)?/, '').replace(/\/$/, '')}
                  </a>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Open Source */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.openSource}
          </h2>
          <p className="text-sm text-slate-300 leading-relaxed mb-4">
            {t.about.openSourceDesc}
          </p>
        </section>

        {/* For Developers (collapsible) */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <button
            onClick={() => setShowDevDetails(!showDevDetails)}
            className="cursor-pointer flex items-center gap-2 w-full text-left"
          >
            <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider">
              {t.about.forDevelopers}
            </h2>
            <svg
              className={`h-4 w-4 text-slate-400 transition-transform ${showDevDetails ? 'rotate-90' : ''}`}
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth="2"
              stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
          </button>

          {showDevDetails && (
            <div className="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-6">
              <div>
                <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">{t.about.backend}</h3>
                <ul className="space-y-1.5 text-sm text-slate-300">
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Python 3.12 with strict typing</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />FastAPI with async/await</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />DuckDB for analytical queries</li>
                </ul>
              </div>
              <div>
                <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">{t.about.frontend}</h3>
                <ul className="space-y-1.5 text-sm text-slate-300">
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />React 19 with TypeScript</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />TailwindCSS v4</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Recharts for data visualization</li>
                </ul>
              </div>
              <div>
                <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">{t.about.aiMl}</h3>
                <ul className="space-y-1.5 text-sm text-slate-300">
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Claude Sonnet (Anthropic API)</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Tiered query routing</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Daily AI-generated insights</li>
                </ul>
              </div>
              <div>
                <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">{t.about.infrastructure}</h3>
                <ul className="space-y-1.5 text-sm text-slate-300">
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Cloudflare R2 (object storage)</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />GitHub Actions (CI/CD)</li>
                  <li className="flex items-center gap-2"><span className="h-1.5 w-1.5 rounded-full bg-brand-500" />Medallion architecture</li>
                </ul>
              </div>
            </div>
          )}
        </section>

        {/* Author */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.author}
          </h2>
          <p className="text-sm text-slate-300">
            {t.about.authorBio}
          </p>
        </section>
      </div>
    </div>
  );
}
