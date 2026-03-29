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
          <a
            href={cfg.app.github_url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 text-sm text-brand-400 hover:text-brand-300"
          >
            <svg className="h-4 w-4" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z" />
            </svg>
            GitHub
          </a>
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
