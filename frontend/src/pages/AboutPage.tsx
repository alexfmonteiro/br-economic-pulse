import { useLanguage } from '@/lib/LanguageContext';

export function AboutPage() {
  const { t } = useLanguage();

  return (
    <div className="min-h-[calc(100vh-3.5rem)] p-4 sm:p-6 lg:p-8 max-w-4xl mx-auto">
      <header className="mb-8">
        <h1 className="text-2xl font-bold text-slate-100">{t.about.title}</h1>
        <p className="text-sm text-slate-500 mt-1">
          {t.about.subtitle}
        </p>
      </header>

      <div className="space-y-8">
        {/* Architecture Diagram */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.architecture}
          </h2>
          <div className="overflow-x-auto">
            <pre className="text-xs text-slate-400 font-mono leading-relaxed">
{`  Data Sources          Medallion Pipeline            API Layer         Frontend
  +-----------+      +---------------------------+   +-----------+   +-----------+
  | BCB       |--+   |                           |   |           |   |           |
  +-----------+  |   |  Bronze    Silver    Gold  |   |  FastAPI  |   |   React   |
                 +-->|  (raw) --> (clean) --> (agg)|-->|  + DuckDB |-->|  + Vite   |
  +-----------+  |   |                           |   |           |   |           |
  | IBGE      |--+   +---------------------------+   +-----------+   +-----------+
  +-----------+  |           |                           |
                 |   +-------v--------+          +-------v--------+
  +-----------+  |   | Quality Checks |          |  Claude Sonnet |
  | Tesouro   |--+   | (validation)   |          |  (Insights +   |
  +-----------+      +----------------+          |   Query Agent) |
                                                 +----------------+

  Storage: Cloudflare R2 (production) / Local filesystem (dev)
  Orchestration: GitHub Actions + Webhook triggers`}</pre>
          </div>
        </section>

        {/* Tech Stack */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.techStack}
          </h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
            <div>
              <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
                {t.about.backend}
              </h3>
              <ul className="space-y-1.5 text-sm text-slate-300">
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Python 3.12 with strict typing
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  FastAPI with async/await
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  DuckDB for analytical queries
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Pydantic v2 for data validation
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  structlog for structured logging
                </li>
              </ul>
            </div>
            <div>
              <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
                {t.about.frontend}
              </h3>
              <ul className="space-y-1.5 text-sm text-slate-300">
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  React 19 with TypeScript
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Vite for build tooling
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  TailwindCSS v4 (dark theme)
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Recharts for data visualization
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  TanStack Query for data fetching
                </li>
              </ul>
            </div>
            <div>
              <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
                {t.about.aiMl}
              </h3>
              <ul className="space-y-1.5 text-sm text-slate-300">
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Claude Sonnet (Anthropic API)
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Tiered query routing (Tier 1 regex + Tier 3 LLM)
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Daily AI-generated insight digests
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  XML data fencing for prompt safety
                </li>
              </ul>
            </div>
            <div>
              <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">
                {t.about.infrastructure}
              </h3>
              <ul className="space-y-1.5 text-sm text-slate-300">
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Cloudflare R2 (object storage)
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Terraform (IaC)
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  GitHub Actions (CI/CD)
                </li>
                <li className="flex items-center gap-2">
                  <span className="h-1.5 w-1.5 rounded-full bg-brand-500" />
                  Medallion architecture (Bronze/Silver/Gold)
                </li>
              </ul>
            </div>
          </div>
        </section>

        {/* Data Sources */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.dataSources}
          </h2>
          <div className="space-y-4">
            <div className="flex items-start gap-4 rounded-lg border border-slate-700/30 bg-slate-900/30 p-4">
              <div className="flex-1">
                <h3 className="text-sm font-semibold text-slate-200">Banco Central do Brasil (BCB)</h3>
                <p className="text-xs text-slate-400 mt-1">
                  SELIC rate, IPCA inflation, and USD/BRL exchange rate from the BCB open data API.
                </p>
                <a
                  href="https://www3.bcb.gov.br/sgspub/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-brand-400 hover:text-brand-300 mt-1 inline-block"
                >
                  bcb.gov.br/sgspub
                </a>
              </div>
            </div>

            <div className="flex items-start gap-4 rounded-lg border border-slate-700/30 bg-slate-900/30 p-4">
              <div className="flex-1">
                <h3 className="text-sm font-semibold text-slate-200">
                  Instituto Brasileiro de Geografia e Estatistica (IBGE)
                </h3>
                <p className="text-xs text-slate-400 mt-1">
                  Quarterly GDP and unemployment data from the IBGE SIDRA API.
                </p>
                <a
                  href="https://sidra.ibge.gov.br/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-brand-400 hover:text-brand-300 mt-1 inline-block"
                >
                  sidra.ibge.gov.br
                </a>
              </div>
            </div>

            <div className="flex items-start gap-4 rounded-lg border border-slate-700/30 bg-slate-900/30 p-4">
              <div className="flex-1">
                <h3 className="text-sm font-semibold text-slate-200">Tesouro Nacional</h3>
                <p className="text-xs text-slate-400 mt-1">
                  Treasury bond yields and pricing.
                </p>
                <a
                  href="https://www.tesourotransparente.gov.br/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs text-brand-400 hover:text-brand-300 mt-1 inline-block"
                >
                  tesourotransparente.gov.br
                </a>
              </div>
            </div>
          </div>
        </section>

        {/* Author */}
        <section className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6">
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
            {t.about.author}
          </h2>
          <div>
            <p className="text-sm text-slate-300">
              {t.about.authorBio}
            </p>
            <p className="text-xs text-slate-500 mt-3">
              {t.about.authorArch}
            </p>
          </div>
        </section>
      </div>
    </div>
  );
}
