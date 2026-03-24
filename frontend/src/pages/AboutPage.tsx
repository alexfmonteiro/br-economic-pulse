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
          <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-6">
            {t.about.architecture}
          </h2>

          {/* Flow diagram */}
          <div className="flex flex-col gap-6">
            {/* Main pipeline row */}
            <div className="grid grid-cols-1 sm:grid-cols-4 gap-3 items-stretch">
              {/* Data Sources */}
              <div className="rounded-lg border border-slate-700/50 bg-slate-900/60 p-4">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-3">Data Sources</p>
                <div className="flex flex-col gap-2">
                  <span className="inline-flex items-center rounded-md bg-blue-500/10 border border-blue-500/20 px-2.5 py-1 text-xs text-blue-400">BCB</span>
                  <span className="inline-flex items-center rounded-md bg-blue-500/10 border border-blue-500/20 px-2.5 py-1 text-xs text-blue-400">IBGE</span>
                  <span className="inline-flex items-center rounded-md bg-blue-500/10 border border-blue-500/20 px-2.5 py-1 text-xs text-blue-400">Tesouro</span>
                </div>
              </div>

              {/* Medallion Pipeline */}
              <div className="rounded-lg border border-slate-700/50 bg-slate-900/60 p-4">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-3">Medallion Pipeline</p>
                <div className="flex items-center gap-1.5 text-xs">
                  <span className="rounded bg-amber-500/15 border border-amber-500/25 px-2 py-1 text-amber-400">Bronze</span>
                  <span className="text-slate-600">&rarr;</span>
                  <span className="rounded bg-slate-500/15 border border-slate-500/25 px-2 py-1 text-slate-300">Silver</span>
                  <span className="text-slate-600">&rarr;</span>
                  <span className="rounded bg-yellow-500/15 border border-yellow-500/25 px-2 py-1 text-yellow-400">Gold</span>
                </div>
                <p className="text-[10px] text-slate-600 mt-2">raw &rarr; clean &rarr; aggregated</p>
              </div>

              {/* API Layer */}
              <div className="rounded-lg border border-slate-700/50 bg-slate-900/60 p-4">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-3">API Layer</p>
                <div className="flex flex-col gap-2">
                  <span className="inline-flex items-center rounded-md bg-emerald-500/10 border border-emerald-500/20 px-2.5 py-1 text-xs text-emerald-400">FastAPI</span>
                  <span className="inline-flex items-center rounded-md bg-emerald-500/10 border border-emerald-500/20 px-2.5 py-1 text-xs text-emerald-400">DuckDB</span>
                </div>
              </div>

              {/* Frontend */}
              <div className="rounded-lg border border-slate-700/50 bg-slate-900/60 p-4">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-3">Frontend</p>
                <div className="flex flex-col gap-2">
                  <span className="inline-flex items-center rounded-md bg-cyan-500/10 border border-cyan-500/20 px-2.5 py-1 text-xs text-cyan-400">React + Vite</span>
                  <span className="inline-flex items-center rounded-md bg-cyan-500/10 border border-cyan-500/20 px-2.5 py-1 text-xs text-cyan-400">Recharts</span>
                </div>
              </div>
            </div>

            {/* Supporting services */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <div className="rounded-lg border border-dashed border-slate-700/50 bg-slate-900/30 px-4 py-3 text-center">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Quality</p>
                <p className="text-xs text-slate-400">Null rates, ranges, freshness</p>
              </div>
              <div className="rounded-lg border border-dashed border-slate-700/50 bg-slate-900/30 px-4 py-3 text-center">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">AI Agents</p>
                <p className="text-xs text-slate-400">Claude Sonnet &mdash; insights + queries</p>
              </div>
              <div className="rounded-lg border border-dashed border-slate-700/50 bg-slate-900/30 px-4 py-3 text-center">
                <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Infra</p>
                <p className="text-xs text-slate-400">R2 storage, GitHub Actions, Terraform</p>
              </div>
            </div>
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
