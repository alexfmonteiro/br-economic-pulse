import { Link } from 'react-router-dom';
import { useLanguage } from '@/lib/LanguageContext';
import { useDomain, localize } from '@/lib/domain';

function FeatureCard({ title, description, icon }: { title: string; description: string; icon: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-6 hover:border-slate-600/50 transition-colors">
      <div className="mb-3 text-brand-400">{icon}</div>
      <h3 className="text-sm font-semibold text-slate-200 mb-2">{title}</h3>
      <p className="text-sm text-slate-400 leading-relaxed">{description}</p>
    </div>
  );
}

function StepCard({ number, title, description }: { number: number; title: string; description: string }) {
  return (
    <div className="text-center">
      <div className="mx-auto mb-4 flex h-10 w-10 items-center justify-center rounded-full bg-brand-600/20 border border-brand-500/30 text-brand-400 text-sm font-bold">
        {number}
      </div>
      <h3 className="text-sm font-semibold text-slate-200 mb-2">{title}</h3>
      <p className="text-sm text-slate-400 leading-relaxed">{description}</p>
    </div>
  );
}

function DataSourceBadge({ name, description }: { name: string; description: string }) {
  return (
    <div className="rounded-xl border border-slate-700/50 bg-slate-800/50 p-5">
      <h4 className="text-sm font-bold text-slate-200 mb-1">{name}</h4>
      <p className="text-xs text-slate-400 leading-relaxed">{description}</p>
    </div>
  );
}

// Simple SVG icons to avoid external deps
const icons: Record<string, React.ReactNode> = {
  chart: (
    <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
    </svg>
  ),
  ai: (
    <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456z" />
    </svg>
  ),
  alert: (
    <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M14.857 17.082a23.848 23.848 0 005.454-1.31A8.967 8.967 0 0118 9.75v-.7V9A6 6 0 006 9v.75a8.967 8.967 0 01-2.312 6.022c1.733.64 3.56 1.085 5.455 1.31m5.714 0a24.255 24.255 0 01-5.714 0m5.714 0a3 3 0 11-5.714 0" />
    </svg>
  ),
  globe: (
    <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M12 21a9.004 9.004 0 008.716-6.747M12 21a9.004 9.004 0 01-8.716-6.747M12 21c2.485 0 4.5-4.03 4.5-9S14.485 3 12 3m0 18c-2.485 0-4.5-4.03-4.5-9S9.515 3 12 3m0 0a8.997 8.997 0 017.843 4.582M12 3a8.997 8.997 0 00-7.843 4.582m15.686 0A11.953 11.953 0 0112 10.5c-2.998 0-5.74-1.1-7.843-2.918m15.686 0A8.959 8.959 0 0121 12c0 .778-.099 1.533-.284 2.253m0 0A17.919 17.919 0 0112 16.5c-3.162 0-6.133-.815-8.716-2.247m0 0A9.015 9.015 0 013 12c0-1.605.42-3.113 1.157-4.418" />
    </svg>
  ),
  document: (
    <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
    </svg>
  ),
  download: (
    <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
    </svg>
  ),
};

export function LandingPage() {
  const { language, t } = useLanguage();
  const cfg = useDomain();
  const l = (ls: { en: string; pt: string }) => localize(ls, language);

  return (
    <div className="min-h-[calc(100vh-3.5rem)]">
      {/* Hero */}
      <section className="px-4 sm:px-6 lg:px-8 pt-16 pb-20 max-w-7xl mx-auto text-center">
        <img src="/veredas_logo.png" alt="Veredas" className="h-16 w-16 mx-auto mb-6 rounded-xl" />
        <h1 className="text-4xl sm:text-5xl font-bold text-slate-100 tracking-tight mb-4">
          {l(cfg.landing.hero_title)}
        </h1>
        <p className="text-lg text-slate-400 max-w-2xl mx-auto mb-8 leading-relaxed">
          {l(cfg.landing.hero_subtitle)}
        </p>
        <div className="flex items-center justify-center gap-3">
          <Link
            to="/ask"
            className="inline-flex items-center gap-2 rounded-lg bg-brand-600 px-6 py-3 text-sm font-semibold text-white hover:bg-brand-500 transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none"
          >
            {t.landing.ctaPrimary}
            <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth="2" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5L21 12m0 0l-7.5 7.5M21 12H3" />
            </svg>
          </Link>
          <Link
            to="/dashboard"
            className="inline-flex items-center gap-2 rounded-lg border border-slate-700/50 bg-slate-800/50 px-6 py-3 text-sm font-medium text-slate-300 hover:text-slate-100 hover:border-slate-600/50 transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none"
          >
            {t.landing.ctaSecondary}
          </Link>
        </div>
      </section>

      {/* Features Grid */}
      <section className="px-4 sm:px-6 lg:px-8 py-16 max-w-7xl mx-auto">
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider text-center mb-8">
          {t.landing.featuresTitle}
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 max-w-3xl mx-auto">
          {cfg.landing.features.map((f, i) => (
            <FeatureCard
              key={i}
              title={l(f.title)}
              description={l(f.description)}
              icon={icons[f.icon] ?? icons.chart}
            />
          ))}
        </div>
      </section>

      {/* How It Works */}
      <section className="px-4 sm:px-6 lg:px-8 py-16 max-w-7xl mx-auto">
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider text-center mb-10">
          {t.landing.howItWorksTitle}
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-8">
          <StepCard number={1} title={t.landing.step1Title} description={t.landing.step1Desc} />
          <StepCard number={2} title={t.landing.step2Title} description={t.landing.step2Desc} />
          <StepCard number={3} title={t.landing.step3Title} description={t.landing.step3Desc} />
        </div>
      </section>

      {/* Data Sources */}
      <section className="px-4 sm:px-6 lg:px-8 py-16 max-w-7xl mx-auto">
        <h2 className="text-sm font-semibold text-slate-300 uppercase tracking-wider text-center mb-8">
          {t.landing.dataSourcesTitle}
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          {cfg.data_sources.map((ds) => (
            <DataSourceBadge key={ds.id} name={ds.name} description={l(ds.description)} />
          ))}
        </div>
      </section>

      {/* Open Source */}
      <section className="px-4 sm:px-6 lg:px-8 py-16 max-w-7xl mx-auto text-center">
        <h2 className="text-lg font-bold text-slate-200 mb-2">{t.landing.openSourceTitle}</h2>
        <p className="text-sm text-slate-400 max-w-lg mx-auto mb-6">
          {t.landing.openSourceDesc}
        </p>
        <Link
          to="/quality"
          className="inline-flex items-center gap-2 rounded-lg border border-slate-700/50 bg-slate-800/50 px-5 py-2.5 text-sm font-medium text-slate-300 hover:text-slate-100 hover:border-slate-600/50 transition-colors"
        >
          <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          {t.landing.viewOnGithub}
        </Link>
      </section>

      {/* Footer */}
      <footer className="border-t border-slate-800 px-4 sm:px-6 lg:px-8 py-8 max-w-7xl mx-auto">
        <div className="flex flex-col sm:flex-row items-center justify-between gap-4 text-xs text-slate-500">
          <span className="font-medium text-slate-400">{cfg.app.title}</span>
          <div className="flex items-center gap-4">
            <Link to="/about" className="hover:text-slate-300 transition-colors">{t.nav.about}</Link>
            <Link to="/ask" className="hover:text-slate-300 transition-colors">{t.nav.askAi}</Link>
            <Link to="/dashboard" className="hover:text-slate-300 transition-colors">{t.nav.dashboard}</Link>
            <Link to="/quality" className="hover:text-slate-300 transition-colors">
              {language === 'pt' ? 'Qualidade dos Dados' : 'Data Quality'}
            </Link>
          </div>
        </div>
      </footer>
    </div>
  );
}
