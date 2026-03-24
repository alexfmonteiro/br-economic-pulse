import { useState } from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import { Dashboard } from '@/pages/Dashboard';
import { AskPage } from '@/pages/AskPage';
import { AnalyticsPage } from '@/pages/AnalyticsPage';
import { QualityPage } from '@/pages/QualityPage';
import { AboutPage } from '@/pages/AboutPage';
import { useLanguage } from '@/lib/LanguageContext';

function LanguageToggle() {
  const { language, setLanguage } = useLanguage();

  return (
    <button
      onClick={() => setLanguage(language === 'en' ? 'pt' : 'en')}
      className="cursor-pointer ml-auto flex items-center gap-1 rounded-md border border-slate-700/50 px-2 py-1 text-xs font-medium text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none"
      aria-label={`Switch language to ${language === 'en' ? 'Portuguese' : 'English'}`}
    >
      <span className={language === 'pt' ? 'text-slate-200 font-semibold' : ''}>PT</span>
      <span className="text-slate-600">/</span>
      <span className={language === 'en' ? 'text-slate-200 font-semibold' : ''}>EN</span>
    </button>
  );
}

function NavBar() {
  const { t } = useLanguage();
  const [open, setOpen] = useState(false);

  const linkClass = ({ isActive }: { isActive: boolean }) =>
    `px-3 py-1.5 rounded-md text-sm font-medium transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none ${
      isActive
        ? 'bg-brand-600 text-white'
        : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50'
    }`;

  const mobileLinkClass = ({ isActive }: { isActive: boolean }) =>
    `block px-3 py-2 rounded-md text-sm font-medium transition-colors ${
      isActive
        ? 'bg-brand-600 text-white'
        : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50'
    }`;

  return (
    <nav className="border-b border-slate-800 bg-slate-900/50 backdrop-blur-sm sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex items-center h-14 gap-1">
        <NavLink to="/" className="text-lg font-bold text-slate-100 mr-4 shrink-0 whitespace-nowrap">
          BR Economic Pulse
        </NavLink>

        {/* Desktop nav */}
        <div className="hidden md:flex items-center gap-1">
          <NavLink to="/" end className={linkClass}>{t.nav.home}</NavLink>
          <NavLink to="/dashboard" className={linkClass}>{t.nav.dashboard}</NavLink>
          <NavLink to="/ask" className={linkClass}>{t.nav.askAi}</NavLink>
          <NavLink to="/quality" className={linkClass}>{t.nav.quality}</NavLink>
          <NavLink to="/about" className={linkClass}>{t.nav.about}</NavLink>
        </div>

        <div className="ml-auto flex items-center gap-2">
          <LanguageToggle />
          {/* Hamburger button — mobile only */}
          <button
            onClick={() => setOpen(!open)}
            className="md:hidden p-1.5 rounded-md text-slate-400 hover:text-slate-200 hover:bg-slate-700/50 transition-colors"
            aria-label="Toggle menu"
          >
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" strokeWidth="1.5" stroke="currentColor">
              {open ? (
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5" />
              )}
            </svg>
          </button>
        </div>
      </div>

      {/* Mobile dropdown */}
      {open && (
        <div className="md:hidden border-t border-slate-800 px-4 py-2 space-y-1 bg-slate-900/95 backdrop-blur-sm">
          <NavLink to="/" end className={mobileLinkClass} onClick={() => setOpen(false)}>{t.nav.home}</NavLink>
          <NavLink to="/dashboard" className={mobileLinkClass} onClick={() => setOpen(false)}>{t.nav.dashboard}</NavLink>
          <NavLink to="/ask" className={mobileLinkClass} onClick={() => setOpen(false)}>{t.nav.askAi}</NavLink>
          <NavLink to="/quality" className={mobileLinkClass} onClick={() => setOpen(false)}>{t.nav.quality}</NavLink>
          <NavLink to="/about" className={mobileLinkClass} onClick={() => setOpen(false)}>{t.nav.about}</NavLink>
        </div>
      )}
    </nav>
  );
}

function App() {
  return (
    <BrowserRouter>
      <NavBar />
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/dashboard" element={<AnalyticsPage />} />
        <Route path="/ask" element={<AskPage />} />
        <Route path="/quality" element={<QualityPage />} />
        <Route path="/about" element={<AboutPage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
