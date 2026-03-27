import { createContext, useContext, useState, useEffect, createElement, type ReactNode } from 'react';
import type { Language } from './i18n';

// --- Types matching the API response ---

export interface LocalizedStr {
  en: string;
  pt: string;
}

export interface DomainInfo {
  name: string;
  description: string;
  country: string;
  country_code: string;
  default_language: string;
  supported_languages: string[];
}

export interface DomainAIConfig {
  scope_description: LocalizedStr;
  example_indicators: LocalizedStr;
}

export interface DomainDataSource {
  id: string;
  name: string;
  url: string;
  description: LocalizedStr;
}

export interface DomainSeriesConfig {
  label: string;
  unit: string;
  source: string;
  color: string;
  freshness_hours: number;
  domain: string;
  description: LocalizedStr;
  keywords: string[];
}

export interface DomainLandingFeature {
  icon: string;
  title: LocalizedStr;
  description: LocalizedStr;
}

export interface DomainAppConfig {
  title: string;
  meta_description: LocalizedStr;
  github_url: string;
}

export interface DomainLandingConfig {
  hero_title: LocalizedStr;
  hero_subtitle: LocalizedStr;
  features: DomainLandingFeature[];
}

export interface DomainConfig {
  domain: DomainInfo;
  ai: DomainAIConfig;
  data_sources: DomainDataSource[];
  series: Record<string, DomainSeriesConfig>;
  app: DomainAppConfig;
  landing: DomainLandingConfig;
}

// --- Fetcher ---

const API_BASE = import.meta.env.VITE_API_URL ?? '';

export async function fetchDomainConfig(domainId?: string): Promise<DomainConfig> {
  const params = domainId ? `?domain_id=${encodeURIComponent(domainId)}` : '';
  const res = await fetch(`${API_BASE}/api/config/domain${params}`);
  if (!res.ok) throw new Error(`Failed to load domain config: ${res.status}`);
  return res.json() as Promise<DomainConfig>;
}

// --- Context ---

const DomainConfigContext = createContext<DomainConfig | null>(null);

export function DomainProvider({ children, domainId }: { children: ReactNode; domainId?: string }) {
  const [config, setConfig] = useState<DomainConfig | null>(null);

  useEffect(() => {
    fetchDomainConfig(domainId).then(setConfig).catch((err) => {
      console.error('Failed to load domain config:', err);
    });
  }, [domainId]);

  if (!config) {
    return createElement('div', {
      className: 'min-h-screen flex items-center justify-center bg-slate-900',
    }, createElement('div', {
      className: 'h-6 w-6 rounded-full border-2 border-brand-500 border-t-transparent animate-spin',
    }));
  }

  return createElement(DomainConfigContext.Provider, { value: config }, children);
}

// --- Hook ---

export function useDomain(): DomainConfig {
  const ctx = useContext(DomainConfigContext);
  if (!ctx) throw new Error('useDomain must be used within DomainProvider');
  return ctx;
}

/** Resolve a LocalizedStr to the given language. */
export function localize(ls: LocalizedStr, lang: Language): string {
  return ls[lang];
}
