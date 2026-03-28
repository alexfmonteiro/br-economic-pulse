import type { ChartGranularity } from '@/lib/api';
import type { Language } from '@/lib/i18n';

const GRANULARITIES: {
  value: ChartGranularity;
  label: { en: string; pt: string };
  title: { en: string; pt: string };
}[] = [
  { value: 'day', label: { en: 'D', pt: 'D' }, title: { en: 'Day', pt: 'Dia' } },
  { value: 'week', label: { en: 'W', pt: 'S' }, title: { en: 'Week', pt: 'Semana' } },
  { value: 'month', label: { en: 'M', pt: 'M' }, title: { en: 'Month', pt: 'Mês' } },
  { value: 'year', label: { en: 'Y', pt: 'A' }, title: { en: 'Year', pt: 'Ano' } },
];

interface GranularitySelectorProps {
  value: ChartGranularity;
  onChange: (g: ChartGranularity) => void;
  language?: Language;
}

export function GranularitySelector({ value, onChange, language = 'en' }: GranularitySelectorProps) {
  return (
    <div className="inline-flex items-center gap-0.5 rounded-md bg-slate-800/80 p-0.5">
      {GRANULARITIES.map((g) => (
        <button
          key={g.value}
          onClick={() => onChange(g.value)}
          title={g.title[language]}
          className={`cursor-pointer rounded px-1.5 py-0.5 text-[10px] font-medium transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none ${
            value === g.value
              ? 'bg-brand-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50'
          }`}
        >
          {g.label[language]}
        </button>
      ))}
    </div>
  );
}
