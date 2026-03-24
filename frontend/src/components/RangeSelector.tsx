import { TIME_RANGES, type TimeRange } from '@/lib/api';

interface RangeSelectorProps {
  value: TimeRange;
  onChange: (range: TimeRange) => void;
}

export function RangeSelector({ value, onChange }: RangeSelectorProps) {
  return (
    <div className="inline-flex items-center gap-1 rounded-lg bg-slate-800/80 p-1">
      {TIME_RANGES.map((r) => (
        <button
          key={r.value}
          onClick={() => onChange(r.value)}
          className={`cursor-pointer rounded-md px-2.5 py-1 text-xs font-medium transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none ${
            value === r.value
              ? 'bg-brand-600 text-white'
              : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50'
          }`}
        >
          {r.label}
        </button>
      ))}
    </div>
  );
}
