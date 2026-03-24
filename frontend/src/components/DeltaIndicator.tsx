interface DeltaIndicatorProps {
  current: number;
  previous: number | null;
  unit?: string;
}

export function DeltaIndicator({ current, previous, unit = '' }: DeltaIndicatorProps) {
  if (previous === null || previous === undefined) {
    return <span className="text-xs text-slate-500">--</span>;
  }

  const delta = current - previous;
  const pctChange = previous !== 0 ? (delta / Math.abs(previous)) * 100 : 0;
  const isUp = delta > 0;
  const isZero = delta === 0;

  if (isZero) {
    return <span className="text-xs text-slate-400">0.00{unit}</span>;
  }

  return (
    <span
      className={`inline-flex items-center gap-0.5 text-xs font-medium ${isUp ? 'text-emerald-400' : 'text-red-400'}`}
      aria-label={`${isUp ? 'increased' : 'decreased'} by ${Math.abs(delta).toFixed(2)}`}
    >
      <span aria-hidden="true">{isUp ? '\u25B2' : '\u25BC'}</span>
      <span>{Math.abs(delta).toFixed(2)}{unit}</span>
      <span className="text-slate-500 ml-1">({pctChange >= 0 ? '+' : ''}{pctChange.toFixed(1)}%)</span>
    </span>
  );
}
