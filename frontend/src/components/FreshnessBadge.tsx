/** Default thresholds (hours) — used when no per-series value is provided. */
const DEFAULT_FRESH = 26;
const DEFAULT_STALE = 120;

interface FreshnessBadgeProps {
  lastUpdated: string | null;
  /** Per-series "fresh" threshold in hours.  Stale = 1×, Critical = 2×. */
  freshnessHours?: number;
}

function getStatus(
  lastUpdated: string | null,
  freshnessHours?: number,
): { label: string; className: string } {
  if (!lastUpdated) {
    return { label: 'No data', className: 'bg-red-500/20 text-red-400 border-red-500/30' };
  }

  const hours = (Date.now() - new Date(lastUpdated).getTime()) / 3_600_000;
  const freshThreshold = freshnessHours ?? DEFAULT_FRESH;
  const staleThreshold = freshnessHours ? freshnessHours * 2 : DEFAULT_STALE;

  if (hours < freshThreshold) {
    return { label: 'Fresh', className: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30' };
  }
  if (hours < staleThreshold) {
    return { label: 'Stale', className: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30' };
  }
  return { label: 'Critical', className: 'bg-red-500/20 text-red-400 border-red-500/30' };
}

export function FreshnessBadge({ lastUpdated, freshnessHours }: FreshnessBadgeProps) {
  const { label, className } = getStatus(lastUpdated, freshnessHours);

  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${className}`}>
      {label}
    </span>
  );
}
