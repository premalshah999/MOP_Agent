import type { ChatbotMapIntent } from '@/types/chat';
import MapPreview from './MapPreview';


interface ChatbotMapRendererProps {
  mapIntent: ChatbotMapIntent;
  rows: Record<string, unknown>[];
  loading: boolean;
  error: string | null;
}

const ATLAS_TYPES = new Set([
  'atlas-single-metric',
  'atlas-comparison',
  'atlas-within-state',
  'single-state-spotlight',
  'single-state-ranked-subregions',
  'agency-choropleth',
  'top-n-highlight',
]);

function getNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value.replace(/,/g, '').trim());
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function formatValue(metric: string | undefined, value: unknown): string {
  const numeric = getNumber(value);
  if (numeric === null) return String(value ?? 'N/A');
  const lowered = (metric ?? '').toLowerCase();
  const moneyLike = /(contracts|grants|payments|wage|liabilit|assets|revenue|expenses|flow|amount|position|cash)/i.test(lowered);
  const percentLike = /(poverty|education|owner|renter|white|black|asian|hispanic|satisfied|risk_averse)/i.test(lowered);
  const countLike = /(employees|residents|population|household)/i.test(lowered) && !moneyLike;

  if (moneyLike) {
    const abs = Math.abs(numeric);
    const sign = numeric < 0 ? '-' : '';
    if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) return `${sign}$${Math.round(abs).toLocaleString()}`;
    return `${sign}$${abs.toFixed(2)}`;
  }

  if (percentLike) {
    if (Math.abs(numeric) <= 1.5) return `${(numeric * 100).toFixed(1)}%`;
    return `${numeric.toFixed(1)}%`;
  }

  if (countLike) {
    return `${Math.round(numeric).toLocaleString()}`;
  }

  return numeric.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function humanizeMetric(metric: string | undefined): string {
  if (!metric) return 'Metric';
  if (metric === 'spending_total') return 'Default federal spending';
  return metric.replace(/_/g, ' ');
}

function prettyText(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return 'Selected geography';
  if (/^[A-Z]{2}-\d{2}$/.test(trimmed)) return trimmed;
  if (/^[A-Z]{2,5}$/.test(trimmed)) return trimmed;
  return trimmed
    .split(/\s+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}

function resolveMetric(mapIntent: ChatbotMapIntent, rows: Record<string, unknown>[]): string | null {
  if (mapIntent.metric && rows.some((row) => mapIntent.metric! in row)) return mapIntent.metric;
  const first = rows[0];
  if (!first) return null;
  for (const key of Object.keys(first)) {
    if (getNumber(first[key]) !== null) return key;
  }
  return null;
}

function rowLabel(row: Record<string, unknown>): string {
  if (typeof row.county === 'string' && typeof row.state === 'string') return `${prettyText(row.county)}, ${prettyText(row.state)}`;
  if (typeof row.cd_118 === 'string') return row.cd_118;
  if (typeof row.state === 'string') return prettyText(row.state);
  if (typeof row.agency === 'string') return prettyText(row.agency);
  return 'Selected geography';
}

function findFocusedRow(mapIntent: ChatbotMapIntent, sorted: Record<string, unknown>[]): Record<string, unknown> | null {
  if (!sorted.length) return null;
  if (mapIntent.mapType === 'single-state-spotlight' && mapIntent.state) {
    const stateLower = mapIntent.state.toLowerCase();
    return (
      sorted.find((row) => typeof row.state === 'string' && row.state.toLowerCase() === stateLower) ??
      null
    );
  }
  return null;
}

function rankRows(rows: Record<string, unknown>[], metric: string): Record<string, unknown>[] {
  return [...rows].sort((a, b) => (getNumber(b[metric]) ?? -Infinity) - (getNumber(a[metric]) ?? -Infinity));
}

function levelLabel(level: ChatbotMapIntent['level']): string {
  if (level === 'county') return 'county';
  if (level === 'congress') return 'district';
  return 'state';
}

function supportingText(mapIntent: ChatbotMapIntent): string {
  switch (mapIntent.mapType) {
    case 'single-state-spotlight':
      return 'This view keeps the national context in frame while zooming directly into the answer state.';
    case 'single-state-ranked-subregions':
    case 'atlas-within-state':
      return 'This view narrows the geography to one state so the answer county or district is easier to read in local context.';
    case 'atlas-comparison':
      return 'This view highlights the requested comparison geographies while keeping the broader distribution visible.';
    case 'top-n-highlight':
      return 'This view pairs a choropleth with a compact leaderboard so the strongest locations stand out immediately.';
    case 'agency-choropleth':
      return 'This view keeps the selected agency metric on the map so you can see where that department is strongest geographically.';
    default:
      return 'This view provides the geographic context behind the answer.';
  }
}

function InsightPanel({ mapIntent, rows }: { mapIntent: ChatbotMapIntent; rows: Record<string, unknown>[] }) {
  const metric = resolveMetric(mapIntent, rows);
  if (!metric) return null;

  const sorted = rankRows(rows, metric);
  const focusedRow = findFocusedRow(mapIntent, sorted);
  const leader = focusedRow ?? sorted[0];
  const topFive = sorted.slice(0, Math.min(sorted.length, mapIntent.topN ?? 5, 5));
  const values = sorted.map((row) => getNumber(row[metric])).filter((value): value is number => value !== null);
  const spread = values.length >= 2 ? Math.max(...values) - Math.min(...values) : null;

  return (
    <aside className="space-y-4 rounded-[10px] border border-black/5 bg-white/86 p-4 shadow-[0_10px_28px_rgba(15,23,42,0.04)] backdrop-blur">
      <div>
        <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">
          View type
        </p>
        <p className="mt-2 text-[13px] leading-6 text-[var(--ink)]">{supportingText(mapIntent)}</p>
      </div>

      <div className="rounded-[8px] border border-black/5 bg-[var(--surface)]/92 px-4 py-3">
        <p className="text-[10px] uppercase tracking-[0.22em] text-[var(--muted)]">
          {focusedRow ? 'Focus geography' : 'Lead result'}
        </p>
        <div className="mt-2 text-[14px] font-semibold text-[var(--ink)]">{rowLabel(leader)}</div>
        <div className="mt-1 text-[22px] font-semibold tracking-tight text-[var(--ink)]">
          {formatValue(metric, leader[metric])}
        </div>
        <div className="mt-2 text-[12px] leading-6 text-[var(--muted)]">
          {humanizeMetric(metric)} · {levelLabel(mapIntent.level)}
        </div>
      </div>

      {topFive.length > 1 && (
        <div>
          <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">
            Top results
          </p>
          <div className="mt-3 space-y-2">
            {topFive.map((row, index) => (
              <div
                key={`${rowLabel(row)}-${index}`}
                className="flex items-center justify-between gap-3 rounded-[8px] border border-black/5 bg-white px-3 py-2"
              >
                <div className="min-w-0">
                  <div className="text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">
                    #{index + 1}
                  </div>
                  <div className="truncate text-[13px] font-medium text-[var(--ink)]">{rowLabel(row)}</div>
                </div>
                <div className="shrink-0 text-[12px] font-semibold text-[var(--ink)]">
                  {formatValue(metric, row[metric])}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="rounded-[8px] border border-dashed border-black/8 px-4 py-3 text-[12px] leading-6 text-[var(--muted)]">
        {values.length > 1 ? (
          <>
            Across the {rows.length} mapped results here, values span from{' '}
            <strong className="text-[var(--ink)]">{formatValue(metric, Math.min(...values))}</strong> to{' '}
            <strong className="text-[var(--ink)]">{formatValue(metric, Math.max(...values))}</strong>.
            {spread !== null && (
              <>
                {' '}The total spread is{' '}
                <strong className="text-[var(--ink)]">{formatValue(metric, spread)}</strong>.
              </>
            )}
          </>
        ) : (
          <>This map is centered on a single resolved geography from the answer.</>
        )}
      </div>
    </aside>
  );
}

export function ChatbotMapRenderer({ mapIntent, rows, loading, error }: ChatbotMapRendererProps) {
  if (loading) {
    return (
      <div className="flex h-[min(74vh,780px)] items-center justify-center rounded-[16px] border border-black/6 bg-[var(--surface)]/88 shadow-[0_14px_42px_rgba(15,23,42,0.08)]">
        <div className="text-[13px] text-[var(--muted)]">Loading map data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-[min(74vh,780px)] items-center justify-center rounded-[16px] border border-black/6 bg-[var(--surface)]/88 px-6 text-center text-[13px] text-[var(--muted)] shadow-[0_14px_42px_rgba(15,23,42,0.08)]">
        {error}
      </div>
    );
  }

  if (!rows.length) {
    return (
      <div className="flex h-[min(74vh,780px)] items-center justify-center rounded-[16px] border border-black/6 bg-[var(--surface)]/88 px-6 text-center text-[13px] text-[var(--muted)] shadow-[0_14px_42px_rgba(15,23,42,0.08)]">
        No geographic rows were available for this answer yet.
      </div>
    );
  }

  if (ATLAS_TYPES.has(mapIntent.mapType)) {
    return (
      <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_340px]">
        <MapPreview rows={rows} variant="modal" mapIntent={mapIntent} />
        <InsightPanel mapIntent={mapIntent} rows={rows} />
      </div>
    );
  }

  return (
    <div className="flex h-[min(74vh,780px)] items-center justify-center rounded-[16px] border border-black/6 bg-[var(--surface)]/88 px-6 text-center text-[13px] text-[var(--muted)] shadow-[0_14px_42px_rgba(15,23,42,0.08)]">
      This answer does not have a dedicated interactive map renderer in this release yet.
    </div>
  );
}
