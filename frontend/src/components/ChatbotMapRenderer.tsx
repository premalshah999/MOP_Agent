import type { ReactNode } from 'react';
import type { ChatbotMapIntent } from '@/types/chat';
import MapPreview from './MapPreview';
import { VegaChart } from './VegaChart';

interface ChatbotMapRendererProps {
  mapIntent: ChatbotMapIntent;
  mapRows: Record<string, unknown>[];
  insightRows: Record<string, unknown>[];
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
const SPENDING_TYPES = new Set(['single-state-agency', 'spending-breakdown']);
const FLOW_TYPES = new Set(['flow-map', 'flow-state-focused', 'flow-pair', 'flow-within-state']);

function getNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value.replace(/,/g, '').trim());
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function metricKind(metric: string | undefined): 'money' | 'percent' | 'count' | 'generic' {
  const lowered = (metric ?? '').toLowerCase();
  if (/(contracts|grants|payments|wage|liabilit|assets|revenue|expenses|flow|amount|position|cash|spending)/i.test(lowered)) return 'money';
  if (/(poverty|education|owner|renter|white|black|asian|hispanic|satisfied|risk_averse|literacy|constraint|financing|share|ratio|per_1000)/i.test(lowered)) return 'percent';
  if (/(employees|residents|population|household|jobs|count)/i.test(lowered)) return 'count';
  return 'generic';
}

function formatValue(metric: string | undefined, value: unknown): string {
  const numeric = getNumber(value);
  if (numeric === null) return String(value ?? 'N/A');
  const kind = metricKind(metric);

  if (kind === 'money') {
    const abs = Math.abs(numeric);
    const sign = numeric < 0 ? '-' : '';
    if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) return `${sign}$${Math.round(abs).toLocaleString()}`;
    return `${sign}$${abs.toFixed(2)}`;
  }

  if (kind === 'percent') {
    if (Math.abs(numeric) <= 1.5) return `${(numeric * 100).toFixed(1)}%`;
    return `${numeric.toFixed(1)}%`;
  }

  if (kind === 'count') {
    return `${Math.round(numeric).toLocaleString()}`;
  }

  return numeric.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function humanizeMetric(metric: string | undefined): string {
  if (!metric) return 'Metric';
  if (metric === 'spending_total') return 'Default federal spending';
  if (/^black$/i.test(metric)) return 'Black population share';
  if (/^white$/i.test(metric)) return 'White population share';
  if (/^hispanic$/i.test(metric)) return 'Hispanic population share';
  if (/^asian$/i.test(metric)) return 'Asian population share';
  if (/financial_literacy/i.test(metric)) return 'Financial literacy';
  if (/financial_constraint/i.test(metric)) return 'Financial constraint';
  if (/alternative_financing/i.test(metric)) return 'Alternative financing';
  return metric
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
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
  if (typeof row.label === 'string') return prettyText(row.label);
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

function hasGeographicRows(rows: Record<string, unknown>[]): boolean {
  return rows.some((row) =>
    typeof row.state === 'string' ||
    typeof row.county === 'string' ||
    typeof row.cd_118 === 'string' ||
    typeof row.rcpt_state_name === 'string' ||
    typeof row.subawardee_state_name === 'string',
  );
}

function levelLabel(level: ChatbotMapIntent['level']): string {
  if (level === 'county') return 'county';
  if (level === 'congress') return 'district';
  return 'state';
}

function levelPluralLabel(level: ChatbotMapIntent['level']): string {
  if (level === 'county') return 'counties';
  if (level === 'congress') return 'districts';
  return 'states';
}

function supportingText(mapIntent: ChatbotMapIntent): string {
  switch (mapIntent.mapType) {
    case 'single-state-spotlight':
      return 'This view keeps the national context visible while moving into the focal state and summarizing how it sits in the national distribution.';
    case 'single-state-ranked-subregions':
    case 'atlas-within-state':
      return 'This view isolates one state and reads the answer at the county or district level so the internal pattern is visible immediately.';
    case 'atlas-comparison':
      return 'This view keeps the full choropleth but emphasizes the requested comparison geographies so the contrast is easy to read.';
    case 'top-n-highlight':
      return 'This view pairs the choropleth with a compact leaderboard and distribution view so the leading places stand out without losing national context.';
    case 'agency-choropleth':
      return 'This view keeps the selected agency metric on the map so you can see where that department is strongest geographically.';
    default:
      return 'This view provides the geographic evidence behind the answer and complements it with compact analytic context.';
  }
}

function buildBarChartSpec(metric: string, rows: Record<string, unknown>[]): Record<string, unknown> | null {
  const ranked = rankRows(rows, metric)
    .slice(0, 10)
    .map((row) => ({ place: rowLabel(row), value: getNumber(row[metric]) ?? 0 }));
  if (!ranked.length) return null;

  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
    width: 'container',
    height: 280,
    padding: { left: 4, top: 4, right: 12, bottom: 4 },
    data: { values: ranked },
    mark: { type: 'bar' },
    encoding: {
      y: {
        field: 'place',
        type: 'nominal',
        sort: '-x',
        axis: { title: null, labelLimit: 220, labelPadding: 8 },
      },
      x: {
        field: 'value',
        type: 'quantitative',
        axis: { title: null, tickCount: 5, grid: true, format: metricKind(metric) === 'money' ? '~s' : undefined },
      },
      color: { value: '#3458a5' },
    },
    config: { view: { stroke: null } },
  };
}

function buildHistogramSpec(metric: string, rows: Record<string, unknown>[]): Record<string, unknown> | null {
  const values = rows
    .map((row) => getNumber(row[metric]))
    .filter((value): value is number => value !== null);
  if (values.length < 5) return null;

  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
    width: 'container',
    height: 140,
    padding: { left: 4, top: 4, right: 8, bottom: 4 },
    data: { values: values.map((value) => ({ value })) },
    mark: { type: 'bar', color: '#b8622d' },
    encoding: {
      x: {
        field: 'value',
        type: 'quantitative',
        bin: { maxbins: 12 },
        axis: { title: null, grid: false, format: metricKind(metric) === 'money' ? '~s' : undefined },
      },
      y: {
        aggregate: 'count',
        type: 'quantitative',
        axis: { title: null, tickCount: 4, grid: true },
      },
    },
    config: { view: { stroke: null } },
  };
}

function buildStackedAgencySpec(rows: Record<string, unknown>[]): Record<string, unknown> | null {
  const components = ['contracts', 'grants', 'resident_wage']
    .filter((column) => rows.some((row) => getNumber(row[column]) !== null));
  if (!components.length || !rows.some((row) => typeof row.agency === 'string')) return null;

  const ranked = rows
    .map((row) => ({
      agency: rowLabel(row),
      contracts: getNumber(row.contracts) ?? 0,
      grants: getNumber(row.grants) ?? 0,
      resident_wage: getNumber(row.resident_wage) ?? 0,
      spending_total: getNumber(row.spending_total) ?? (
        (getNumber(row.contracts) ?? 0) +
        (getNumber(row.grants) ?? 0) +
        (getNumber(row.resident_wage) ?? 0)
      ),
    }))
    .sort((a, b) => b.spending_total - a.spending_total)
    .slice(0, 10);
  if (!ranked.length) return null;

  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
    width: 'container',
    height: 320,
    data: { values: ranked },
    transform: [{ fold: ['contracts', 'grants', 'resident_wage'], as: ['component', 'value'] }],
    mark: { type: 'bar' },
    encoding: {
      y: { field: 'agency', type: 'nominal', sort: '-x', axis: { title: null, labelLimit: 220 } },
      x: { field: 'value', type: 'quantitative', stack: 'zero', axis: { title: null, format: '~s', grid: true } },
      color: {
        field: 'component',
        type: 'nominal',
        scale: { domain: ['contracts', 'grants', 'resident_wage'], range: ['#243b68', '#3b6fb6', '#b8622d'] },
        legend: { title: null, orient: 'top' },
      },
      tooltip: [
        { field: 'agency', type: 'nominal' },
        { field: 'component', type: 'nominal' },
        { field: 'value', type: 'quantitative', format: ',.2f' },
      ],
    },
    config: { view: { stroke: null } },
  };
}

function buildSingleSeriesBarSpec(
  rows: Record<string, unknown>[],
  metric: string,
  color: string,
): Record<string, unknown> | null {
  const ranked = rankRows(rows, metric)
    .slice(0, 10)
    .map((row) => ({ label: rowLabel(row), value: getNumber(row[metric]) ?? 0 }));
  if (!ranked.length) return null;
  return {
    $schema: 'https://vega.github.io/schema/vega-lite/v5.json',
    width: 'container',
    height: 300,
    data: { values: ranked },
    mark: { type: 'bar' },
    encoding: {
      y: { field: 'label', type: 'nominal', sort: '-x', axis: { title: null, labelLimit: 220 } },
      x: { field: 'value', type: 'quantitative', axis: { title: null, format: metricKind(metric) === 'money' ? '~s' : undefined, grid: true } },
      color: { value: color },
      tooltip: [
        { field: 'label', type: 'nominal' },
        { field: 'value', type: 'quantitative', format: ',.2f' },
      ],
    },
    config: { view: { stroke: null } },
  };
}

function InsightCard({
  eyebrow,
  title,
  value,
  meta,
  children,
}: {
  eyebrow: string;
  title: string;
  value?: string;
  meta?: string;
  children?: ReactNode;
}) {
  return (
    <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
      <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">{eyebrow}</div>
      <div className="mt-3 text-[14px] font-semibold text-[var(--ink)]">{title}</div>
      {value && <div className="mt-2 text-[42px] font-semibold leading-none tracking-[-0.03em] text-[var(--ink)]">{value}</div>}
      {meta && <div className="mt-3 text-[12px] leading-6 text-[var(--muted)]">{meta}</div>}
      {children && <div className="mt-4">{children}</div>}
    </section>
  );
}

function InsightPanel({ mapIntent, rows }: { mapIntent: ChatbotMapIntent; rows: Record<string, unknown>[] }) {
  const metric = resolveMetric(mapIntent, rows);
  if (!metric) return null;

  const sorted = rankRows(rows, metric);
  const focusedRow = findFocusedRow(mapIntent, sorted);
  const leader = focusedRow ?? sorted[0];
  const topFive = sorted.slice(0, Math.min(sorted.length, mapIntent.topN ?? 5, 5));
  const values = sorted.map((row) => getNumber(row[metric])).filter((value): value is number => value !== null);
  const max = values.length ? Math.max(...values) : null;
  const min = values.length ? Math.min(...values) : null;
  const spread = values.length >= 2 && max !== null && min !== null ? max - min : null;
  const barSpec = buildBarChartSpec(metric, rows);
  const histogramSpec = buildHistogramSpec(metric, rows);

  return (
    <aside className="space-y-4">
      <InsightCard
        eyebrow="View type"
        title={focusedRow ? rowLabel(leader) : 'Lead result'}
        value={formatValue(metric, leader[metric])}
        meta={`${humanizeMetric(metric)} · ${levelLabel(mapIntent.level)}`}
      >
        <p className="text-[13px] leading-7 text-[var(--ink)]">{supportingText(mapIntent)}</p>
      </InsightCard>

      {topFive.length > 1 && (
        <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
          <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Top results</div>
          <div className="mt-4 space-y-2">
            {topFive.map((row, index) => (
              <div
                key={`${rowLabel(row)}-${index}`}
                className="flex items-center justify-between gap-3 border border-[var(--line)] px-3 py-2.5"
              >
                <div className="min-w-0">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-[var(--muted)]">#{index + 1}</div>
                  <div className="truncate text-[13px] font-medium text-[var(--ink)]">{rowLabel(row)}</div>
                </div>
                <div className="shrink-0 text-[12px] font-semibold text-[var(--ink)]">
                  {formatValue(metric, row[metric])}
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {barSpec && (
        <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
          <div className="flex items-end justify-between gap-4">
            <div>
              <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Leaderboard</div>
              <div className="mt-2 text-[18px] font-semibold text-[var(--ink)]">
                Top mapped {levelPluralLabel(mapIntent.level)}
              </div>
            </div>
            <div className="text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              {humanizeMetric(metric)}
            </div>
          </div>
          <div className="mt-4">
            <VegaChart spec={barSpec} />
          </div>
        </section>
      )}

      {histogramSpec && (
        <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
          <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Distribution</div>
          <div className="mt-2 text-[18px] font-semibold text-[var(--ink)]">
            Spread across the returned places
          </div>
          <div className="mt-3 text-[12px] leading-6 text-[var(--muted)]">
            {values.length > 1 && min !== null && max !== null ? (
              <>
                Values run from <strong className="text-[var(--ink)]">{formatValue(metric, min)}</strong> to{' '}
                <strong className="text-[var(--ink)]">{formatValue(metric, max)}</strong>
                {spread !== null && (
                  <>
                    , with an overall spread of{' '}
                    <strong className="text-[var(--ink)]">{formatValue(metric, spread)}</strong>.
                  </>
                )}
              </>
            ) : (
              <>This view is centered on a single resolved geography.</>
            )}
          </div>
          <div className="mt-4">
            <VegaChart spec={histogramSpec} />
          </div>
        </section>
      )}
    </aside>
  );
}

function SpendingPanel({
  mapIntent,
  mapRows,
  insightRows,
}: {
  mapIntent: ChatbotMapIntent;
  mapRows: Record<string, unknown>[];
  insightRows: Record<string, unknown>[];
}) {
  const stackedSpec = buildStackedAgencySpec(insightRows);
  const metric = resolveMetric(mapIntent, insightRows) ?? resolveMetric(mapIntent, mapRows);
  const jobsMetric = insightRows.some((row) => getNumber(row.Employees) !== null)
    ? 'Employees'
    : insightRows.some((row) => getNumber(row.employees) !== null)
      ? 'employees'
      : null;
  const jobsSpec = jobsMetric ? buildSingleSeriesBarSpec(insightRows, jobsMetric, '#b8622d') : null;
  const histogramSpec = metric ? buildHistogramSpec(metric, insightRows) : null;
  const geoRows = mapRows.length ? mapRows : insightRows;
  const canMap = hasGeographicRows(geoRows);

  return (
    <div className="space-y-6">
      <div className={`grid gap-6 ${canMap ? 'xl:grid-cols-[minmax(0,1.02fr)_400px]' : ''}`}>
        {canMap ? (
          <MapPreview rows={geoRows} variant="modal" mapIntent={mapIntent} />
        ) : (
          <InsightCard
            eyebrow="State spotlight"
            title={mapIntent.state ?? 'Selected state'}
            meta="The geographic spotlight is centered on the selected state while the charts summarize agency composition."
          />
        )}
        <aside className="space-y-4">
          <InsightCard
            eyebrow="Focus"
            title={mapIntent.state ?? 'Selected state'}
            meta="This view pairs a state spotlight with agency-level composition, which is the closest chatbot analogue to the dashboard’s spending breakdown experience."
          />
          <InsightCard
            eyebrow="Returned rows"
            title={`${insightRows.length} agency rows`}
            meta="These agency rows drive the composition charts and the ranking blocks in this modal."
          />
        </aside>
      </div>

      {stackedSpec && (
        <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
          <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Agency composition</div>
          <div className="mt-2 text-[20px] font-semibold text-[var(--ink)]">Top agencies by spending mix</div>
          <div className="mt-4">
            <VegaChart spec={stackedSpec} />
          </div>
        </section>
      )}

      <div className="grid gap-6 xl:grid-cols-2">
        {jobsSpec && (
          <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
            <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Jobs view</div>
            <div className="mt-2 text-[20px] font-semibold text-[var(--ink)]">Top agencies by federal jobs</div>
            <div className="mt-4">
              <VegaChart spec={jobsSpec} />
            </div>
          </section>
        )}

        {metric && histogramSpec && (
          <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
            <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Distribution</div>
            <div className="mt-2 text-[20px] font-semibold text-[var(--ink)]">
              {humanizeMetric(metric)} across the returned agencies
            </div>
            <div className="mt-4">
              <VegaChart spec={histogramSpec} />
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

function FlowPanel({
  mapIntent,
  mapRows,
  insightRows,
}: {
  mapIntent: ChatbotMapIntent;
  mapRows: Record<string, unknown>[];
  insightRows: Record<string, unknown>[];
}) {
  const metric = resolveMetric(mapIntent, insightRows) ?? resolveMetric(mapIntent, mapRows);
  const rows = insightRows.length ? insightRows : mapRows;
  const geoRows = mapRows.length ? mapRows : rows;
  const canMap = hasGeographicRows(geoRows) && mapIntent.mapType !== 'flow-pair';
  const barSpec = metric ? buildSingleSeriesBarSpec(rows, metric, '#3458a5') : null;
  const histogramSpec = metric ? buildHistogramSpec(metric, rows) : null;

  return (
    <div className="space-y-6">
      <div className={`grid gap-6 ${canMap ? 'xl:grid-cols-[minmax(0,1.05fr)_380px]' : ''}`}>
        {canMap ? (
          <MapPreview rows={geoRows} variant="modal" mapIntent={mapIntent} />
        ) : (
          <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
            <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Flow routes</div>
            <div className="mt-2 text-[20px] font-semibold text-[var(--ink)]">Top flow pairs</div>
            <div className="mt-4 space-y-2">
              {rows.slice(0, 10).map((row, index) => (
                <div key={`${rowLabel(row)}-${index}`} className="flex items-center justify-between gap-3 border border-[var(--line)] px-3 py-2.5">
                  <div className="text-[13px] font-medium text-[var(--ink)]">{rowLabel(row)}</div>
                  <div className="text-[12px] font-semibold text-[var(--ink)]">
                    {metric ? formatValue(metric, row[metric]) : 'N/A'}
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}

        <aside className="space-y-4">
          <InsightCard
            eyebrow="Flow focus"
            title={mapIntent.state ?? 'Selected geography'}
            meta={supportingText(mapIntent)}
          />
          {metric && rows.length > 0 && (
            <InsightCard
              eyebrow="Lead route"
              title={rowLabel(rankRows(rows, metric)[0])}
              value={formatValue(metric, rankRows(rows, metric)[0][metric])}
              meta={`${humanizeMetric(metric)} · ${rows.length} returned rows`}
            />
          )}
        </aside>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        {barSpec && (
          <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
            <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Leaderboard</div>
            <div className="mt-2 text-[20px] font-semibold text-[var(--ink)]">Largest flow results in this answer</div>
            <div className="mt-4">
              <VegaChart spec={barSpec} />
            </div>
          </section>
        )}

        {histogramSpec && (
          <section className="border border-[var(--line)] bg-[var(--surface)] px-5 py-4">
            <div className="text-[10px] uppercase tracking-[0.28em] text-[var(--muted)]">Distribution</div>
            <div className="mt-2 text-[20px] font-semibold text-[var(--ink)]">How the flow values are distributed</div>
            <div className="mt-4">
              <VegaChart spec={histogramSpec} />
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

export function ChatbotMapRenderer({ mapIntent, mapRows, insightRows, loading, error }: ChatbotMapRendererProps) {
  if (loading) {
    return (
      <div className="flex h-[min(78vh,860px)] items-center justify-center border border-[var(--line)] bg-[var(--surface)]">
        <div className="text-[13px] text-[var(--muted)]">Loading map data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex h-[min(78vh,860px)] items-center justify-center border border-[var(--line)] bg-[var(--surface)] px-6 text-center text-[13px] text-[var(--muted)]">
        {error}
      </div>
    );
  }

  const resolvedMapRows = mapRows.length ? mapRows : insightRows;
  const resolvedInsightRows = insightRows.length ? insightRows : mapRows;

  if (!resolvedMapRows.length && !resolvedInsightRows.length) {
    return (
      <div className="flex h-[min(78vh,860px)] items-center justify-center border border-[var(--line)] bg-[var(--surface)] px-6 text-center text-[13px] text-[var(--muted)]">
        No geographic rows were available for this answer yet.
      </div>
    );
  }

  if (ATLAS_TYPES.has(mapIntent.mapType)) {
    return (
      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.08fr)_380px]">
        <MapPreview rows={resolvedMapRows} variant="modal" mapIntent={mapIntent} />
        <InsightPanel mapIntent={mapIntent} rows={resolvedMapRows} />
      </div>
    );
  }

  if (SPENDING_TYPES.has(mapIntent.mapType)) {
    return <SpendingPanel mapIntent={mapIntent} mapRows={resolvedMapRows} insightRows={resolvedInsightRows} />;
  }

  if (FLOW_TYPES.has(mapIntent.mapType)) {
    return <FlowPanel mapIntent={mapIntent} mapRows={resolvedMapRows} insightRows={resolvedInsightRows} />;
  }

  return (
    <div className="flex h-[min(78vh,860px)] items-center justify-center border border-[var(--line)] bg-[var(--surface)] px-6 text-center text-[13px] text-[var(--muted)]">
      This answer does not have a dedicated interactive map renderer in this release yet.
    </div>
  );
}
