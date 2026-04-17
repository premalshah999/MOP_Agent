import type { ChatbotMapIntent } from '@/types/chat';

type DataRow = Record<string, unknown>;

const HELPER_COLUMN_PATTERN =
  /(^year$|_year$|^rank$|_rank$|list_position|sample_size|row_count|total_states|overall_|national_|state_average|mean|median|average|percentile|pct_|^pct|^diff_|_diff|_gap_rank|^gap_rank$|fips|^id$|_id$)/i;

const DEFAULT_YEARS: Record<string, string> = {
  government_finance: 'Fiscal Year 2023',
  acs: '2023',
  federal_spending: '2024',
  federal_spending_agency: '2024',
  finra: '2021',
};

function getNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value.replace(/,/g, '').trim());
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function isGeoColumn(column: string): boolean {
  return /(^state$|^county$|^cd_118$|district|_state_name|_county_name|^abbr$|state_abbr)/i.test(column);
}

function metricScore(column: string): number {
  let score = 0;
  if (HELPER_COLUMN_PATTERN.test(column) || isGeoColumn(column)) return -999;
  if (/(per_capita|ratio|assets|liabilit|revenue|expenses|expenditure|spending|contracts|grants|payments|wage|employment|residents|employees|population|poverty|income|literacy|flow|amount|score)/i.test(column)) score += 40;
  if (/(_per_1000|_per_capita|_ratio|_amount)/i.test(column)) score += 16;
  if (/^total_/i.test(column)) score += 12;
  return score;
}

function pickMetricColumn(rows: DataRow[]): string | null {
  const firstRow = rows[0];
  if (!firstRow) return null;
  const candidates = Object.keys(firstRow).filter((column) => rows.some((row) => getNumber(row[column]) !== null));
  if (!candidates.length) return null;
  const ranked = candidates
    .map((column, index) => ({ column, score: metricScore(column) + Math.max(0, 8 - index) }))
    .filter((entry) => entry.score > -900)
    .sort((a, b) => b.score - a.score);
  return ranked[0]?.column ?? null;
}

function titleCase(value: string): string {
  return value
    .split(/[\s-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}

function levelLabel(level: 'state' | 'county' | 'congress' | null): string {
  if (level === 'county') return 'counties';
  if (level === 'congress') return 'districts';
  return 'states';
}

function detectLevel(rows: DataRow[]): 'state' | 'county' | 'congress' | null {
  if (!rows.length) return null;
  const columns = new Set(Object.keys(rows[0]));
  if (columns.has('cd_118')) return 'congress';
  if (columns.has('county') && columns.has('state')) return 'county';
  if (columns.has('state') || columns.has('state_name')) return 'state';
  return null;
}

function inferDataset(datasetId: string): ChatbotMapIntent['dataset'] | undefined {
  if (datasetId === 'government_finance') return 'gov_spending';
  if (datasetId === 'acs') return 'census';
  if (datasetId === 'federal_spending') return 'contract_static';
  if (datasetId === 'federal_spending_agency') return 'contract_agency';
  if (datasetId === 'finra') return 'finra';
  return undefined;
}

function singleState(rows: DataRow[]): string | null {
  const states = new Set(
    rows
      .map((row) => row.state)
      .filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
      .map((value) => value.trim()),
  );
  return states.size === 1 ? [...states][0] : null;
}

function uniqueStringCount(rows: DataRow[], column: string): number {
  return new Set(
    rows
      .map((row) => row[column])
      .filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
      .map((value) => value.trim()),
  ).size;
}

export function deriveFallbackMapIntent(datasetId: string, rows: DataRow[]): ChatbotMapIntent | null {
  if (!rows.length) return null;

  if (datasetId === 'federal_spending_agency' && rows.some((row) => 'agency' in row)) {
    const agencyCount = uniqueStringCount(rows, 'agency');
    const stateCount = uniqueStringCount(rows, 'state');
    const hasSubregions = rows.some((row) => 'county' in row || 'cd_118' in row);
    if (!hasSubregions && agencyCount > 1 && stateCount <= 1) {
      return null;
    }
  }

  const dataset = inferDataset(datasetId);
  const level = detectLevel(rows);
  const metric = pickMetricColumn(rows);

  if (!dataset || !level || !metric || datasetId === 'fund_flow' || datasetId === 'cross_dataset') {
    return null;
  }

  const state = singleState(rows);
  const mapType =
    level !== 'state' && state
      ? 'single-state-ranked-subregions'
      : rows.length === 1 && level === 'state' && state
        ? 'single-state-spotlight'
        : rows.length > 1
          ? 'top-n-highlight'
          : 'atlas-single-metric';

  const metricTitle = titleCase(metric.replace(/_/g, ' '));
  const title =
    mapType === 'single-state-spotlight' && state
      ? `${titleCase(state)} · ${metricTitle}`
      : mapType === 'single-state-ranked-subregions' && state
        ? `${titleCase(state)} · ${metricTitle} by ${levelLabel(level)}`
        : mapType === 'top-n-highlight'
          ? `Top ${Math.min(rows.length, 10)} ${levelLabel(level)} · ${metricTitle}`
          : `${metricTitle} · Map`;

  const reason =
    mapType === 'single-state-spotlight' && state
      ? `This answer is centered on ${titleCase(state)}, so the map can keep that state in focus while preserving national context.`
      : mapType === 'single-state-ranked-subregions' && state
        ? `This answer is about subregions within ${titleCase(state)}, so a focused state view is the clearest fit.`
        : mapType === 'top-n-highlight'
          ? 'This answer returns a ranked set of places, so the map highlights the leaders while keeping the full distribution visible.'
          : 'This answer returned geographic rows, so the map can show where those values sit spatially.';

  return {
    enabled: true,
    mapType,
    dataset,
    level,
    metric,
    year: rows[0]?.year?.toString?.() ?? rows[0]?.Year?.toString?.() ?? DEFAULT_YEARS[datasetId],
    state: state ? titleCase(state) : undefined,
    title,
    subtitle: DEFAULT_YEARS[datasetId] ? `${titleCase(datasetId.replace(/_/g, ' '))} · ${DEFAULT_YEARS[datasetId]}` : titleCase(datasetId.replace(/_/g, ' ')),
    reason,
    showLegend: true,
  };
}
