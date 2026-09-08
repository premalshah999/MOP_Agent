import type { ChatbotMapIntent, QueryContract } from '@/types/chat';

export interface DashboardDestination {
  href: string;
  label: string;
  description: string;
}

interface DashboardBridgeInput {
  datasetId?: string;
  contract?: QueryContract;
  mapIntent?: ChatbotMapIntent | null;
}

type DashboardKind =
  | 'census'
  | 'government-spending'
  | 'federal-spending-agency'
  | 'federal-spending-breaks'
  | 'government-finances'
  | 'finra-financial-literacy'
  | 'fund-flow';

const DASHBOARD_DETAILS: Record<DashboardKind, { label: string; description: string }> = {
  census: {
    label: 'Explore Census dashboard',
    description: 'Continue with the Census geography and indicator controls.',
  },
  'government-spending': {
    label: 'Explore spending dashboard',
    description: 'Continue with the federal spending geography and metric controls.',
  },
  'federal-spending-agency': {
    label: 'Explore agency dashboard',
    description: 'Continue with federal spending by agency.',
  },
  'federal-spending-breaks': {
    label: 'Explore spending breakdown',
    description: 'Continue with the federal spending composition view.',
  },
  'government-finances': {
    label: 'Explore finance dashboard',
    description: 'Continue with government finance indicators and geographies.',
  },
  'finra-financial-literacy': {
    label: 'Explore FINRA dashboard',
    description: 'Continue with financial capability indicators and geographies.',
  },
  'fund-flow': {
    label: 'Explore fund-flow dashboard',
    description: 'Continue with origin, destination, agency, and period controls.',
  },
};

const DATASET_KIND: Record<string, DashboardKind> = {
  acs: 'census',
  census: 'census',
  federal_spending: 'government-spending',
  contract_static: 'government-spending',
  federal_spending_agency: 'federal-spending-agency',
  contract_agency: 'federal-spending-agency',
  spending_breakdown: 'federal-spending-breaks',
  government_finance: 'government-finances',
  gov_spending: 'government-finances',
  finra: 'finra-financial-literacy',
  fund_flow: 'fund-flow',
};

function kindFromTable(table: string): DashboardKind | null {
  const value = table.toLowerCase();
  if (value.endsWith('_flow') || value.includes('fund_flow')) return 'fund-flow';
  if (value.startsWith('acs_')) return 'census';
  if (value.startsWith('finra_')) return 'finra-financial-literacy';
  if (value.startsWith('gov_')) return 'government-finances';
  if (value.startsWith('spending_') && value.includes('agency')) return 'federal-spending-agency';
  if (value.startsWith('spending_')) return 'federal-spending-breaks';
  if (value.startsWith('contract_') && value.includes('agency')) return 'federal-spending-agency';
  if (value.startsWith('contract_')) return 'government-spending';
  return null;
}

function dashboardKind({ datasetId, contract, mapIntent }: DashboardBridgeInput): DashboardKind | null {
  if (mapIntent?.dataset && DATASET_KIND[mapIntent.dataset]) return DATASET_KIND[mapIntent.dataset];

  const tables = contract?.tables ?? contract?.context_memory?.tables ?? [];
  for (const table of tables) {
    const match = kindFromTable(table);
    if (match) return match;
  }

  if (contract?.family) {
    const match = kindFromTable(contract.family);
    if (match) return match;
  }
  return datasetId ? DATASET_KIND[datasetId] ?? null : null;
}

function siteBase(): string {
  const configured = import.meta.env.VITE_MOP_SITE_URL?.trim();
  return (configured || 'https://mop.rhsmith.umd.edu').replace(/\/$/, '');
}

function normalizedYear(value: string | number | null | undefined): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  if (typeof value !== 'string') return null;
  const exact = value.trim().match(/^\d{4}$/);
  return exact ? exact[0] : null;
}

function firstFilterValue(contract: QueryContract | undefined, pattern: RegExp): string | null {
  const filter = contract?.context_memory?.filters?.find((item) => pattern.test(item.column));
  return filter?.values?.find(Boolean) ?? null;
}

/**
 * Resolve an answer to the matching MOP dashboard using the answer contract,
 * not keywords in the generated prose. Query parameters are intentionally
 * limited to filters the dashboard understands and can safely ignore when a
 * value is unavailable.
 */
export function dashboardDestination(input: DashboardBridgeInput): DashboardDestination | null {
  const kind = dashboardKind(input);
  if (!kind) return null;

  const { contract, mapIntent } = input;
  const params = new URLSearchParams();
  const level = mapIntent?.level ?? contract?.geography_level ?? contract?.context_memory?.geography_level;
  const metric = contract?.metric ?? contract?.context_memory?.metrics?.[0] ?? mapIntent?.metric;
  const year = normalizedYear(mapIntent?.year ?? contract?.year)
    ?? normalizedYear(contract?.context_memory?.period as string | number | undefined);
  const state = mapIntent?.state ?? contract?.focus_state ?? contract?.context_memory?.focus_state;
  const agency = mapIntent?.agency ?? firstFilterValue(contract, /agency/i);

  if (kind === 'fund-flow') {
    if (level) params.set('flowLevel', level);
    if (state) params.set('state', state);
    const direction = contract?.flow_direction ?? contract?.context_memory?.flow_direction;
    if (direction === 'inflow') params.set('direction', 'Inflow');
    if (direction === 'outflow') params.set('direction', 'Outflow');
    if (agency) params.set('agency', agency);
    const naics = firstFilterValue(contract, /naics|industry/i);
    if (naics) params.set('naics', naics);
    const requestedYears = contract?.context_memory?.requested_years ?? [];
    if (requestedYears.length) {
      params.set('year_start', String(Math.min(...requestedYears)));
      params.set('year_end', String(Math.max(...requestedYears)));
    } else if (year) {
      params.set('year_start', year);
      params.set('year_end', year);
    }
  } else {
    if (level) params.set('level', level);
    if (metric) params.set(kind === 'federal-spending-breaks' ? 'metric' : 'variable', metric);
    if (year) params.set('year', year);
    if (state && kind === 'federal-spending-breaks') params.set('state', state);
    if (agency && kind === 'federal-spending-agency') params.set('agency', agency);
  }

  const query = params.toString();
  return {
    href: `${siteBase()}/dashboard/${kind}${query ? `?${query}` : ''}`,
    ...DASHBOARD_DETAILS[kind],
  };
}
