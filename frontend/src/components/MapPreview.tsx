import { Loader2, RotateCcw } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import maplibregl, {
  type ExpressionSpecification,
  type GeoJSONSourceSpecification,
  type LngLatBoundsLike,
  type StyleSpecification,
} from 'maplibre-gl';
import { buildApiUrl } from '@/lib/api';
import type { ChatbotMapIntent } from '@/types/chat';

type DataRow = Record<string, unknown>;
type GeoLevel = 'state' | 'county' | 'congress';
type DisplayMode = 'state' | 'focused-subdivision' | 'national-subdivision';
type PreviewView = 'heat' | 'comparison' | 'state-zoom' | 'subdivision' | 'subdivision-zoom';
type BoundsTuple = [number, number, number, number];

interface GeoFeature {
  type: 'Feature';
  geometry: unknown;
  properties: Record<string, unknown>;
}

interface GeoCollection {
  type: 'FeatureCollection';
  features: GeoFeature[];
}

interface MatchValue {
  key: string;
  label: string;
  value: number;
  stateAbbr?: string;
}

interface MapDescriptor {
  geography: GeoLevel;
  metricLabel: string;
  matches: Map<string, MatchValue>;
  focusStateAbbr?: string;
}

interface PreparedMap {
  displayMode: DisplayMode;
  headline: string;
  metricLabel: string;
  topMatch: MatchValue;
  values: number[];
  bounds: BoundsTuple;
  baseData: GeoCollection;
  highlightData: GeoCollection;
}

interface ViewOption {
  id: PreviewView;
  label: string;
  description: string;
}

/* ── Constants ── */

const DEFAULT_BOUNDS: BoundsTuple = [-125, 24, -66.5, 49.5];
const DEFAULT_FILL = '#eef3f8';
const DEFAULT_STROKE = '#cbd5e1';
const STATE_FOCUS_FILL = '#f8e6e3';
const COMPARISON_FILL = '#e3edf9';

// Dashboard quintile palette (sequential red, matching Maryland Opportunities Dashboard)
const QUINTILE_COLORS = ['#fee5d9', '#fcae91', '#fb6a4a', '#de2d26', '#a50f15'];
const QUINTILE_LABELS = ['Low', 'Q2', 'Q3', 'Q4', 'High'];

const HELPER_COLUMN_PATTERN =
  /(^year$|_year$|^rank$|_rank$|list_position|sample_size|row_count|total_states|overall_|national_|state_average|mean|median|average|percentile|pct_|^pct|^diff_|_diff|_gap_rank|^gap_rank$|fips|^id$|_id$)/i;

const GEOJSON_CACHE = new Map<string, Promise<GeoCollection>>();

const CARTO_TILES = [
  'https://a.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
  'https://b.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
  'https://c.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
  'https://d.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png',
];

const MAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    cartoLight: {
      type: 'raster',
      tiles: CARTO_TILES,
      tileSize: 256,
      attribution: '© CARTO, © OpenStreetMap contributors',
    },
  },
  layers: [
    {
      id: 'preview-background',
      type: 'background',
      paint: { 'background-color': '#f8fafc' },
    },
    {
      id: 'preview-basemap',
      type: 'raster',
      source: 'cartoLight',
      minzoom: 0,
      maxzoom: 22,
      paint: {
        'raster-opacity': 0.86,
        'raster-saturation': -0.22,
        'raster-brightness-max': 0.98,
      },
    },
  ],
};

const STATE_TO_POSTAL: Record<string, string> = {
  alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA',
  colorado: 'CO', connecticut: 'CT', delaware: 'DE', 'district of columbia': 'DC',
  florida: 'FL', georgia: 'GA', hawaii: 'HI', idaho: 'ID', illinois: 'IL',
  indiana: 'IN', iowa: 'IA', kansas: 'KS', kentucky: 'KY', louisiana: 'LA',
  maine: 'ME', maryland: 'MD', massachusetts: 'MA', michigan: 'MI', minnesota: 'MN',
  mississippi: 'MS', missouri: 'MO', montana: 'MT', nebraska: 'NE', nevada: 'NV',
  'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
  'north carolina': 'NC', 'north dakota': 'ND', ohio: 'OH', oklahoma: 'OK',
  oregon: 'OR', pennsylvania: 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
  'south dakota': 'SD', tennessee: 'TN', texas: 'TX', utah: 'UT', vermont: 'VT',
  virginia: 'VA', washington: 'WA', 'west virginia': 'WV', wisconsin: 'WI', wyoming: 'WY',
};
const POSTAL_TO_STATE = Object.fromEntries(
  Object.entries(STATE_TO_POSTAL).map(([name, abbr]) => [abbr, name]),
) as Record<string, string>;

/* ── Helpers ── */

function fetchGeoJson(name: 'states' | 'counties' | 'congress'): Promise<GeoCollection> {
  if (!GEOJSON_CACHE.has(name)) {
    GEOJSON_CACHE.set(
      name,
      fetch(buildApiUrl(`/geo/${name}.geojson`)).then(async (response) => {
        if (!response.ok) throw new Error(`Failed to load ${name}.geojson`);
        return (await response.json()) as GeoCollection;
      }),
    );
  }
  return GEOJSON_CACHE.get(name)!;
}

function normalizeText(value: unknown): string {
  return String(value ?? '').toLowerCase().replace(/\bcounty\b/g, '').replace(/[^\w\s-]/g, ' ').replace(/\s+/g, ' ').trim();
}

function titleCase(value: string): string {
  return value.split(/[\s-]+/).filter(Boolean).map((p) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase()).join(' ');
}

function humanizeColumn(column: string): string {
  return column.replace(/[_]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function formatMetricValue(value: number): string {
  if (!Number.isFinite(value)) return 'N/A';
  const abs = Math.abs(value);
  if (abs >= 1e9) return `${(value / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`;
  if (abs >= 1e6) return `${(value / 1e6).toLocaleString(undefined, { maximumFractionDigits: 2 })}M`;
  if (abs >= 1e3) return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function getNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Number(value.replace(/,/g, '').trim());
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function pickFirstValue(row: DataRow, candidates: string[]): string | null {
  for (const key of candidates) {
    const value = row[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return null;
}

function toStateAbbr(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed.length === 2) {
    const upper = trimmed.toUpperCase();
    return POSTAL_TO_STATE[upper] ? upper : null;
  }
  return STATE_TO_POSTAL[trimmed.toLowerCase()] ?? null;
}

function normalizeDistrict(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const match = value.toUpperCase().trim().match(/^([A-Z]{2})[-\s]?0?(\d{1,2})$/);
  if (!match) return null;
  return `${match[1]}-${match[2].padStart(2, '0')}`;
}

function isNumericColumn(rows: DataRow[], column: string): boolean {
  return rows.some((row) => getNumber(row[column]) !== null);
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
  if (/^count$|count_/i.test(column)) score -= 8;
  return score;
}

function pickMetricColumn(rows: DataRow[]): string | null {
  const firstRow = rows[0];
  if (!firstRow) return null;
  const orderedColumns = Object.keys(firstRow);
  const candidates = orderedColumns.filter((c) => isNumericColumn(rows, c));
  if (!candidates.length) return null;
  const ranked = candidates
    .map((column, index) => ({ column, score: metricScore(column) + Math.max(0, 8 - index) }))
    .filter((e) => e.score > -900)
    .sort((a, b) => b.score - a.score);
  return ranked[0]?.column ?? null;
}

function highestMatch(matches: Map<string, MatchValue>): MatchValue | null {
  return [...matches.values()].sort((a, b) => b.value - a.value)[0] ?? null;
}

function cloneFeature(feature: GeoFeature, extra: Record<string, unknown> = {}): GeoFeature {
  return { ...feature, properties: { ...feature.properties, ...extra } };
}

function districtKeyFromFeature(f: GeoFeature): string | null { return normalizeDistrict(f.properties.cd_118); }
function countyKeyFromFeature(f: GeoFeature): string | null {
  const abbr = String(f.properties.abbr ?? '').toUpperCase();
  return abbr ? `${abbr}:${normalizeText(f.properties.name)}` : null;
}
function districtLabelFromFeature(f: GeoFeature): string { return String(f.properties.cd_118 ?? 'District'); }
function countyLabelFromFeature(f: GeoFeature): string {
  return `${titleCase(String(f.properties.name ?? ''))}, ${String(f.properties.abbr ?? '').toUpperCase()}`;
}

function filterFeaturesByState(features: GeoFeature[], stateAbbr: string, geo: GeoLevel): GeoFeature[] {
  if (geo === 'county') return features.filter((f) => String(f.properties.abbr ?? '').toUpperCase() === stateAbbr);
  if (geo === 'congress') return features.filter((f) => String(f.properties.cd_118 ?? '').toUpperCase().startsWith(`${stateAbbr}-`));
  return features;
}

function updateBoundsFromCoordinates(coords: unknown, bounds: BoundsTuple): void {
  if (!Array.isArray(coords) || !coords.length) return;
  if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
    const lng = Number(coords[0]), lat = Number(coords[1]);
    if (Number.isFinite(lng) && Number.isFinite(lat)) {
      bounds[0] = Math.min(bounds[0], lng);
      bounds[1] = Math.min(bounds[1], lat);
      bounds[2] = Math.max(bounds[2], lng);
      bounds[3] = Math.max(bounds[3], lat);
    }
    return;
  }
  coords.forEach((c) => updateBoundsFromCoordinates(c, bounds));
}

function computeBounds(features: GeoFeature[]): BoundsTuple {
  const b: BoundsTuple = [Infinity, Infinity, -Infinity, -Infinity];
  features.forEach((f) => updateBoundsFromCoordinates(f.geometry, b));
  return Number.isFinite(b[0]) ? b : DEFAULT_BOUNDS;
}

function computeQuintileBreaks(values: number[]): number[] {
  if (!values.length) return [0, 0, 0, 0];
  const sorted = [...values].sort((a, b) => a - b);
  return [1, 2, 3, 4].map((s) => sorted[Math.min(sorted.length - 1, Math.floor((sorted.length * s) / 5))]);
}

function getQuintile(value: number, breaks: number[]): number {
  for (let i = 0; i < breaks.length; i++) {
    if (value < breaks[i]) return i + 1;
  }
  return 5;
}

function buildColorExpression(values: number[]): ExpressionSpecification {
  const t = computeQuintileBreaks(values);
  return [
    'step', ['to-number', ['get', 'map_value']],
    QUINTILE_COLORS[0], t[0], QUINTILE_COLORS[1], t[1], QUINTILE_COLORS[2], t[2], QUINTILE_COLORS[3], t[3], QUINTILE_COLORS[4],
  ] as ExpressionSpecification;
}

/* ── Map builders ── */

function buildStateMap(desc: MapDescriptor, statesData: GeoCollection): PreparedMap | null {
  const top = highestMatch(desc.matches);
  if (!top) return null;
  const base = statesData.features.map((f) => {
    const abbr = String(f.properties.abbr ?? '').toUpperCase();
    return cloneFeature(f, { map_key: abbr, map_label: titleCase(String(f.properties.name ?? abbr)) });
  });
  const highlight = statesData.features
    .map((f) => {
      const abbr = String(f.properties.abbr ?? '').toUpperCase();
      const m = desc.matches.get(abbr);
      return m ? cloneFeature(f, { map_key: abbr, map_label: m.label, map_value: m.value }) : null;
    })
    .filter((f): f is GeoFeature => Boolean(f));
  if (!highlight.length) return null;
  return {
    displayMode: 'state', headline: 'State map', metricLabel: desc.metricLabel, topMatch: top,
    values: [...desc.matches.values()].map((m) => m.value),
    bounds: computeBounds(highlight),
    baseData: { type: 'FeatureCollection', features: base },
    highlightData: { type: 'FeatureCollection', features: highlight },
  };
}

function buildFocusedSubdivisionMap(desc: MapDescriptor, geoData: GeoCollection): PreparedMap | null {
  const top = highestMatch(desc.matches);
  if (!top || !desc.focusStateAbbr) return null;
  const filtered = filterFeaturesByState(geoData.features, desc.focusStateAbbr, desc.geography);
  if (!filtered.length) return null;
  const keyFn = desc.geography === 'county' ? countyKeyFromFeature : districtKeyFromFeature;
  const labelFn = desc.geography === 'county' ? countyLabelFromFeature : districtLabelFromFeature;
  const base = filtered.map((f) => cloneFeature(f, { map_key: keyFn(f), map_label: labelFn(f) }));
  const highlight = filtered
    .map((f) => { const k = keyFn(f); if (!k) return null; const m = desc.matches.get(k); return m ? cloneFeature(f, { map_key: k, map_label: m.label, map_value: m.value }) : null; })
    .filter((f): f is GeoFeature => Boolean(f));
  if (!highlight.length) return null;
  const label = desc.geography === 'county' ? `${desc.focusStateAbbr} counties` : `${desc.focusStateAbbr} districts`;
  return {
    displayMode: 'focused-subdivision', headline: label, metricLabel: desc.metricLabel, topMatch: top,
    values: [...desc.matches.values()].map((m) => m.value),
    bounds: computeBounds(base),
    baseData: { type: 'FeatureCollection', features: base },
    highlightData: { type: 'FeatureCollection', features: highlight },
  };
}

function buildNationalSubdivisionMap(desc: MapDescriptor, geoData: GeoCollection, statesData: GeoCollection): PreparedMap | null {
  const top = highestMatch(desc.matches);
  if (!top) return null;
  const matchedStates = new Set([...desc.matches.values()].map((m) => m.stateAbbr).filter((v): v is string => Boolean(v)));
  const base = statesData.features.map((f) => {
    const abbr = String(f.properties.abbr ?? '').toUpperCase();
    return cloneFeature(f, { map_key: abbr, map_label: titleCase(String(f.properties.name ?? abbr)), ...(matchedStates.has(abbr) ? { map_focus: 1 } : {}) });
  });
  const highlight = geoData.features
    .map((f) => {
      const k = desc.geography === 'county' ? countyKeyFromFeature(f) : districtKeyFromFeature(f);
      if (!k) return null; const m = desc.matches.get(k);
      return m ? cloneFeature(f, { map_key: k, map_label: m.label, map_value: m.value }) : null;
    })
    .filter((f): f is GeoFeature => Boolean(f));
  if (!highlight.length) return null;
  return {
    displayMode: 'national-subdivision',
    headline: desc.geography === 'county' ? 'County map' : 'Congressional districts',
    metricLabel: desc.metricLabel, topMatch: top,
    values: [...desc.matches.values()].map((m) => m.value),
    bounds: computeBounds(highlight),
    baseData: { type: 'FeatureCollection', features: base },
    highlightData: { type: 'FeatureCollection', features: highlight },
  };
}

/* ── Descriptor detection ── */

function detectDescriptor(rows: DataRow[]): MapDescriptor | null {
  if (!rows.length) return null;
  const metricColumn = pickMetricColumn(rows);
  if (!metricColumn) return null;

  const districtMatches = new Map<string, MatchValue>();
  const countyMatches = new Map<string, MatchValue>();
  const stateMatches = new Map<string, MatchValue>();

  rows.forEach((row) => {
    const value = getNumber(row[metricColumn]);
    if (value === null) return;

    const district = normalizeDistrict(row.cd_118) ?? normalizeDistrict(row.rcpt_cd_name) ?? normalizeDistrict(row.district) ?? normalizeDistrict(row.congressional_district);
    if (district) {
      districtMatches.set(district, { key: district, label: district, value, stateAbbr: district.split('-')[0] });
      return;
    }

    const county = pickFirstValue(row, ['county', 'county_name', 'rcpt_county_name', 'subawardee_county_name']);
    const countyState = toStateAbbr(row.state) ?? toStateAbbr(row.state_abbr) ?? toStateAbbr(row.rcpt_state_name) ?? toStateAbbr(row.subawardee_state_name);
    if (county && countyState) {
      const k = `${countyState}:${normalizeText(county)}`;
      countyMatches.set(k, { key: k, label: `${titleCase(normalizeText(county))}, ${countyState}`, value, stateAbbr: countyState });
      return;
    }

    const state = pickFirstValue(row, ['state', 'state_name', 'rcpt_state_name', 'subawardee_state_name']);
    const abbr = state ? toStateAbbr(state) : null;
    if (abbr) stateMatches.set(abbr, { key: abbr, label: titleCase(POSTAL_TO_STATE[abbr] ?? abbr), value, stateAbbr: abbr });
  });

  if (districtMatches.size) {
    const states = new Set([...districtMatches.values()].map((m) => m.stateAbbr).filter(Boolean));
    return { geography: 'congress', metricLabel: humanizeColumn(metricColumn), matches: districtMatches, focusStateAbbr: states.size === 1 ? [...states][0] : undefined };
  }
  if (countyMatches.size) {
    const states = new Set([...countyMatches.values()].map((m) => m.stateAbbr).filter(Boolean));
    return { geography: 'county', metricLabel: humanizeColumn(metricColumn), matches: countyMatches, focusStateAbbr: states.size === 1 ? [...states][0] : undefined };
  }
  if (stateMatches.size) return { geography: 'state', metricLabel: humanizeColumn(metricColumn), matches: stateMatches };
  return null;
}

function buildHoverValue(props: Record<string, unknown> | undefined, fallback: MatchValue) {
  const label = typeof props?.map_label === 'string' ? props.map_label : fallback.label;
  const value = getNumber(props?.map_value) ?? fallback.value;
  return { label, value };
}

function featureCollectionByKeys(collection: GeoCollection, keys: readonly string[]): GeoCollection {
  if (!keys.length) return { type: 'FeatureCollection', features: [] };
  const keySet = new Set(keys);
  return {
    type: 'FeatureCollection',
    features: collection.features.filter((feature) => keySet.has(String(feature.properties.map_key ?? ''))),
  };
}

function singleFeatureByKey(collection: GeoCollection, key: string | null | undefined): GeoCollection {
  if (!key) return { type: 'FeatureCollection', features: [] };
  return featureCollectionByKeys(collection, [key]);
}

function buildKeyMatchExpression(keys: readonly string[]): ExpressionSpecification {
  if (keys.length === 1) {
    return ['==', ['get', 'map_key'], keys[0]] as ExpressionSpecification;
  }
  return ['match', ['get', 'map_key'], [...keys], true, false] as ExpressionSpecification;
}

function buildBaseFillExpression(
  mode: DisplayMode,
  view: PreviewView,
  comparisonKeys: readonly string[],
  primaryStateKey: string | null,
): string | ExpressionSpecification {
  if (view === 'comparison' && comparisonKeys.length) {
    return ['case', buildKeyMatchExpression(comparisonKeys), COMPARISON_FILL, DEFAULT_FILL] as ExpressionSpecification;
  }
  if (view === 'state-zoom' && primaryStateKey) {
    return ['case', ['==', ['get', 'map_key'], primaryStateKey], STATE_FOCUS_FILL, DEFAULT_FILL] as ExpressionSpecification;
  }
  if (mode === 'national-subdivision') {
    return ['case', ['==', ['get', 'map_focus'], 1], STATE_FOCUS_FILL, DEFAULT_FILL] as ExpressionSpecification;
  }
  return DEFAULT_FILL;
}

function resolveViewOptions(
  preparedMap: PreparedMap | null,
  descriptor: MapDescriptor | null,
  mapIntent: ChatbotMapIntent | undefined,
): ViewOption[] {
  if (!preparedMap || !descriptor) return [];

  const subdivisionLabel = descriptor.geography === 'congress' ? 'District view' : 'County view';
  const zoomedSubdivisionLabel = descriptor.geography === 'congress' ? 'Zoomed district' : 'Zoomed county';
  const options: ViewOption[] = [
    {
      id: 'heat',
      label: 'Heat map',
      description: 'Calculated quintile shading across the returned values.',
    },
  ];

  if (mapIntent?.mapType === 'atlas-comparison' && (mapIntent.comparisonIds?.length ?? 0) >= 2) {
    options.unshift({
      id: 'comparison',
      label: 'Comparison',
      description: 'Emphasizes the requested comparison geographies while keeping the national backdrop.',
    });
  }

  if (preparedMap.displayMode === 'state' && mapIntent && mapIntent.state && mapIntent.mapType !== 'atlas-comparison') {
    options.push({
      id: 'state-zoom',
      label: 'State zoom',
      description: 'Zooms tightly into the focal state while preserving nearby context.',
    });
  }

  if (preparedMap.displayMode !== 'state') {
    options.push({
      id: 'subdivision',
      label: subdivisionLabel,
      description: 'Shows the county or district pattern at the full available geography.',
    });
    options.push({
      id: 'subdivision-zoom',
      label: zoomedSubdivisionLabel,
      description: 'Zooms further into the lead county or district for a tighter local read.',
    });
  }

  return options;
}

function defaultPreviewView(mapIntent: ChatbotMapIntent | undefined, options: ViewOption[]): PreviewView {
  const available = new Set(options.map((option) => option.id));
  const preferred: PreviewView =
    mapIntent?.defaultView ??
    (mapIntent?.mapType === 'atlas-comparison'
      ? 'comparison'
      : mapIntent?.mapType === 'single-state-spotlight'
        ? 'state-zoom'
        : mapIntent?.mapType === 'atlas-within-state' || mapIntent?.mapType === 'single-state-ranked-subregions'
          ? 'subdivision'
          : 'heat');

  if (available.has(preferred)) return preferred;
  return options[0]?.id ?? 'heat';
}

function boundsForView(
  preparedMap: PreparedMap,
  view: PreviewView,
  comparisonKeys: readonly string[],
  primaryStateKey: string | null,
): { bounds: BoundsTuple; padding: number; maxZoom: number } {
  const defaultPadding = preparedMap.displayMode === 'focused-subdivision' ? 18 : 24;
  const defaultMaxZoom = preparedMap.displayMode === 'focused-subdivision' ? 6.9 : 5.4;

  if (view === 'comparison' && comparisonKeys.length) {
    const comparisonFeatures = featureCollectionByKeys(preparedMap.baseData, comparisonKeys).features;
    if (comparisonFeatures.length) {
      return { bounds: computeBounds(comparisonFeatures), padding: 42, maxZoom: 4.8 };
    }
  }

  if (view === 'state-zoom' && primaryStateKey) {
    const focusFeature = singleFeatureByKey(preparedMap.baseData, primaryStateKey).features;
    if (focusFeature.length) {
      return { bounds: computeBounds(focusFeature), padding: 28, maxZoom: 6.4 };
    }
  }

  if (view === 'subdivision') {
    return {
      bounds: computeBounds(preparedMap.baseData.features.length ? preparedMap.baseData.features : preparedMap.highlightData.features),
      padding: preparedMap.displayMode === 'national-subdivision' ? 24 : 18,
      maxZoom: preparedMap.displayMode === 'national-subdivision' ? 5.5 : 7.1,
    };
  }

  if (view === 'subdivision-zoom') {
    const leadFeature = singleFeatureByKey(preparedMap.highlightData, preparedMap.topMatch.key).features;
    if (leadFeature.length) {
      return {
        bounds: computeBounds(leadFeature),
        padding: 32,
        maxZoom: preparedMap.headline.toLowerCase().includes('district') ? 7.8 : 8.8,
      };
    }
  }

  return { bounds: preparedMap.bounds, padding: defaultPadding, maxZoom: defaultMaxZoom };
}

/* ── Component ── */

interface MapPreviewProps {
  rows: DataRow[];
  variant?: 'card' | 'modal';
  mapHeightClassName?: string;
  mapIntent?: ChatbotMapIntent;
}

export default function MapPreview({ rows, variant = 'card', mapHeightClassName, mapIntent }: MapPreviewProps) {
  const descriptor = useMemo(() => detectDescriptor(rows), [rows]);
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [preparedMap, setPreparedMap] = useState<PreparedMap | null>(null);
  const [loading, setLoading] = useState(false);
  const [mapReady, setMapReady] = useState(false);
  const [errored, setErrored] = useState(false);
  const [hovered, setHovered] = useState<{ label: string; value: number } | null>(null);
  const [retryKey, setRetryKey] = useState(0);
  const viewOptions = useMemo(() => resolveViewOptions(preparedMap, descriptor, mapIntent), [preparedMap, descriptor, mapIntent]);
  const [activeView, setActiveView] = useState<PreviewView>('heat');
  const comparisonKeys = useMemo(
    () => (mapIntent?.comparisonIds ?? []).map((value) => String(value).toUpperCase()),
    [mapIntent?.comparisonIds],
  );
  const primaryStateKey = useMemo(() => {
    if (!mapIntent?.state) return null;
    return toStateAbbr(mapIntent.state)?.toUpperCase() ?? null;
  }, [mapIntent?.state]);

  useEffect(() => {
    if (!viewOptions.length) return;
    setActiveView((current) =>
      viewOptions.some((option) => option.id === current)
        ? current
        : defaultPreviewView(mapIntent, viewOptions),
    );
  }, [mapIntent, viewOptions]);

  // Data preparation
  useEffect(() => {
    let active = true;
    if (!descriptor) {
      setPreparedMap(null); setHovered(null); setLoading(false); setMapReady(false); setErrored(false);
      return () => { active = false; };
    }
    setLoading(true); setMapReady(false); setErrored(false);

    const run = async () => {
      try {
        const statesP = fetchGeoJson('states');
        const geoP = descriptor.geography === 'state' ? statesP : descriptor.geography === 'county' ? fetchGeoJson('counties') : fetchGeoJson('congress');
        const [statesData, geoData] = await Promise.all([statesP, geoP]);
        if (!active) return;
        const next = descriptor.geography === 'state'
          ? buildStateMap(descriptor, statesData)
          : descriptor.focusStateAbbr
            ? buildFocusedSubdivisionMap(descriptor, geoData)
            : buildNationalSubdivisionMap(descriptor, geoData, statesData);
        setPreparedMap(next);
        if (next) setHovered({ label: next.topMatch.label, value: next.topMatch.value });
      } catch (err) {
        console.error('[MOP] Map data load failed:', err);
        if (!active) return;
        setPreparedMap(null); setErrored(true);
      } finally {
        if (active) setLoading(false);
      }
    };
    void run();
    return () => { active = false; };
  }, [descriptor, retryKey]);

  // Map rendering
  useEffect(() => {
    if (!preparedMap || !mapContainerRef.current) return undefined;
    setMapReady(false);

    const colorExpr = buildColorExpression(preparedMap.values);
    const highlightSource: GeoJSONSourceSpecification = { type: 'geojson', data: preparedMap.highlightData as GeoJSON.GeoJSON };
    const baseSource: GeoJSONSourceSpecification = { type: 'geojson', data: preparedMap.baseData as GeoJSON.GeoJSON };
    const emphasisKeys =
      activeView === 'comparison'
        ? comparisonKeys
        : activeView === 'state-zoom' && primaryStateKey
          ? [primaryStateKey]
          : activeView === 'subdivision-zoom'
            ? [preparedMap.topMatch.key]
            : [];
    const focusBounds = boundsForView(preparedMap, activeView, comparisonKeys, primaryStateKey);

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      attributionControl: false,
      dragRotate: false,
      touchZoomRotate: false,
      scrollZoom: false,
      boxZoom: false,
      pitchWithRotate: false,
      cooperativeGestures: false,
    });
    mapRef.current = map;

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
    map.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');

    const onLoad = () => {
      // Base layer
      map.addSource('preview-base', baseSource);
      map.addLayer({
        id: 'preview-base-fill', type: 'fill', source: 'preview-base',
        paint: {
          'fill-color': buildBaseFillExpression(preparedMap.displayMode, activeView, comparisonKeys, primaryStateKey),
          'fill-opacity':
            activeView === 'state-zoom'
              ? 0.7
              : preparedMap.displayMode === 'national-subdivision'
                ? 0.38
                : 0.16,
        },
      });
      map.addLayer({
        id: 'preview-base-line', type: 'line', source: 'preview-base',
        paint: {
          'line-color': DEFAULT_STROKE,
          'line-width': preparedMap.displayMode === 'focused-subdivision' ? 0.14 : 0.18,
          'line-opacity': 0.5,
        },
      });

      // Highlight (choropleth) layer
      map.addSource('preview-highlight', highlightSource);
      map.addLayer({
        id: 'preview-highlight-fill', type: 'fill', source: 'preview-highlight',
        paint: {
          'fill-color': colorExpr,
          'fill-opacity': activeView === 'comparison' ? 0.84 : activeView === 'state-zoom' ? 0.92 : 0.9,
        },
      });
      map.addLayer({
        id: 'preview-highlight-line', type: 'line', source: 'preview-highlight',
        paint: { 'line-color': 'rgba(255,255,255,0.94)', 'line-width': 0.2, 'line-opacity': 0.78 },
      });

      map.addLayer({
        id: 'preview-emphasis-line',
        type: 'line',
        source: activeView === 'state-zoom' ? 'preview-base' : 'preview-highlight',
        paint: {
          'line-color': activeView === 'comparison' ? '#1e293b' : '#0f172a',
          'line-width':
            emphasisKeys.length > 0
              ? ([
                  'case',
                  buildKeyMatchExpression(emphasisKeys),
                  activeView === 'subdivision-zoom' ? 0.6 : 0.52,
                  0,
                ] as ExpressionSpecification)
              : 0,
          'line-opacity':
            emphasisKeys.length > 0
              ? ([
                  'case',
                  buildKeyMatchExpression(emphasisKeys),
                  0.9,
                  0,
                ] as ExpressionSpecification)
              : 0,
        },
      });

      // Selected feature outline (initially hidden)
      map.addLayer({
        id: 'preview-selected-line', type: 'line', source: 'preview-highlight',
        paint: { 'line-color': '#0f172a', 'line-width': 0.88, 'line-opacity': 0.95 },
        filter: ['==', ['get', 'map_key'], ''],
      });

      // Fit bounds
      map.fitBounds(focusBounds.bounds as LngLatBoundsLike, {
        padding: focusBounds.padding,
        duration: 0,
        maxZoom: focusBounds.maxZoom,
      });
      map.resize();
      window.requestAnimationFrame(() => map.resize());

      // Interaction
      map.on('mouseenter', 'preview-highlight-fill', () => { map.getCanvas().style.cursor = 'pointer'; });
      map.on('mouseleave', 'preview-highlight-fill', () => {
        map.getCanvas().style.cursor = '';
        setHovered({ label: preparedMap.topMatch.label, value: preparedMap.topMatch.value });
        map.setFilter('preview-selected-line', ['==', ['get', 'map_key'], '']);
      });
      map.on('mousemove', 'preview-highlight-fill', (e) => {
        const f = e.features?.[0];
        if (!f) return;
        const props = (f.properties ?? {}) as Record<string, unknown>;
        setHovered(buildHoverValue(props, preparedMap.topMatch));
        map.setFilter('preview-selected-line', ['==', ['get', 'map_key'], String(props.map_key ?? '')]);
      });
      map.on('click', 'preview-highlight-fill', (e) => {
        const f = e.features?.[0];
        if (!f) return;
        setHovered(buildHoverValue((f.properties ?? {}) as Record<string, unknown>, preparedMap.topMatch));
      });

      setMapReady(true);
    };

    map.on('load', onLoad);
    const onResize = () => map.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      map.remove();
      mapRef.current = null;
    };
  }, [activeView, comparisonKeys, preparedMap, primaryStateKey]);

  if (!rows.length || !descriptor) return null;

  // Error state with retry
  if (errored) {
    return (
      <div className="mt-3 flex items-center gap-2 rounded border border-[var(--line)] bg-[var(--surface-2)] px-3 py-2.5 text-[12px] text-[var(--muted)]">
        <span>Map failed to load.</span>
        <button
          type="button"
          onClick={() => { GEOJSON_CACHE.clear(); setRetryKey((k) => k + 1); }}
          className="inline-flex items-center gap-1 text-[var(--ink)] hover:underline"
        >
          <RotateCcw size={11} /> Retry
        </button>
      </div>
    );
  }

  if (!preparedMap) return null;

  const breaks = computeQuintileBreaks(preparedMap.values);
  const hoveredQuintile = hovered ? getQuintile(hovered.value, breaks) : null;
  const isModal = variant === 'modal';
  const mapHeight = mapHeightClassName ?? (isModal ? 'h-[min(72vh,760px)]' : 'h-[240px]');
  const selectedView = viewOptions.find((option) => option.id === activeView) ?? viewOptions[0] ?? null;

  return (
    <section className={`${isModal ? 'overflow-hidden rounded-[10px] border border-black/5 bg-[var(--surface)]/95 shadow-[0_14px_42px_rgba(15,23,42,0.07)]' : 'mt-3 overflow-hidden rounded-[10px] border border-black/5 bg-[var(--surface)]'}`}>
      <div className="border-b border-black/5 bg-white/90 px-3 py-3">
        {viewOptions.length > 1 && (
          <div className="flex flex-wrap gap-2">
            {viewOptions.map((option) => {
              const selected = option.id === activeView;
              return (
                <button
                  key={option.id}
                  type="button"
                  onClick={() => setActiveView(option.id)}
                  className={`inline-flex items-center justify-center rounded-[6px] border px-3 py-1.5 text-[11px] font-medium transition ${
                    selected
                      ? 'border-[var(--accent)] bg-[var(--accent-soft)] text-[var(--ink)]'
                      : 'border-black/6 bg-white text-[var(--muted)] hover:border-[var(--accent)]/30 hover:text-[var(--ink)]'
                  }`}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        )}
        {selectedView && (
          <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px] leading-5 text-[var(--muted)]">
            <span className="inline-flex items-center rounded-[6px] border border-black/6 bg-[var(--surface)] px-2 py-1 font-medium text-[var(--ink)]">
              {selectedView.label}
            </span>
            <span>{selectedView.description}</span>
          </div>
        )}
      </div>
      {/* Map container */}
      <div className="map-shell relative bg-[#f8fafc]">
        <div ref={mapContainerRef} className={`${mapHeight} w-full`} />

        {/* Loading overlay */}
        {(loading || !mapReady) && (
          <div className="absolute inset-0 flex items-center justify-center bg-[var(--surface-2)]/60 backdrop-blur-[1px]">
            <div className="inline-flex items-center gap-1.5 rounded-[6px] border border-black/6 bg-white px-3 py-1.5 text-[11px] text-[var(--muted)] shadow-sm">
              <Loader2 size={12} className="animate-spin" /> Loading map...
            </div>
          </div>
        )}

        {/* Hover tooltip (positioned top-left on the map) */}
        {mapReady && hovered && (
          <div className="absolute left-2.5 top-2.5 z-10 rounded-[6px] border border-black/6 bg-white/95 px-2.5 py-1.5 shadow-sm">
            <div className="text-[11px] font-medium text-[var(--ink)]">{hovered.label}</div>
            <div className="mt-0.5 flex items-center gap-2">
              <span className="font-mono text-[12px] font-semibold text-[var(--ink)]">{formatMetricValue(hovered.value)}</span>
              {hoveredQuintile && (
                <span
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ backgroundColor: QUINTILE_COLORS[hoveredQuintile - 1] }}
                />
              )}
            </div>
          </div>
        )}
      </div>

      {/* Legend bar — dashboard style */}
      <div className="flex items-center gap-3 border-t border-black/5 px-3 py-2">
        <span className="shrink-0 text-[10px] font-medium uppercase tracking-wider text-[var(--muted)]">
          {selectedView?.id === 'heat' ? `${preparedMap.metricLabel} · Calculated quintiles` : preparedMap.metricLabel}
        </span>
        <div className="ml-auto flex items-center gap-0.5">
          {QUINTILE_COLORS.map((color, i) => (
            <div key={color} className="flex flex-col items-center">
              <span
                className="block h-2.5 w-6 first:rounded-l-[3px] last:rounded-r-[3px]"
                style={{ backgroundColor: color }}
              />
              <span className="mt-0.5 text-[8px] text-[var(--muted-2)]">{QUINTILE_LABELS[i]}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
