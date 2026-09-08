import { AlertTriangle, RotateCcw, X } from 'lucide-react';
import { useEffect, useId, useMemo, useRef, useState } from 'react';
import maplibregl, { type LngLatBoundsLike, type MapGeoJSONFeature, type StyleSpecification } from 'maplibre-gl';
import { buildApiUrl } from '@/lib/api';
import type { ChatbotMapIntent } from '@/types/chat';

/* ────────────────────────────────────────────────────────────────────
   MapView — the one map surface. A pure-vector choropleth drawn on the
   app's warm canvas (no raster tiles, no external requests), driven
   entirely by the rows already attached to the answer. Interactions:
   fly-in on open, cursor tooltip with value + rank, click to pin a
   region, top-3 chips that fly to their region.
   ──────────────────────────────────────────────────────────────────── */

type GeoLevel = 'state' | 'county' | 'congress';

interface GeoFeature {
  type: 'Feature';
  geometry: { type: string; coordinates: unknown };
  properties: Record<string, unknown>;
}
interface GeoCollection {
  type: 'FeatureCollection';
  features: GeoFeature[];
}

interface Region {
  key: string;
  label: string;
  value: number;
  rank: number;
  tied?: boolean;
}

interface JoinStats {
  matched: number;
  requested: number;
  unmatchedLabels: string[];
}

const GEO_CACHE = new Map<string, Promise<GeoCollection>>();
function fetchGeo(name: 'states' | 'counties' | 'congress'): Promise<GeoCollection> {
  if (!GEO_CACHE.has(name)) {
    const request = fetch(buildApiUrl(`/geo/${name}.geojson`))
      .then(async (r) => {
        if (!r.ok) throw new Error(`Failed to load ${name} boundaries`);
        return (await r.json()) as GeoCollection;
      })
      .catch((error) => {
        // A transient network failure must not poison the cache permanently.
        GEO_CACHE.delete(name);
        throw error;
      });
    GEO_CACHE.set(name, request);
  }
  return GEO_CACHE.get(name)!;
}

/* ── Value/label helpers ── */

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
  'puerto rico': 'PR', guam: 'GU', 'american samoa': 'AS',
  'virgin islands': 'VI', 'u.s. virgin islands': 'VI', 'united states virgin islands': 'VI',
  'northern mariana islands': 'MP', 'commonwealth of northern mariana islands': 'MP',
  'commonwealth of the northern mariana islands': 'MP',
};
const POSTAL_TO_STATE = Object.fromEntries(Object.entries(STATE_TO_POSTAL).map(([n, a]) => [a, n]));
const STATE_FIPS_TO_POSTAL: Record<string, string> = {
  '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA', '08': 'CO', '09': 'CT',
  '10': 'DE', '11': 'DC', '12': 'FL', '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL',
  '18': 'IN', '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME', '24': 'MD',
  '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS', '29': 'MO', '30': 'MT', '31': 'NE',
  '32': 'NV', '33': 'NH', '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
  '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI', '45': 'SC', '46': 'SD',
  '47': 'TN', '48': 'TX', '49': 'UT', '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV',
  '55': 'WI', '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR', '78': 'VI',
};

function featurePostal(feature: GeoFeature): string | null {
  const abbr = String(feature.properties.abbr ?? '').toUpperCase();
  if (abbr) return abbr;
  const district = String(feature.properties.cd_118 ?? '').toUpperCase();
  if (/^[A-Z]{2}-/.test(district)) return district.slice(0, 2);
  const stateFips = String(feature.properties.statefp ?? '').padStart(2, '0');
  if (stateFips === '02') return 'AK';
  if (stateFips === '15') return 'HI';
  return null;
}

function transformCoordinates(
  coordinates: unknown,
  transform: (longitude: number, latitude: number) => [number, number],
): unknown {
  if (!Array.isArray(coordinates)) return coordinates;
  if (
    coordinates.length >= 2
    && typeof coordinates[0] === 'number'
    && typeof coordinates[1] === 'number'
  ) {
    const [longitude, latitude] = transform(coordinates[0], coordinates[1]);
    return [longitude, latitude, ...coordinates.slice(2)];
  }
  return coordinates.map((child) => transformCoordinates(child, transform));
}

/**
 * Keep non-contiguous states visible without letting Alaska's antimeridian
 * geometry collapse the national view. This mirrors the inset convention
 * used by policy choropleths; feature identities and data values are intact.
 */
function withNationalInsets(collection: GeoCollection): GeoCollection {
  return {
    ...collection,
    features: collection.features.map((feature) => {
      const postal = featurePostal(feature);
      const transforms: Partial<Record<string, (longitude: number, latitude: number) => [number, number]>> = {
        AK: (longitude: number, latitude: number): [number, number] => {
            const normalizedLongitude = longitude > 0 ? longitude - 360 : longitude;
            return [
              -115 + (normalizedLongitude + 152) * 0.32,
              20 + (latitude - 61.5) * 0.32,
            ];
        },
        HI: (longitude: number, latitude: number): [number, number] => [longitude + 55, latitude],
        PR: (longitude: number, latitude: number): [number, number] => [-79 + (longitude + 66.4) * 0.55, 23 + (latitude - 18.2) * 0.55],
        VI: (longitude: number, latitude: number): [number, number] => [-76.5 + (longitude + 64.8) * 0.7, 23 + (latitude - 18.3) * 0.7],
        GU: (longitude: number, latitude: number): [number, number] => [-73.5 + (longitude - 144.75) * 0.55, 23 + (latitude - 13.45) * 0.55],
        MP: (longitude: number, latitude: number): [number, number] => [-70.5 + (longitude - 145.7) * 0.32, 23 + (latitude - 15.2) * 0.32],
        AS: (longitude: number, latitude: number): [number, number] => [-67.5 + (longitude + 170.7) * 0.5, 23 + (latitude + 14.3) * 0.5],
      };
      const transform = postal ? transforms[postal] : undefined;
      if (!transform) return feature;
      return {
        ...feature,
        geometry: {
          ...feature.geometry,
          coordinates: transformCoordinates(feature.geometry.coordinates, transform),
        },
      };
    }),
  };
}

function toNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v.replace(/,/g, '').trim());
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function titleCase(s: string): string {
  return s.split(/\s+/).filter(Boolean).map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

function normName(v: unknown): string {
  return String(v ?? '').toLowerCase().replace(/\bcounty\b/g, '').replace(/[^\w\s-]/g, ' ').replace(/\s+/g, ' ').trim();
}

function toAbbr(v: unknown): string | null {
  if (typeof v !== 'string') return null;
  const t = v.trim();
  if (t.length === 2 && POSTAL_TO_STATE[t.toUpperCase()]) return t.toUpperCase();
  return STATE_TO_POSTAL[t.toLowerCase()] ?? null;
}

function normalizedFips(v: unknown, width: number): string | null {
  if (typeof v !== 'string' && typeof v !== 'number') return null;
  const digits = String(v).trim().replace(/\.0$/, '').replace(/\D/g, '');
  if (!digits || digits.length > width) return null;
  return digits.padStart(width, '0');
}

function normDistrict(v: unknown): string | null {
  if (typeof v !== 'string') return null;
  const text = v.trim();
  const direct = text.toUpperCase().match(/^([A-Z]{2})[-\s]?0?(\d{1,2})$/);
  if (direct) return `${direct[1]}-${direct[2].padStart(2, '0')}`;
  const named = text.match(/^(.+?)\s+CD[-\s]?0?(\d{1,2})$/i);
  if (!named) return null;
  const state = toAbbr(named[1]);
  return state ? `${state}-${named[2].padStart(2, '0')}` : null;
}

const MONEY_RE = /(contract|grant|payment|wage|fund|amount|asset|liabilit|revenue|expense|spend|income|bond|opeb|pension|cash|subaward|flow)/i;
function normalizedUnit(metric: string, unit?: string): string {
  if (unit) return unit.toLowerCase();
  if (!/ratio/i.test(metric) && MONEY_RE.test(metric)) return 'usd';
  if (/(percent|percentage|share|\brate\b)/i.test(metric)) return 'percent';
  if (/(population|people|persons|count|household)/i.test(metric)) return 'persons';
  return 'value';
}

function fmtValue(v: number, unit: string): string {
  if (!Number.isFinite(v)) return 'N/A';
  if (unit === 'percent') return `${v.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
  const sign = v < 0 ? '-' : '';
  const p = unit === 'usd' ? `${sign}$` : sign;
  const a = Math.abs(v);
  if (a >= 1e9) return `${p}${(a / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`;
  if (a >= 1e6) return `${p}${(a / 1e6).toLocaleString(undefined, { maximumFractionDigits: 1 })}M`;
  if (a >= 1e3) return `${p}${a.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  return `${p}${a.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

/* ── Choropleth scales: quantiles; zero-centered when signs diverge ── */
const RAMP = ['#fee5d9', '#fcae91', '#fb6a4a', '#de2d26', '#a50f15'];
const FLOW_IN_RAMP = ['#dbeafe', '#93c5fd', '#60a5fa', '#2563eb', '#1e3a8a'];
const DIVERGING_RAMP = ['#1e40af', '#60a5fa', '#f8fafc', '#fc8181', '#991b1b'];
const NO_VALUE_COLOR = '#e5e7eb';

interface ColorScale {
  colors: string[];
  breaks: number[];
  diverging: boolean;
  method: 'quantile' | 'zero-centered' | 'single-value';
  min: number;
  max: number;
}

function quantileBreaks(values: number[], maxBands = 5): number[] {
  const sorted = [...values].sort((a, b) => a - b);
  const uniqueCount = new Set(sorted).size;
  const bandCount = Math.min(maxBands, uniqueCount);
  if (bandCount <= 1) return [];
  const output: number[] = [];
  for (let band = 1; band < bandCount; band += 1) {
    let index = Math.ceil((sorted.length * band) / bandCount);
    while (index < sorted.length && sorted[index] === sorted[index - 1]) index += 1;
    if (index >= sorted.length) continue;
    const threshold = sorted[index - 1] + (sorted[index] - sorted[index - 1]) / 2;
    if (!output.length || threshold > output[output.length - 1]) output.push(threshold);
  }
  return output;
}

function bucket(v: number, breaks: number[]): number {
  for (let i = 0; i < breaks.length; i++) if (v < breaks[i]) return i;
  return breaks.length;
}

function selectRamp(ramp: string[], count: number): string[] {
  if (count <= 1) return [ramp[ramp.length - 1]];
  return Array.from({ length: count }, (_, index) => (
    ramp[Math.round((index * (ramp.length - 1)) / (count - 1))]
  ));
}

function createColorScale(values: number[], ramp = RAMP): ColorScale {
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min < 0 && max > 0) {
    const extent = Math.max(Math.abs(min), Math.abs(max)) || 1;
    return {
      colors: DIVERGING_RAMP,
      breaks: [-0.6 * extent, -0.2 * extent, 0.2 * extent, 0.6 * extent],
      diverging: true,
      method: 'zero-centered',
      min,
      max,
    };
  }
  const breaks = quantileBreaks(values);
  return {
    colors: selectRamp(ramp, breaks.length + 1),
    breaks,
    diverging: false,
    method: breaks.length ? 'quantile' : 'single-value',
    min,
    max,
  };
}

function colorFor(v: number, scale: ColorScale): string {
  return scale.colors[Math.min(scale.colors.length - 1, bucket(v, scale.breaks))];
}

function scaleBandLabel(scale: ColorScale, index: number, unit: string): string {
  if (!scale.breaks.length) return `${fmtValue(scale.min, unit)} (all)`;
  const lower = index > 0 ? scale.breaks[index - 1] : null;
  const upper = index < scale.breaks.length ? scale.breaks[index] : null;
  if (lower === null && upper !== null) return `< ${fmtValue(upper, unit)}`;
  if (lower !== null && upper === null) return `≥ ${fmtValue(lower, unit)}`;
  return `${fmtValue(lower!, unit)}–<${fmtValue(upper!, unit)}`;
}

/* ── Geometry bounds (walks coordinates arrays) ── */
type Bounds = [number, number, number, number];
function walkCoords(c: unknown, b: Bounds): void {
  if (!Array.isArray(c) || !c.length) return;
  if (typeof c[0] === 'number' && typeof c[1] === 'number') {
    const [lng, lat] = c as number[];
    if (Number.isFinite(lng) && Number.isFinite(lat)) {
      b[0] = Math.min(b[0], lng); b[1] = Math.min(b[1], lat);
      b[2] = Math.max(b[2], lng); b[3] = Math.max(b[3], lat);
    }
    return;
  }
  c.forEach((x) => walkCoords(x, b));
}
function boundsOf(features: GeoFeature[]): Bounds | null {
  const b: Bounds = [Infinity, Infinity, -Infinity, -Infinity];
  features.forEach((f) => walkCoords(f.geometry?.coordinates, b));
  return Number.isFinite(b[0]) ? b : null;
}

function centerOf(feature: GeoFeature): [number, number] | null {
  const bounds = boundsOf([feature]);
  return bounds ? [(bounds[0] + bounds[2]) / 2, (bounds[1] + bounds[3]) / 2] : null;
}

function curveBetween(start: [number, number], end: [number, number], points = 32): number[][] {
  const [startLng, startLat] = start;
  const [endLng, endLat] = end;
  const dx = endLng - startLng;
  const dy = endLat - startLat;
  const distance = Math.sqrt(dx * dx + dy * dy) || 1;
  const bend = Math.min(distance * 0.18, 7);
  const controlLng = (startLng + endLng) / 2 - (dy / distance) * bend;
  const controlLat = (startLat + endLat) / 2 + (dx / distance) * bend;
  return Array.from({ length: points + 1 }, (_, index) => {
    const t = index / points;
    const inverse = 1 - t;
    return [
      inverse * inverse * startLng + 2 * inverse * t * controlLng + t * t * endLng,
      inverse * inverse * startLat + 2 * inverse * t * controlLat + t * t * endLat,
    ];
  });
}

/* ── Row → region matching ── */

interface DetectedRegions {
  level: GeoLevel;
  regions: Map<string, Region>;
  focusAbbr: string | null;
  metricCol: string;
  rankDirection: 'highest' | 'lowest';
}

function firstValue(row: Record<string, unknown>, keys: string[]): unknown {
  for (const key of keys) {
    const value = row[key];
    if (value !== undefined && value !== null && String(value).trim()) return value;
  }
  return null;
}

function fallbackGeoValue(
  row: Record<string, unknown>,
  level: GeoLevel,
  side: ChatbotMapIntent['geoSide'],
): unknown {
  const geographyPattern = level === 'state' ? /state/i : level === 'county' ? /county|cty/i : /district|(^|_)cd($|_)/i;
  const sidePattern = side === 'source'
    ? /send|source|origin|prime|rcpt/i
    : side === 'destination'
      ? /receiv|dest|subawardee/i
      : null;
  const candidates = Object.entries(row).filter(
    ([key, value]) => typeof value === 'string'
      && value.trim()
      && geographyPattern.test(key)
      && !/fips|code|_id$/i.test(key),
  );
  const sided = sidePattern ? candidates.filter(([key]) => sidePattern.test(key)) : [];
  if (sidePattern) return sided.length === 1 ? sided[0][1] : null;
  return candidates.length === 1 ? candidates[0][1] : null;
}

function detectRegions(rows: Record<string, unknown>[], mapIntent: ChatbotMapIntent): DetectedRegions | null {
  if (!rows.length) return null;
  // metric column: named one if present, else first numeric non-geo column
  const first = rows[0];
  let metricCol = mapIntent.metric && mapIntent.metric in first ? mapIntent.metric : '';
  if (!metricCol) {
    for (const k of Object.keys(first)) {
      if (/(^state$|^county$|^cd_118$|fips|_name$|^year$|^rank$)/i.test(k)) continue;
      if (toNumber(first[k]) !== null) { metricCol = k; break; }
    }
  }
  if (!metricCol) return null;

  const side = mapIntent.geoSide ?? 'direct';
  const rankDirection: DetectedRegions['rankDirection'] = mapIntent.sortDirection === 'asc' ? 'lowest' : 'highest';
  const districts = new Map<string, Region>();
  const counties = new Map<string, Region>();
  const states = new Map<string, Region>();
  let duplicate = false;

  const districtKeys = side === 'source'
    ? ['source_district', 'origin_district', 'rcpt_cd_name', 'prime_awardee_stcd118']
    : side === 'destination'
      ? ['destination_district', 'subawardee_cd_name', 'subawardee_stcd118']
      : ['cd_118', 'district', 'label'];
  const countyKeys = side === 'source'
    ? ['source_county', 'origin_county', 'rcpt_cty_name']
    : side === 'destination'
      ? ['destination_county', 'subawardee_cty_name']
      : ['county', 'county_name', 'label'];
  const stateKeys = side === 'source'
    ? ['source', 'origin', 'source_state', 'rcpt_state_name', 'rcpt_state', 'rcpt_st_cd']
    : side === 'destination'
      ? ['destination', 'destination_state', 'subawardee_state_name', 'subawardee_state', 'subawardee_st_cd']
      : ['state', 'state_name', 'label'];
  const countyStateKeys = side === 'source'
    ? ['source_state', 'rcpt_state_name', 'rcpt_state']
    : side === 'destination'
      ? ['destination_state', 'subawardee_state_name', 'subawardee_state']
      : ['state', 'state_name', 'state_abbr'];
  const stateFipsKeys = side === 'source'
    ? ['source_state_fips', 'rcpt_state_fips', 'rcpt_st_fips']
    : side === 'destination'
      ? ['destination_state_fips', 'subawardee_state_fips', 'subawardee_st_fips']
      : ['state_fips', 'statefp'];
  const countyFipsKeys = side === 'source'
    ? ['source_county_fips', 'rcpt_cty_fips', 'rcpt_cty']
    : side === 'destination'
      ? ['destination_county_fips', 'subawardee_cty_fips', 'subawardee_cty']
      : ['county_fips', 'fips', 'geoid'];

  const setUnique = (collection: Map<string, Region>, region: Region) => {
    if (collection.has(region.key)) duplicate = true;
    collection.set(region.key, region);
  };

  const levels: GeoLevel[] = mapIntent.level
    ? [mapIntent.level]
    : ['congress', 'county', 'state'];
  for (const row of rows) {
    const value = toNumber(row[metricCol]);
    if (value === null) continue;
    for (const level of levels) {
      if (level === 'congress') {
        const cd = normDistrict(firstValue(row, districtKeys) ?? fallbackGeoValue(row, 'congress', side));
        if (cd) {
          setUnique(districts, { key: cd, label: cd, value, rank: 0 });
          break;
        }
      } else if (level === 'county') {
        const countyFips = normalizedFips(firstValue(row, countyFipsKeys), 5);
        const county = firstValue(row, countyKeys) ?? fallbackGeoValue(row, 'county', side);
        const fipsState = countyFips ? STATE_FIPS_TO_POSTAL[countyFips.slice(0, 2)] : null;
        const cState = toAbbr(firstValue(row, countyStateKeys) ?? fallbackGeoValue(row, 'state', side)) ?? fipsState;
        if (countyFips) {
          const label = county && cState
            ? `${titleCase(normName(county))}, ${cState}`
            : `County FIPS ${countyFips}`;
          setUnique(counties, { key: `county:${countyFips}`, label, value, rank: 0 });
          break;
        }
        if (county && cState) {
          const key = `${cState}:${normName(county)}`;
          setUnique(counties, { key, label: `${titleCase(normName(county))}, ${cState}`, value, rank: 0 });
          break;
        }
      } else {
        const stateFips = normalizedFips(firstValue(row, stateFipsKeys), 2);
        const stateFromFips = stateFips ? STATE_FIPS_TO_POSTAL[stateFips] : null;
        const st = toAbbr(firstValue(row, stateKeys) ?? fallbackGeoValue(row, 'state', side)) ?? stateFromFips;
        if (st) {
          const key = stateFips && stateFromFips ? `state:${stateFips}` : st;
          setUnique(states, { key, label: titleCase(POSTAL_TO_STATE[st] ?? st), value, rank: 0 });
          break;
        }
      }
    }
  }
  // Multiple values for one boundary cannot be represented faithfully by a
  // choropleth. The server normally prevents this; keep this UI guard too.
  if (duplicate) return null;

  const pick = (m: Map<string, Region>, level: GeoLevel) => {
    const direction = rankDirection === 'lowest' ? 1 : -1;
    const ordered = [...m.entries()].sort((a, b) => (
      direction * (a[1].value - b[1].value) || a[1].label.localeCompare(b[1].label)
    ));
    const valueCounts = new Map<number, number>();
    ordered.forEach(([, region]) => valueCounts.set(region.value, (valueCounts.get(region.value) ?? 0) + 1));
    let previous: number | null = null;
    let rank = 0;
    ordered.forEach(([, region], index) => {
      if (previous === null || region.value !== previous) rank = index + 1;
      region.rank = rank;
      region.tied = (valueCounts.get(region.value) ?? 0) > 1;
      previous = region.value;
    });
    const regions = new Map(ordered);
    const stateSet = new Set(
      [...regions.keys()].map((key) => {
        if (level === 'county') {
          if (key.startsWith('county:')) return STATE_FIPS_TO_POSTAL[key.slice(7, 9)] ?? null;
          return key.split(':')[0];
        }
        if (level === 'congress') return key.split('-')[0];
        return null;
      }).filter(Boolean),
    );
    return { level, regions, focusAbbr: stateSet.size === 1 ? ([...stateSet][0] as string) : null, metricCol, rankDirection };
  };

  if (mapIntent.level === 'congress') return districts.size ? pick(districts, 'congress') : null;
  if (mapIntent.level === 'county') return counties.size ? pick(counties, 'county') : null;
  if (mapIntent.level === 'state') return states.size ? pick(states, 'state') : null;
  if (districts.size) return pick(districts, 'congress');
  if (counties.size) return pick(counties, 'county');
  if (states.size) return pick(states, 'state');
  return null;
}

function featureKeys(level: GeoLevel, f: GeoFeature): string[] {
  if (level === 'state') {
    const a = String(f.properties.abbr ?? '').toUpperCase();
    const fips = normalizedFips(f.properties.id, 2);
    return [a || null, fips ? `state:${fips}` : null].filter((value): value is string => Boolean(value));
  }
  if (level === 'county') {
    const a = String(f.properties.abbr ?? '').toUpperCase();
    const fips = normalizedFips(f.properties.id, 5);
    return [
      fips ? `county:${fips}` : null,
      a ? `${a}:${normName(f.properties.name)}` : null,
    ].filter((value): value is string => Boolean(value));
  }
  const district = normDistrict(f.properties.cd_118);
  return district ? [district] : [];
}

/* ── Component ── */

interface MapViewProps {
  isOpen: boolean;
  onClose: () => void;
  mapIntent: ChatbotMapIntent;
  rows: Record<string, unknown>[];
}

export function MapView({ isOpen, onClose, mapIntent, rows }: MapViewProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const featureBoundsRef = useRef(new Map<string, Bounds>());
  const viewBoundsRef = useRef<Bounds | null>(null);
  const hoveredIdRef = useRef<number | string | null>(null);
  const [ready, setReady] = useState(false);
  const [failed, setFailed] = useState<string | null>(null);
  const [hover, setHover] = useState<{ region: Region; x: number; y: number } | null>(null);
  const [pinned, setPinned] = useState<Region | null>(null);
  const [joinStats, setJoinStats] = useState<JoinStats | null>(null);

  const titleId = useId();
  const detected = useMemo(() => detectRegions(rows, mapIntent), [rows, mapIntent]);
  const unit = useMemo(
    () => normalizedUnit(detected?.metricCol ?? mapIntent.metric ?? '', mapIntent.unit),
    [detected?.metricCol, mapIntent.metric, mapIntent.unit],
  );
  const metricLabel = mapIntent.metricLabel ?? (detected?.metricCol ?? mapIntent.metric ?? 'value').replace(/_/g, ' ');
  const mapSubtitle = mapIntent.subtitle?.trim();
  const showSubtitle = Boolean(mapSubtitle && !/^geographic view$/i.test(mapSubtitle));
  const regionList = useMemo(() => (detected ? [...detected.regions.values()] : []), [detected]);
  const values = useMemo(() => regionList.map((region) => region.value), [regionList]);
  const isFlow = mapIntent.mapType.startsWith('flow-');
  const focusPostal = toAbbr(mapIntent.state);
  const flowDirection = isFlow && focusPostal
    ? mapIntent.flowDirection === 'inflow' || mapIntent.flowDirection === 'outflow'
      ? mapIntent.flowDirection
      : mapIntent.geoSide === 'source' ? 'inflow' : 'outflow'
    : null;
  const valueRamp = isFlow && flowDirection !== 'outflow' ? FLOW_IN_RAMP : RAMP;
  const colorScale = useMemo(
    () => createColorScale(values.length ? values : [0], valueRamp),
    [values, valueRamp],
  );

  // esc to close + scroll lock
  useEffect(() => {
    if (!isOpen) return undefined;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    const prior = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => { document.removeEventListener('keydown', onKey); document.body.style.overflow = prior; };
  }, [isOpen, onClose]);

  // build the map
  useEffect(() => {
    if (!isOpen || !detected || !containerRef.current) return undefined;
    let disposed = false;
    featureBoundsRef.current.clear();
    viewBoundsRef.current = null;
    setReady(false); setFailed(null); setPinned(null); setHover(null); setJoinStats(null);

    const style: StyleSpecification = {
      version: 8,
      sources: {},
      layers: [{ id: 'bg', type: 'background', paint: { 'background-color': '#fafafa' } }],
    };
    const map = new maplibregl.Map({
      container: containerRef.current,
      style,
      attributionControl: false,
      dragRotate: false,
      pitchWithRotate: false,
      scrollZoom: false,
    });
    mapRef.current = map;
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');

    const load = async () => {
      try {
        const statesData = withNationalInsets(await fetchGeo('states'));
        const geoData = detected.level === 'state' ? statesData
          : detected.level === 'county' ? withNationalInsets(await fetchGeo('counties'))
          : withNationalInsets(await fetchGeo('congress'));
        if (disposed) return;

        // Base context: national state outlines, always.
        map.addSource('states-base', { type: 'geojson', data: statesData as never });
        map.addLayer({
          id: 'states-base-fill', type: 'fill', source: 'states-base',
          paint: { 'fill-color': NO_VALUE_COLOR, 'fill-opacity': 0.72 },
        });
        map.addLayer({
          id: 'states-base-line', type: 'line', source: 'states-base',
          paint: { 'line-color': '#fecaca', 'line-width': 0.75 },
        });
        if (focusPostal && mapIntent.mapType.startsWith('flow-')) {
          // The rows describe the opposite side of a focused flow (origins
          // into the state or destinations from it), so keep the focal state
          // visibly anchored even when it has no value row of its own.
          map.addLayer({
            id: 'flow-focus-line',
            type: 'line',
            source: 'states-base',
            filter: ['==', ['get', 'abbr'], focusPostal],
            // A solid Maryland-gold outline stays legible around complex
            // coastlines; dashed dark strokes become visual noise around bays.
            paint: { 'line-color': '#ffd200', 'line-width': 2.4 },
          });
        }

        // Data features (joined to rows), with value/rank/color baked in.
        const dataFeatures: GeoFeature[] = [];
        const scopeFeatures: GeoFeature[] = [];
        const matchedKeys = new Set<string>();
        for (const f of geoData.features) {
          const key = featureKeys(detected.level, f).find((candidate) => detected.regions.has(candidate));
          const postal = featurePostal(f);
          const inFocus = !detected.focusAbbr
            || postal === detected.focusAbbr
            || detected.level === 'state';
          if (inFocus && detected.level !== 'state') scopeFeatures.push(f);
          if (!key) continue;
          const region = detected.regions.get(key);
          if (!region) continue;
          matchedKeys.add(key);
          const featureBounds = boundsOf([f]);
          if (featureBounds) featureBoundsRef.current.set(key, featureBounds);
          dataFeatures.push({
            ...f,
            properties: {
              ...f.properties,
              __key: key,
              __label: region.label,
              __value: region.value,
              __rank: region.rank,
              __tied: Boolean(region.tied),
              __color: colorFor(region.value, colorScale),
            },
          });
        }
        const unmatched = regionList.filter((region) => !matchedKeys.has(region.key));
        setJoinStats({
          matched: dataFeatures.length,
          requested: regionList.length,
          unmatchedLabels: unmatched.slice(0, 5).map((region) => region.label),
        });
        if (!dataFeatures.length) { setFailed('None of the returned places matched the map boundaries.'); return; }

        // Scope silhouette (e.g. all MD counties) so the state reads whole.
        if (scopeFeatures.length) {
          map.addSource('scope', { type: 'geojson', data: { type: 'FeatureCollection', features: scopeFeatures } as never });
          map.addLayer({ id: 'scope-fill', type: 'fill', source: 'scope', paint: { 'fill-color': NO_VALUE_COLOR, 'fill-opacity': 0.72 } });
          map.addLayer({ id: 'scope-line', type: 'line', source: 'scope', paint: { 'line-color': '#fecaca', 'line-width': 0.8 } });
        }

        map.addSource('data', { type: 'geojson', generateId: true, data: { type: 'FeatureCollection', features: dataFeatures } as never });
        map.addLayer({
          id: 'data-fill', type: 'fill', source: 'data',
          paint: {
            'fill-color': ['get', '__color'],
            'fill-opacity': ['case', ['boolean', ['feature-state', 'hover'], false], 1, 0.92],
          },
        });

        // A focused flow answer gets the directional arcs used by the main
        // MOP Fund Flow dashboard. The endpoints come from the same boundary
        // features as the choropleth, so no separate coordinate table or
        // guessed place matching is involved.
        if (isFlow && focusPostal && flowDirection) {
          const focusFeature = statesData.features.find((feature) => featurePostal(feature) === focusPostal);
          const focusCenter = focusFeature ? centerOf(focusFeature) : null;
          if (focusCenter) {
            const flowColor = flowDirection === 'inflow' ? '#2563eb' : '#e21833';
            const absoluteValues = regionList.map((item) => Math.abs(item.value));
            const absoluteBreaks = quantileBreaks(absoluteValues);
            const flowFeatures: GeoFeature[] = [];
            const endpointFeatures: GeoFeature[] = [];
            for (const feature of dataFeatures) {
              const key = String(feature.properties.__key ?? '');
              const region = detected.regions.get(key);
              const counterpart = centerOf(feature);
              if (!region || !counterpart || (detected.level === 'state' && featurePostal(feature) === focusPostal)) continue;
              const start = flowDirection === 'inflow' ? counterpart : focusCenter;
              const end = flowDirection === 'inflow' ? focusCenter : counterpart;
              const origin = flowDirection === 'inflow' ? region.label : titleCase(POSTAL_TO_STATE[focusPostal] ?? focusPostal);
              const destination = flowDirection === 'inflow' ? titleCase(POSTAL_TO_STATE[focusPostal] ?? focusPostal) : region.label;
              const widthBand = bucket(Math.abs(region.value), absoluteBreaks);
              const width = absoluteBreaks.length
                ? 1.25 + (3.35 * widthBand) / absoluteBreaks.length
                : 3;
              flowFeatures.push({
                type: 'Feature',
                geometry: { type: 'LineString', coordinates: curveBetween(start, end) },
                properties: {
                  __key: key,
                  __label: `${origin} → ${destination}`,
                  __value: region.value,
                  __rank: region.rank,
                  __tied: Boolean(region.tied),
                  __color: flowColor,
                  __width: width,
                },
              });
              endpointFeatures.push({
                type: 'Feature',
                geometry: { type: 'Point', coordinates: counterpart },
                properties: { __color: flowColor, __radius: width + 1.4, __stroke: '#ffffff', __strokeWidth: 1.25 },
              });
            }
            if (flowFeatures.length) {
              endpointFeatures.push({
                type: 'Feature',
                geometry: { type: 'Point', coordinates: focusCenter },
                properties: { __color: '#0f172a', __radius: 7, __stroke: '#ffd200', __strokeWidth: 2.25 },
              });
              map.addSource('flow-lines', {
                type: 'geojson', generateId: true,
                data: { type: 'FeatureCollection', features: flowFeatures } as never,
              });
              map.addLayer({
                id: 'flow-lines', type: 'line', source: 'flow-lines',
                paint: {
                  'line-color': ['get', '__color'],
                  'line-width': ['get', '__width'],
                  'line-opacity': 0.78,
                  'line-blur': 0.25,
                },
                layout: { 'line-cap': 'round', 'line-join': 'round' },
              });
              map.addLayer({
                id: 'flow-lines-hit', type: 'line', source: 'flow-lines',
                // Keep a generous pointer target without relying on a
                // transparent RGBA stroke, which some WebGL renderers can
                // rasterize as black artifacts where several arcs overlap.
                paint: { 'line-color': flowColor, 'line-opacity': 0, 'line-width': 12 },
              });
              map.addSource('flow-endpoints', {
                type: 'geojson', data: { type: 'FeatureCollection', features: endpointFeatures } as never,
              });
              map.addLayer({
                id: 'flow-endpoints', type: 'circle', source: 'flow-endpoints',
                paint: {
                  'circle-color': ['get', '__color'],
                  'circle-radius': ['get', '__radius'],
                  'circle-stroke-color': ['get', '__stroke'],
                  'circle-stroke-width': ['get', '__strokeWidth'],
                },
              });
              map.on('mousemove', 'flow-lines-hit', (event) => {
                const feature = event.features?.[0] as MapGeoJSONFeature | undefined;
                if (!feature) return;
                const props = feature.properties as Record<string, unknown>;
                map.getCanvas().style.cursor = 'pointer';
                setHover({
                  region: {
                    key: String(props.__key),
                    label: String(props.__label),
                    value: Number(props.__value),
                    rank: Number(props.__rank),
                    tied: Boolean(props.__tied),
                  },
                  x: event.point.x,
                  y: event.point.y,
                });
              });
              map.on('mouseleave', 'flow-lines-hit', () => {
                map.getCanvas().style.cursor = '';
                setHover(null);
              });
              map.on('click', 'flow-lines-hit', (event) => {
                const feature = event.features?.[0] as MapGeoJSONFeature | undefined;
                if (!feature) return;
                const props = feature.properties as Record<string, unknown>;
                setPinned({
                  key: String(props.__key),
                  label: String(props.__label),
                  value: Number(props.__value),
                  rank: Number(props.__rank),
                  tied: Boolean(props.__tied),
                });
              });
            }
          }
        }
        map.addLayer({
          id: 'data-line', type: 'line', source: 'data',
          paint: {
            'line-color': ['case', ['boolean', ['feature-state', 'hover'], false], '#0f172a', '#ffffff'],
            'line-width': ['case', ['boolean', ['feature-state', 'hover'], false], 1.6, 1],
          },
        });

        // Fly in — start wide, settle on the data.
        const target = boundsOf(scopeFeatures.length ? scopeFeatures : dataFeatures);
        if (target) {
          viewBoundsRef.current = target;
          const fitPadding = (containerRef.current?.clientWidth ?? 1024) < 640 ? 28 : 72;
          const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
          map.jumpTo({ center: [-96, 38], zoom: 2.4 });
          map.fitBounds(target as LngLatBoundsLike, {
            padding: fitPadding,
            duration: reduceMotion ? 0 : 1100,
            essential: false,
            maxZoom: detected.level === 'state' ? 5.5 : 8,
          });
        }

        // Hover: tooltip + feature-state emphasis
        const clearHover = () => {
          if (hoveredIdRef.current !== null) {
            map.setFeatureState({ source: 'data', id: hoveredIdRef.current }, { hover: false });
            hoveredIdRef.current = null;
          }
          setHover(null);
          map.getCanvas().style.cursor = '';
        };
        map.on('mousemove', 'data-fill', (e) => {
          const f = e.features?.[0] as MapGeoJSONFeature | undefined;
          if (!f) return;
          if (hoveredIdRef.current !== null && hoveredIdRef.current !== f.id) {
            map.setFeatureState({ source: 'data', id: hoveredIdRef.current }, { hover: false });
          }
          hoveredIdRef.current = f.id ?? null;
          if (f.id !== undefined) map.setFeatureState({ source: 'data', id: f.id }, { hover: true });
          map.getCanvas().style.cursor = 'pointer';
          const p = f.properties as Record<string, unknown>;
          setHover({
            region: {
              key: String(p.__key), label: String(p.__label), value: Number(p.__value),
              rank: Number(p.__rank), tied: Boolean(p.__tied),
            },
            x: e.point.x, y: e.point.y,
          });
        });
        map.on('mouseleave', 'data-fill', clearHover);
        map.on('click', 'data-fill', (e) => {
          const f = e.features?.[0] as MapGeoJSONFeature | undefined;
          if (!f) return;
          const p = f.properties as Record<string, unknown>;
          setPinned({
            key: String(p.__key), label: String(p.__label), value: Number(p.__value),
            rank: Number(p.__rank), tied: Boolean(p.__tied),
          });
        });

        setReady(true);
      } catch (err) {
        if (!disposed) setFailed(err instanceof Error ? err.message : 'Map failed to load.');
      }
    };
    map.on('load', () => void load());
    const onResize = () => map.resize();
    window.addEventListener('resize', onResize);
    const resizeObserver = typeof ResizeObserver === 'undefined'
      ? null
      : new ResizeObserver(() => map.resize());
    if (containerRef.current) resizeObserver?.observe(containerRef.current);
    return () => {
      disposed = true;
      window.removeEventListener('resize', onResize);
      resizeObserver?.disconnect();
      map.remove();
      mapRef.current = null;
      featureBoundsRef.current.clear();
      viewBoundsRef.current = null;
    };
  }, [isOpen, detected, colorScale, regionList, mapIntent.mapType, mapIntent.state, isFlow, focusPostal, flowDirection]);

  const flyToRegion = (region: Region) => {
    setPinned(region);
    const map = mapRef.current;
    if (!map) return;
    const bounds = featureBoundsRef.current.get(region.key);
    if (bounds) map.fitBounds(bounds as LngLatBoundsLike, { padding: 160, duration: 850, maxZoom: 8.5 });
  };

  const resetView = () => {
    setPinned(null);
    const map = mapRef.current;
    const bounds = viewBoundsRef.current;
    if (!map || !bounds) return;
    const padding = (containerRef.current?.clientWidth ?? 1024) < 640 ? 28 : 72;
    map.fitBounds(bounds as LngLatBoundsLike, { padding, duration: 700, maxZoom: detected?.level === 'state' ? 5.5 : 8 });
  };

  if (!isOpen) return null;

  const top3 = regionList.slice(0, 3);
  const minV = colorScale.min;
  const total = regionList.reduce((s, r) => s + r.value, 0);
  const diverging = colorScale.diverging;
  const pinnedPercentile = pinned
    ? Math.round((values.filter((value) => value <= pinned.value).length / Math.max(values.length, 1)) * 100)
    : null;
  const pinnedBand = pinned ? bucket(pinned.value, colorScale.breaks) + 1 : null;
  const geographyLabel = detected?.level === 'county'
    ? 'counties'
    : detected?.level === 'congress' ? 'districts' : 'states and territories';
  const mappedCount = joinStats?.matched ?? regionList.length;
  const returnedCount = mapIntent.returnedGeographyCount ?? regionList.length;
  const missingValueCount = mapIntent.missingValueCount ?? 0;
  const qualityNotes = [
    mapIntent.partialResult
      ? 'The query result was truncated; unshaded areas must not be interpreted as zero.'
      : null,
    joinStats && joinStats.matched < joinStats.requested
      ? `${joinStats.requested - joinStats.matched} returned ${joinStats.requested - joinStats.matched === 1 ? 'place did' : 'places did'} not match a boundary${joinStats.unmatchedLabels.length ? `: ${joinStats.unmatchedLabels.join(', ')}` : ''}.`
      : null,
    missingValueCount > 0
      ? `${missingValueCount} returned ${missingValueCount === 1 ? 'place has' : 'places have'} no numeric value for this measure.`
      : null,
  ].filter((note): note is string => Boolean(note));
  const tooltipLeft = hover
    ? Math.min(hover.x + 12, Math.max(12, (containerRef.current?.clientWidth ?? 320) - 252))
    : 0;
  const tooltipTop = hover ? Math.max(86, hover.y - 10) : 0;

  return (
    <div className="fixed inset-0 z-[120] bg-[#0f172a]/28 backdrop-blur-[2px]">
      <div className="absolute inset-0" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="absolute inset-3 flex flex-col overflow-hidden border border-[var(--line)] bg-[var(--surface)] shadow-[0_24px_64px_rgba(15,23,42,0.18)] sm:inset-6"
      >
        {/* Header */}
        <header className="flex items-start justify-between gap-4 border-b border-[var(--line)] px-5 py-3.5 sm:px-6">
          <div className="min-w-0">
            <div className="mop-kicker mb-1">{flowDirection ? `${flowDirection} view` : 'Geographic view'}</div>
            <h2 id={titleId} className="font-display text-[20px] font-medium leading-tight text-[var(--ink)] sm:text-[23px]">
              {mapIntent.title ?? `${metricLabel} by ${detected?.level === 'county' ? 'county' : detected?.level === 'congress' ? 'district' : 'state'}`}
            </h2>
            {showSubtitle && <p className="truncate text-[12px] text-[var(--muted)]">{mapSubtitle}</p>}
            <div className="mt-2 flex max-w-full flex-wrap items-center gap-1.5 text-[10.5px] text-[var(--muted)]">
              <span className="border border-[var(--line)] bg-[var(--bg)] px-2 py-0.5 tabular-nums">
                {mappedCount} mapped {geographyLabel}
                {returnedCount !== mappedCount ? ` of ${returnedCount} returned` : ''}
              </span>
              {mapIntent.periodLabel && (
                <span className="max-w-[360px] truncate border border-[var(--line)] bg-[var(--bg)] px-2 py-0.5" title={mapIntent.periodLabel}>
                  Period: {mapIntent.periodLabel}
                </span>
              )}
              {mapIntent.sourceLabel && (
                <span className="max-w-[360px] truncate border border-[var(--line)] bg-[var(--bg)] px-2 py-0.5" title={mapIntent.sourceLabel}>
                  Source: {mapIntent.sourceLabel}
                </span>
              )}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close map"
            autoFocus
            className="border border-[var(--line)] p-2 text-[var(--muted)] transition hover:border-[var(--brand-red)] hover:text-[var(--brand-red)]"
          >
            <X size={17} />
          </button>
        </header>

        {/* Map canvas */}
        <div className="relative min-h-0 flex-1">
          {!detected || failed ? (
            <div className="flex h-full items-center justify-center px-8 text-center text-[14px] text-[var(--muted)]">
              {failed ?? "This answer doesn't have mappable places."}
            </div>
          ) : (
            <>
              <div ref={containerRef} className="h-full w-full" />

              {!ready && (
                <div className="absolute inset-0 grid place-items-center bg-[var(--bg)]/70">
                  <span className="text-[13px] text-[var(--muted)]">Drawing the map…</span>
                </div>
              )}

              {qualityNotes.length > 0 && ready && (
                <div className="absolute left-1/2 top-3 z-20 flex max-w-[calc(100%_-_9rem)] -translate-x-1/2 items-start gap-2 border border-amber-300 bg-amber-50/95 px-3 py-2 text-[10.5px] leading-4 text-amber-950 shadow-sm sm:max-w-[620px]">
                  <AlertTriangle size={14} className="mt-px shrink-0 text-amber-700" />
                  <span>{qualityNotes.join(' ')}</span>
                </div>
              )}

              {ready && viewBoundsRef.current && (
                <button
                  type="button"
                  onClick={resetView}
                  className="absolute right-3 top-[78px] z-10 inline-flex items-center gap-1.5 border border-[var(--line)] bg-[var(--surface)]/95 px-2.5 py-1.5 text-[10.5px] font-semibold text-[var(--muted)] shadow-sm transition hover:border-[var(--brand-red)] hover:text-[var(--brand-red)]"
                  aria-label="Reset map extent"
                >
                  <RotateCcw size={12} /> Reset
                </button>
              )}

              {/* Cursor tooltip */}
              {hover && (
                <div
                  className="pointer-events-none absolute z-10 -translate-y-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2 shadow-[0_12px_26px_rgba(15,23,42,0.12)]"
                  style={{ left: tooltipLeft, top: tooltipTop, width: 240 }}
                >
                  <div className="text-[12.5px] font-semibold text-[var(--ink)]">{hover.region.label}</div>
                  <div className="mt-0.5 truncate text-[10px] text-[var(--muted-2)]">{metricLabel}{mapIntent.periodLabel ? ` · ${mapIntent.periodLabel}` : ''}</div>
                  <div className="tabular-nums mt-0.5 flex items-baseline gap-2 text-[13px]">
                    <span className="font-mono font-medium text-[var(--ink)]">{fmtValue(hover.region.value, unit)}</span>
                    <span className="text-[10.5px] text-[var(--muted)]">{hover.region.tied ? 'Tied ' : ''}#{hover.region.rank} {detected.rankDirection} of {regionList.length}</span>
                  </div>
                </div>
              )}

              {/* Top-3 quick-jump chips */}
              {top3.length > 1 && (
                <div className="absolute left-4 top-4 z-10 flex flex-col gap-1.5">
                  <div className="w-fit border border-[var(--line)] bg-[var(--surface)]/94 px-2 py-1 text-[9.5px] font-bold uppercase tracking-[0.12em] text-[var(--muted)]">
                    {detected.rankDirection === 'lowest' ? 'Lowest returned' : 'Highest returned'}
                  </div>
                  {top3.map((r) => (
                    <button
                      key={r.key}
                      type="button"
                      onClick={() => flyToRegion(r)}
                      className={`group flex items-center gap-2 border px-3 py-1.5 text-left transition ${
                        pinned?.key === r.key
                          ? 'border-[var(--brand-red)] bg-[var(--surface)]'
                          : 'border-[var(--line)] bg-[var(--surface)]/94 hover:border-[var(--brand-red)]'
                      }`}
                    >
                      <span className="grid h-5 w-5 shrink-0 place-items-center bg-[var(--ink)] text-[10px] font-bold text-white">
                        {r.tied ? `T${r.rank}` : r.rank}
                      </span>
                      <span className="max-w-44 truncate text-[12px] font-medium text-[var(--ink)]">{r.label}</span>
                      <span className="tabular-nums text-[11.5px] font-semibold text-[var(--muted)]">{fmtValue(r.value, unit)}</span>
                    </button>
                  ))}
                </div>
              )}

              {/* Pinned detail card */}
              {pinned && (
                <div className="absolute bottom-24 left-4 z-10 w-64 border border-[var(--line)] bg-[var(--surface)] p-4 shadow-[0_18px_36px_rgba(15,23,42,0.12)]">
                  <div className="flex items-start justify-between gap-2">
                    <div className="text-[14px] font-semibold leading-5 text-[var(--ink)]">{pinned.label}</div>
                    <button type="button" onClick={() => setPinned(null)} aria-label="Clear selection" className="rounded-md p-0.5 text-[var(--muted-2)] hover:text-[var(--ink)]">
                      <X size={13} />
                    </button>
                  </div>
                  <div className="tabular-nums mt-2 font-mono text-[22px] font-medium leading-none text-[var(--brand-red)]">
                    {fmtValue(pinned.value, unit)}
                  </div>
                  <div className="mt-2 space-y-1 text-[11.5px] leading-4 text-[var(--muted)]">
                    <div>{pinned.tied ? 'Tied rank' : 'Rank'} <span className="font-semibold text-[var(--ink)]">#{pinned.rank}</span> {detected.rankDirection} of {regionList.length} returned</div>
                    {pinnedPercentile !== null && (
                      <div>Value percentile <span className="font-semibold text-[var(--ink)]">{pinnedPercentile}</span> in the displayed result</div>
                    )}
                    {pinnedBand !== null && colorScale.colors.length > 1 && (
                      <div>Map band <span className="font-semibold text-[var(--ink)]">{pinnedBand}</span> of {colorScale.colors.length}</div>
                    )}
                    {unit === 'usd' && minV >= 0 && total > 0 && pinned.value > 0 && (
                      <div><span className="font-semibold text-[var(--ink)]">{((pinned.value / total) * 100).toFixed(1)}%</span> of the mapped total</div>
                    )}
                  </div>
                </div>
              )}

              {/* Legend */}
              {mapIntent.showLegend !== false && regionList.length > 0 && (
                <div className="absolute bottom-3 left-3 right-3 z-10 border border-[var(--line)] bg-[var(--surface)]/96 px-3 py-2 shadow-sm sm:left-1/2 sm:right-auto sm:max-w-[calc(100%_-_2rem)] sm:-translate-x-1/2">
                  <div className="mb-1.5 flex items-center justify-between gap-4 text-[9.5px] font-semibold uppercase tracking-[0.1em] text-[var(--muted)]">
                    <span>{metricLabel}</span>
                    <span className="whitespace-nowrap font-normal normal-case tracking-normal text-[var(--muted-2)]">
                      {diverging
                        ? 'Equal bands centered on zero'
                        : colorScale.method === 'single-value' ? 'Single returned value' : 'Quantile bands'}
                    </span>
                  </div>
                  <div className="flex max-w-full items-start gap-2 overflow-x-auto pb-0.5">
                    {colorScale.colors.map((color, index) => (
                      <div key={`${color}-${index}`} className="min-w-[88px] flex-1">
                        <span className="block h-2.5 w-full" style={{ backgroundColor: color }} />
                        <span className="mt-1 block whitespace-nowrap text-[9.5px] tabular-nums text-[var(--muted)]">
                          {scaleBandLabel(colorScale, index, unit)}
                        </span>
                      </div>
                    ))}
                    <div className="min-w-[76px]">
                      <span className="block h-2.5 w-full border border-[#d1d5db]" style={{ backgroundColor: NO_VALUE_COLOR }} />
                      <span className="mt-1 block whitespace-nowrap text-[9.5px] text-[var(--muted)]">Not returned</span>
                    </div>
                  </div>
                  {flowDirection && (
                    <div className="mt-1 text-[9.5px] text-[var(--muted-2)]">
                      Curves run {flowDirection === 'inflow' ? 'from each origin into' : 'from'}{' '}
                      <span className="font-semibold text-[var(--ink)]">{titleCase(POSTAL_TO_STATE[focusPostal ?? ''] ?? focusPostal ?? '')}</span>
                      {flowDirection === 'outflow' ? ' to each destination' : ''}; thicker curves represent larger absolute amounts.
                    </div>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
