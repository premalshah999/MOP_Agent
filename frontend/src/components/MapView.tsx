import { X } from 'lucide-react';
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
const RAMP = ['#f6e2d8', '#edc0ab', '#e09a79', '#cd6f47', '#a34c27'];
const DIVERGING_RAMP = ['#2868a8', '#7aadd2', '#eceae2', '#e3a080', '#a34c27'];

function quintileBreaks(values: number[]): number[] {
  const sorted = [...values].sort((a, b) => a - b);
  return [1, 2, 3, 4].map((s) => sorted[Math.min(sorted.length - 1, Math.floor((sorted.length * s) / 5))]);
}

function bucket(v: number, breaks: number[]): number {
  for (let i = 0; i < breaks.length; i++) if (v < breaks[i]) return i;
  return 4;
}

function colorFor(v: number, values: number[], breaks: number[]): string {
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min < 0 && max > 0) {
    const extent = Math.max(Math.abs(min), Math.abs(max)) || 1;
    const normalized = Math.max(-1, Math.min(1, v / extent));
    return DIVERGING_RAMP[Math.min(4, Math.floor(((normalized + 1) / 2) * 5))];
  }
  return RAMP[bucket(v, breaks)];
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
  if (sided.length === 1) return sided[0][1];
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
    ? ['source_district', 'origin_district', 'rcpt_cd_name', 'cd_118', 'district', 'label']
    : side === 'destination'
      ? ['destination_district', 'subawardee_cd_name', 'cd_118', 'district', 'label']
      : ['cd_118', 'district', 'label'];
  const countyKeys = side === 'source'
    ? ['source_county', 'origin_county', 'rcpt_cty_name', 'county', 'county_name', 'label']
    : side === 'destination'
      ? ['destination_county', 'subawardee_cty_name', 'county', 'county_name', 'label']
      : ['county', 'county_name', 'label'];
  const stateKeys = side === 'source'
    ? ['source', 'origin', 'source_state', 'rcpt_state_name', 'rcpt_state', 'state', 'state_name', 'label']
    : side === 'destination'
      ? ['destination', 'destination_state', 'subawardee_state_name', 'subawardee_state', 'state', 'state_name', 'label']
      : ['state', 'state_name', 'label'];
  const countyStateKeys = side === 'source'
    ? ['source_state', 'rcpt_state_name', 'rcpt_state', 'state', 'state_name', 'state_abbr']
    : side === 'destination'
      ? ['destination_state', 'subawardee_state_name', 'subawardee_state', 'state', 'state_name', 'state_abbr']
      : ['state', 'state_name', 'state_abbr'];

  const setUnique = (collection: Map<string, Region>, region: Region) => {
    if (collection.has(region.key)) duplicate = true;
    collection.set(region.key, region);
  };

  for (const row of rows) {
    const value = toNumber(row[metricCol]);
    if (value === null) continue;
    const cd = normDistrict(firstValue(row, districtKeys) ?? fallbackGeoValue(row, 'congress', side));
    if (cd) { setUnique(districts, { key: cd, label: cd, value, rank: 0 }); continue; }
    const county = firstValue(row, countyKeys) ?? fallbackGeoValue(row, 'county', side);
    const cState = toAbbr(firstValue(row, countyStateKeys) ?? fallbackGeoValue(row, 'state', side));
    if (county && cState) {
      const key = `${cState}:${normName(county)}`;
      setUnique(counties, { key, label: `${titleCase(normName(county))}, ${cState}`, value, rank: 0 });
      continue;
    }
    const st = toAbbr(firstValue(row, stateKeys) ?? fallbackGeoValue(row, 'state', side));
    if (st) setUnique(states, { key: st, label: titleCase(POSTAL_TO_STATE[st] ?? st), value, rank: 0 });
  }
  // Multiple values for one boundary cannot be represented faithfully by a
  // choropleth. The server normally prevents this; keep this UI guard too.
  if (duplicate) return null;

  const pick = (m: Map<string, Region>, level: GeoLevel) => {
    const direction = rankDirection === 'lowest' ? 1 : -1;
    const regions = new Map([...m.entries()].sort((a, b) => direction * (a[1].value - b[1].value)));
    let i = 0;
    regions.forEach((r) => { r.rank = ++i; });
    const stateSet = new Set(
      [...regions.keys()].map((k) => (level === 'county' ? k.split(':')[0] : level === 'congress' ? k.split('-')[0] : null)).filter(Boolean),
    );
    return { level, regions, focusAbbr: stateSet.size === 1 ? ([...stateSet][0] as string) : null, metricCol, rankDirection };
  };

  if (mapIntent.level === 'congress' && districts.size) return pick(districts, 'congress');
  if (mapIntent.level === 'county' && counties.size) return pick(counties, 'county');
  if (mapIntent.level === 'state' && states.size) return pick(states, 'state');
  if (districts.size) return pick(districts, 'congress');
  if (counties.size) return pick(counties, 'county');
  if (states.size) return pick(states, 'state');
  return null;
}

function featureKey(level: GeoLevel, f: GeoFeature): string | null {
  if (level === 'state') {
    const a = String(f.properties.abbr ?? '').toUpperCase();
    return a || null;
  }
  if (level === 'county') {
    const a = String(f.properties.abbr ?? '').toUpperCase();
    return a ? `${a}:${normName(f.properties.name)}` : null;
  }
  return normDistrict(f.properties.cd_118);
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
  const hoveredIdRef = useRef<number | string | null>(null);
  const [ready, setReady] = useState(false);
  const [failed, setFailed] = useState<string | null>(null);
  const [hover, setHover] = useState<{ region: Region; x: number; y: number } | null>(null);
  const [pinned, setPinned] = useState<Region | null>(null);

  const titleId = useId();
  const detected = useMemo(() => detectRegions(rows, mapIntent), [rows, mapIntent]);
  const unit = useMemo(
    () => normalizedUnit(detected?.metricCol ?? mapIntent.metric ?? '', mapIntent.unit),
    [detected?.metricCol, mapIntent.metric, mapIntent.unit],
  );
  const metricLabel = mapIntent.metricLabel ?? (detected?.metricCol ?? mapIntent.metric ?? 'value').replace(/_/g, ' ');
  const regionList = useMemo(() => (detected ? [...detected.regions.values()] : []), [detected]);
  const breaks = useMemo(() => (regionList.length ? quintileBreaks(regionList.map((r) => r.value)) : []), [regionList]);

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
    setReady(false); setFailed(null); setPinned(null); setHover(null);

    const style: StyleSpecification = {
      version: 8,
      sources: {},
      layers: [{ id: 'bg', type: 'background', paint: { 'background-color': '#f5f4ee' } }],
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
        const statesData = await fetchGeo('states');
        const geoData = detected.level === 'state' ? statesData
          : detected.level === 'county' ? await fetchGeo('counties')
          : await fetchGeo('congress');
        if (disposed) return;

        // Base context: national state outlines, always.
        map.addSource('states-base', { type: 'geojson', data: statesData as never });
        map.addLayer({
          id: 'states-base-fill', type: 'fill', source: 'states-base',
          paint: { 'fill-color': '#edebe2', 'fill-opacity': 0.65 },
        });
        map.addLayer({
          id: 'states-base-line', type: 'line', source: 'states-base',
          paint: { 'line-color': '#fdfcfa', 'line-width': 1 },
        });
        const focusPostal = toAbbr(mapIntent.state);
        if (focusPostal && mapIntent.mapType.startsWith('flow-')) {
          // The rows describe the opposite side of a focused flow (origins
          // into the state or destinations from it), so keep the focal state
          // visibly anchored even when it has no value row of its own.
          map.addLayer({
            id: 'flow-focus-line',
            type: 'line',
            source: 'states-base',
            filter: ['==', ['get', 'abbr'], focusPostal],
            paint: { 'line-color': '#1f1e1d', 'line-width': 2.4, 'line-dasharray': [2, 1] },
          });
        }

        // Data features (joined to rows), with value/rank/color baked in.
        const dataFeatures: GeoFeature[] = [];
        const scopeFeatures: GeoFeature[] = [];
        for (const f of geoData.features) {
          const key = featureKey(detected.level, f);
          if (!key) continue;
          const inFocus = !detected.focusAbbr
            || (detected.level === 'county' && key.startsWith(`${detected.focusAbbr}:`))
            || (detected.level === 'congress' && key.startsWith(`${detected.focusAbbr}-`))
            || detected.level === 'state';
          if (inFocus && detected.level !== 'state') scopeFeatures.push(f);
          const region = detected.regions.get(key);
          if (!region) continue;
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
              __color: colorFor(region.value, regionList.map((item) => item.value), breaks),
            },
          });
        }
        if (!dataFeatures.length) { setFailed('None of the returned places matched the map boundaries.'); return; }

        // Scope silhouette (e.g. all MD counties) so the state reads whole.
        if (scopeFeatures.length) {
          map.addSource('scope', { type: 'geojson', data: { type: 'FeatureCollection', features: scopeFeatures } as never });
          map.addLayer({ id: 'scope-fill', type: 'fill', source: 'scope', paint: { 'fill-color': '#e7e4da', 'fill-opacity': 0.9 } });
          map.addLayer({ id: 'scope-line', type: 'line', source: 'scope', paint: { 'line-color': '#fdfcfa', 'line-width': 0.8 } });
        }

        map.addSource('data', { type: 'geojson', generateId: true, data: { type: 'FeatureCollection', features: dataFeatures } as never });
        map.addLayer({
          id: 'data-fill', type: 'fill', source: 'data',
          paint: {
            'fill-color': ['get', '__color'],
            'fill-opacity': ['case', ['boolean', ['feature-state', 'hover'], false], 1, 0.92],
          },
        });
        map.addLayer({
          id: 'data-line', type: 'line', source: 'data',
          paint: {
            'line-color': ['case', ['boolean', ['feature-state', 'hover'], false], '#1f1e1d', '#fdfcfa'],
            'line-width': ['case', ['boolean', ['feature-state', 'hover'], false], 1.6, 1],
          },
        });

        // Fly in — start wide, settle on the data.
        const target = boundsOf(scopeFeatures.length ? scopeFeatures : dataFeatures);
        if (target) {
          map.jumpTo({ center: [-96, 38], zoom: 2.4 });
          map.fitBounds(target as LngLatBoundsLike, { padding: 72, duration: 1100, essential: true, maxZoom: detected.level === 'state' ? 5.5 : 8 });
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
            region: { key: String(p.__key), label: String(p.__label), value: Number(p.__value), rank: Number(p.__rank) },
            x: e.point.x, y: e.point.y,
          });
        });
        map.on('mouseleave', 'data-fill', clearHover);
        map.on('click', 'data-fill', (e) => {
          const f = e.features?.[0] as MapGeoJSONFeature | undefined;
          if (!f) return;
          const p = f.properties as Record<string, unknown>;
          setPinned({ key: String(p.__key), label: String(p.__label), value: Number(p.__value), rank: Number(p.__rank) });
        });

        setReady(true);
      } catch (err) {
        if (!disposed) setFailed(err instanceof Error ? err.message : 'Map failed to load.');
      }
    };
    map.on('load', () => void load());
    const onResize = () => map.resize();
    window.addEventListener('resize', onResize);
    return () => {
      disposed = true;
      window.removeEventListener('resize', onResize);
      map.remove();
      mapRef.current = null;
      featureBoundsRef.current.clear();
    };
  }, [isOpen, detected, breaks, regionList, mapIntent.mapType, mapIntent.state]);

  const flyToRegion = (region: Region) => {
    setPinned(region);
    const map = mapRef.current;
    if (!map) return;
    const bounds = featureBoundsRef.current.get(region.key);
    if (bounds) map.fitBounds(bounds as LngLatBoundsLike, { padding: 160, duration: 850, maxZoom: 8.5 });
  };

  if (!isOpen) return null;

  const top3 = regionList.slice(0, 3);
  const values = regionList.map((region) => region.value);
  const minV = values.length ? Math.min(...values) : 0;
  const maxV = values.length ? Math.max(...values) : 0;
  const total = regionList.reduce((s, r) => s + r.value, 0);
  const diverging = minV < 0 && maxV > 0;
  const legendRamp = diverging ? DIVERGING_RAMP : RAMP;

  return (
    <div className="fixed inset-0 z-[120] bg-[#1f1e1d]/25 backdrop-blur-[2px]">
      <div className="absolute inset-0" onClick={onClose} />
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="absolute inset-3 flex flex-col overflow-hidden rounded-2xl bg-[var(--surface)] shadow-[0_24px_64px_rgba(31,30,29,0.18)] sm:inset-6"
      >
        {/* Header */}
        <header className="flex items-center justify-between gap-4 px-5 py-4 sm:px-6">
          <div className="min-w-0">
            <h2 id={titleId} className="truncate font-display text-[20px] font-medium text-[var(--ink)] sm:text-[23px]">
              {mapIntent.title ?? `${metricLabel} by ${detected?.level === 'county' ? 'county' : detected?.level === 'congress' ? 'district' : 'state'}`}
            </h2>
            {mapIntent.subtitle && <p className="truncate text-[12px] text-[var(--muted)]">{mapIntent.subtitle}</p>}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close map"
            autoFocus
            className="rounded-lg p-2 text-[var(--muted)] transition hover:bg-[var(--surface-2)] hover:text-[var(--ink)]"
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

              {/* Cursor tooltip */}
              {hover && (
                <div
                  className="pointer-events-none absolute z-10 -translate-y-full rounded-xl border border-[var(--line)] bg-[var(--surface)] px-3 py-2 shadow-lg"
                  style={{ left: hover.x + 12, top: hover.y - 10 }}
                >
                  <div className="text-[12.5px] font-semibold text-[var(--ink)]">{hover.region.label}</div>
                  <div className="tabular-nums mt-0.5 flex items-baseline gap-2 text-[13px]">
                    <span className="font-semibold text-[var(--accent)]">{fmtValue(hover.region.value, unit)}</span>
                    <span className="text-[10.5px] text-[var(--muted)]">#{hover.region.rank} {detected.rankDirection} of {regionList.length}</span>
                  </div>
                </div>
              )}

              {/* Top-3 quick-jump chips */}
              {top3.length > 1 && (
                <div className="absolute left-4 top-4 z-10 flex flex-col gap-1.5">
                  {top3.map((r) => (
                    <button
                      key={r.key}
                      type="button"
                      onClick={() => flyToRegion(r)}
                      className={`group flex items-center gap-2 rounded-full border px-3 py-1.5 text-left shadow-sm transition ${
                        pinned?.key === r.key
                          ? 'border-[var(--accent)] bg-[var(--surface)]'
                          : 'border-[var(--line-soft)] bg-[var(--surface)]/92 hover:border-[var(--muted-2)]'
                      }`}
                    >
                      <span className="grid h-5 w-5 shrink-0 place-items-center rounded-full bg-[var(--accent-soft)] text-[10px] font-bold text-[var(--accent)]">
                        {r.rank}
                      </span>
                      <span className="max-w-44 truncate text-[12px] font-medium text-[var(--ink)]">{r.label}</span>
                      <span className="tabular-nums text-[11.5px] font-semibold text-[var(--muted)]">{fmtValue(r.value, unit)}</span>
                    </button>
                  ))}
                </div>
              )}

              {/* Pinned detail card */}
              {pinned && (
                <div className="absolute bottom-14 left-4 z-10 w-64 rounded-2xl border border-[var(--line-soft)] bg-[var(--surface)] p-4 shadow-xl">
                  <div className="flex items-start justify-between gap-2">
                    <div className="text-[14px] font-semibold leading-5 text-[var(--ink)]">{pinned.label}</div>
                    <button type="button" onClick={() => setPinned(null)} aria-label="Clear selection" className="rounded-md p-0.5 text-[var(--muted-2)] hover:text-[var(--ink)]">
                      <X size={13} />
                    </button>
                  </div>
                  <div className="tabular-nums mt-2 font-display text-[26px] font-semibold leading-none text-[var(--accent)]">
                    {fmtValue(pinned.value, unit)}
                  </div>
                  <div className="mt-2 space-y-1 text-[11.5px] leading-4 text-[var(--muted)]">
                    <div>Rank <span className="font-semibold text-[var(--ink)]">#{pinned.rank}</span> {detected.rankDirection} of {regionList.length} shown</div>
                    {unit === 'usd' && minV >= 0 && total > 0 && pinned.value > 0 && (
                      <div><span className="font-semibold text-[var(--ink)]">{((pinned.value / total) * 100).toFixed(1)}%</span> of the mapped total</div>
                    )}
                  </div>
                </div>
              )}

              {/* Legend */}
              {regionList.length > 1 && (
                <div className="absolute bottom-4 left-1/2 z-10 flex -translate-x-1/2 items-center gap-2.5 rounded-full border border-[var(--line-soft)] bg-[var(--surface)]/94 px-4 py-2 shadow-sm">
                  <span className="tabular-nums text-[10.5px] font-medium text-[var(--muted)]">{fmtValue(minV, unit)}</span>
                  <span className="flex overflow-hidden rounded-full">
                    {legendRamp.map((c) => (
                      <span key={c} className="block h-2 w-7" style={{ backgroundColor: c }} />
                    ))}
                  </span>
                  <span className="tabular-nums text-[10.5px] font-medium text-[var(--muted)]">{fmtValue(maxV, unit)}</span>
                  <span className="hidden text-[9.5px] text-[var(--muted-2)] sm:inline">{diverging ? 'Centered at zero' : 'Quantile scale'}</span>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
