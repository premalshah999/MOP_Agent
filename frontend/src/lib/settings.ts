import { useSyncExternalStore } from 'react';

/* Client-side preferences. Stored in localStorage, broadcast via a tiny
   subscriber list so every component re-renders on change. */

export interface AppSettings {
  /** Answer text size */
  textSize: 'compact' | 'default' | 'large';
  /** Wrap known terms (ACS, FY, …) with hover definitions */
  glossaryTooltips: boolean;
  /** Show suggested follow-up chips under answers */
  showFollowups: boolean;
  /** Start new questions in Extended (reasoning) mode */
  extendedByDefault: boolean;
}

const KEY = 'mop-settings-v1';

const DEFAULTS: AppSettings = {
  textSize: 'default',
  glossaryTooltips: true,
  showFollowups: true,
  extendedByDefault: false,
};

let cached: AppSettings = load();
const listeners = new Set<() => void>();

function load(): AppSettings {
  if (typeof window === 'undefined') return DEFAULTS;
  try {
    const raw = window.localStorage.getItem(KEY);
    if (!raw) return DEFAULTS;
    return { ...DEFAULTS, ...(JSON.parse(raw) as Partial<AppSettings>) };
  } catch {
    return DEFAULTS;
  }
}

export function getSettings(): AppSettings {
  return cached;
}

export function updateSettings(patch: Partial<AppSettings>): void {
  cached = { ...cached, ...patch };
  try {
    window.localStorage.setItem(KEY, JSON.stringify(cached));
  } catch { /* private mode */ }
  listeners.forEach((fn) => fn());
}

function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function useSettings(): AppSettings {
  return useSyncExternalStore(subscribe, getSettings, () => DEFAULTS);
}

export const TEXT_SIZE_PX: Record<AppSettings['textSize'], string> = {
  compact: '14px',
  default: '15.5px',
  large: '17px',
};
