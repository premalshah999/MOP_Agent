import type { ApiAskResponse } from '@/types/chat';

const MAX_ENTRIES = 100;
const TTL_MS = 30 * 60 * 1000; // 30 minutes

interface CacheEntry {
  response: ApiAskResponse;
  ts: number;
}

const store = new Map<string, CacheEntry>();

function key(question: string): string {
  return question.trim().toLowerCase().replace(/\s+/g, ' ');
}

export function getCached(question: string): ApiAskResponse | null {
  const entry = store.get(key(question));
  if (!entry) return null;
  if (Date.now() - entry.ts > TTL_MS) {
    store.delete(key(question));
    return null;
  }
  return entry.response;
}

export function setCache(question: string, response: ApiAskResponse): void {
  if (response.error) return; // don't cache errors
  if (store.size >= MAX_ENTRIES) {
    // evict oldest
    const oldest = [...store.entries()].sort((a, b) => a[1].ts - b[1].ts)[0];
    if (oldest) store.delete(oldest[0]);
  }
  store.set(key(question), { response, ts: Date.now() });
}

export function clearCache(): void {
  store.clear();
}
