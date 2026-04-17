import type {
  ApiAskRequest,
  ApiAskResponse,
  AuthResponse,
  ChatThread,
  DatasetCatalogEntry,
  HealthSummary,
  HistoryMessage,
} from '@/types/chat';

const API_BASE = (import.meta.env.VITE_API_BASE_URL || '').replace(/\/$/, '');
const TOKEN_KEY = 'mop-token';

export function buildApiUrl(path: string): string {
  return API_BASE ? `${API_BASE}${path}` : path;
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

function authHeaders(): Record<string, string> {
  const token = getToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function parseErrorBody(res: Response): Promise<string> {
  try {
    const body = await res.json();
    if (typeof body?.detail === 'string') return body.detail;
    if (typeof body?.error === 'string') return body.error;
    if (typeof body?.message === 'string') return body.message;
  } catch { /* ignore parse errors */ }
  return `Server error (${res.status})`;
}

function handle401(res: Response): void {
  if (res.status === 401) {
    clearToken();
    window.location.reload();
  }
}

// ── Auth API ──

export async function apiRegister(name: string, email: string, password: string): Promise<AuthResponse> {
  const res = await fetch(buildApiUrl('/api/auth/register'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, email, password }),
  });
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as AuthResponse;
}

export async function apiLogin(email: string, password: string): Promise<AuthResponse> {
  const res = await fetch(buildApiUrl('/api/auth/login'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as AuthResponse;
}

export async function apiGetMe(): Promise<{ user: { id: number; name: string; email: string } }> {
  const res = await fetch(buildApiUrl('/api/auth/me'), {
    headers: authHeaders(),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  return (await res.json()) as { user: { id: number; name: string; email: string } };
}

// ── Thread API ──

export interface ApiThread {
  id: string;
  title: string;
  datasetId: string;
  createdAt: string;
  updatedAt: string;
  messages?: ApiMessage[];
}

export interface ApiMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  ts: string;
  sqlQuery?: string;
  data?: Record<string, unknown>[];
  rowCount?: number;
  error?: string;
}

export async function apiListThreads(): Promise<ApiThread[]> {
  const res = await fetch(buildApiUrl('/api/threads'), {
    headers: authHeaders(),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  const body = await res.json();
  return body.threads as ApiThread[];
}

export async function apiCreateThread(datasetId: string, title?: string): Promise<ApiThread> {
  const res = await fetch(buildApiUrl('/api/threads'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify({ dataset_id: datasetId, title: title || 'New thread' }),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  const body = await res.json();
  return body.thread as ApiThread;
}

export async function apiGetThread(threadId: string): Promise<ApiThread> {
  const res = await fetch(buildApiUrl(`/api/threads/${threadId}`), {
    headers: authHeaders(),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  const body = await res.json();
  return body.thread as ApiThread;
}

export async function apiUpdateThread(threadId: string, updates: { title?: string; dataset_id?: string }): Promise<ApiThread> {
  const res = await fetch(buildApiUrl(`/api/threads/${threadId}`), {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(updates),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  const body = await res.json();
  return body.thread as ApiThread;
}

export async function apiDeleteThread(threadId: string): Promise<void> {
  const res = await fetch(buildApiUrl(`/api/threads/${threadId}`), {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
}

export async function apiClearAllThreads(): Promise<void> {
  const res = await fetch(buildApiUrl('/api/threads'), {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
}

export async function apiGetMessages(threadId: string): Promise<ApiMessage[]> {
  const res = await fetch(buildApiUrl(`/api/threads/${threadId}/messages`), {
    headers: authHeaders(),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  const body = await res.json();
  return body.messages as ApiMessage[];
}

// ── Data API ──

export async function getHealthSummary(): Promise<HealthSummary> {
  const res = await fetch(buildApiUrl('/health'));
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as HealthSummary;
}

export async function checkHealth(): Promise<boolean> {
  const body = await getHealthSummary();
  return body.status === 'ok';
}

export interface AskPayload {
  question: string;
  thread_id?: string;
  history?: HistoryMessage[];
}

export async function askAgent(payload: AskPayload): Promise<ApiAskResponse> {
  const res = await fetch(buildApiUrl('/api/ask'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(payload),
  });
  if (res.status === 401) {
    clearToken();
    window.location.reload();
    throw new Error('Session expired. Please sign in again.');
  }
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as ApiAskResponse;
}

export function buildHistory(messages: { role: string; content: string }[]): HistoryMessage[] {
  return messages
    .filter((m) => m.role === 'user' || m.role === 'assistant')
    .map((m) => ({ role: m.role as HistoryMessage['role'], content: m.content }));
}

export async function getDatasetCatalog(): Promise<DatasetCatalogEntry[]> {
  const res = await fetch(buildApiUrl('/api/datasets'));
  if (!res.ok) throw new Error(await parseErrorBody(res));
  const body = await res.json();
  return (body.datasets ?? []) as DatasetCatalogEntry[];
}

export interface MapValuesParams {
  dataset: string;
  level: string;
  variable: string;
  year?: string;
  state?: string;
  agency?: string;
}

export async function getMapValues(params: MapValuesParams): Promise<Record<string, unknown>[]> {
  const search = new URLSearchParams();
  search.set('dataset', params.dataset);
  search.set('level', params.level);
  search.set('variable', params.variable);
  if (params.year) search.set('year', params.year);
  if (params.state) search.set('state', params.state);
  if (params.agency) search.set('agency', params.agency);

  const res = await fetch(buildApiUrl(`/api/values?${search.toString()}`));
  if (!res.ok) throw new Error(await parseErrorBody(res));
  const body = await res.json();
  return (body.rows ?? []) as Record<string, unknown>[];
}
