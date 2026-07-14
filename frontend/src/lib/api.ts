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
  chart?: Record<string, unknown>;
  charts?: import('@/types/chat').ChartBlock[];
  evidence?: import('@/types/chat').EvidenceBlock;
  resolution?: import('@/types/chat').ApiAskResponse['resolution'];
  mapIntent?: import('@/types/chat').ChatbotMapIntent | null;
  resultPackage?: import('@/types/chat').ResultPackage;
  contract?: import('@/types/chat').QueryContract;
  pipelineTrace?: import('@/types/chat').PipelineTrace;
  quality?: import('@/types/chat').PipelineQuality;
  error?: string;
  keyNumbers?: import('@/types/chat').KeyNumber[];
  caveats?: string[];
  confidence?: string;
  glossary?: Record<string, string>;
  verifiedQuery?: { id: string; score?: number } | null;
  suggestedFollowups?: string[];
}

export async function apiUpdateProfile(updates: { name: string }): Promise<void> {
  const res = await fetch(buildApiUrl('/api/auth/me'), {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(updates),
  });
  if (!res.ok) {
    handle401(res);
    throw new Error(await parseErrorBody(res));
  }
  // Server re-issues the token with the new name embedded.
  const data = (await res.json()) as { token?: string };
  if (data.token) setToken(data.token);
}

export const apiDeleteAllThreads = apiClearAllThreads;

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
  mode?: 'normal' | 'reasoning';
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

// --- Share thread (read-only public link via token) ---
export async function createShareToken(threadId: string): Promise<{ token: string; thread_id: string }> {
  const res = await fetch(buildApiUrl(`/api/threads/${threadId}/share`), {
    method: 'POST', headers: authHeaders(),
  });
  if (res.status === 401) { clearToken(); window.location.reload(); throw new Error('Session expired'); }
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as { token: string; thread_id: string };
}

export interface SharedThreadPayload {
  thread: { id: string; title: string; datasetId: string; createdAt?: string; updatedAt?: string };
  messages: Record<string, unknown>[];
}

export async function getSharedThread(token: string): Promise<SharedThreadPayload> {
  const res = await fetch(buildApiUrl(`/api/share/${token}`));
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as SharedThreadPayload;
}


// --- Admin (gated by ADMIN_EMAILS env on the server) ---
async function adminGet<T>(path: string): Promise<T> {
  const res = await fetch(buildApiUrl(path), { headers: authHeaders() });
  if (res.status === 401) { clearToken(); window.location.reload(); throw new Error('Session expired'); }
  if (!res.ok) throw new Error(await parseErrorBody(res));
  return (await res.json()) as T;
}

export interface AdminUsage {
  total_questions: number;
  by_resolution: Record<string, number>;
  by_intent: Record<string, number>;
  unique_users: number;
  top_users: [string, number][];
  recent_failures: { timestamp?: string; question?: string; resolution?: string; warnings?: string[]; user_id?: number }[];
  feedback: { total: number; up: number; down: number; recent_down: { timestamp?: string; user_email?: string; message_id?: string; note?: string | null }[] };
}

export interface AdminQuestion {
  timestamp?: string;
  user_id?: number;
  question?: string;
  intent?: string;
  resolution?: string;
  row_count?: number;
  confidence?: string;
  warnings?: string[];
  datasets?: string[];
}

export interface AdminFeedback {
  timestamp?: string;
  user_email?: string;
  verdict?: 'up' | 'down';
  note?: string | null;
  message_id?: string;
}

export const getAdminUsage = () => adminGet<AdminUsage>('/api/admin/usage');
export const getAdminQuestions = (limit = 50) => adminGet<{ items: AdminQuestion[] }>(`/api/admin/questions?limit=${limit}`);
export const getAdminFeedback = (limit = 50) => adminGet<{ items: AdminFeedback[] }>(`/api/admin/feedback?limit=${limit}`);

// --- Per-message feedback ---
export async function sendFeedback(payload: {
  message_id: string;
  thread_id?: string;
  verdict: 'up' | 'down';
  note?: string;
}): Promise<void> {
  const res = await fetch(buildApiUrl('/api/feedback'), {
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
}


// --- Streaming (Server-Sent Events) variant of askAgent ---
// EventSource only supports GET so we POST + read the body as a stream
// and parse the `event:`/`data:` blocks manually. Each event triggers
// onEvent(name, payload); on the terminal `done`/`error` event we resolve.

export interface StreamEvent {
  name: string;
  payload: Record<string, unknown>;
}

export async function askAgentStream(
  payload: AskPayload,
  onEvent: (e: StreamEvent) => void,
): Promise<ApiAskResponse> {
  // Single safe retry: only retry if NO events arrived (pure connection
  // failure). Mid-stream failures throw cleanly so we don't risk duplicate
  // assistant messages from a partially-completed request.
  for (let attempt = 0; attempt < 2; attempt++) {
    let eventsSeen = 0;
    try {
      return await _streamOnce(payload, (e) => { eventsSeen += 1; onEvent(e); });
    } catch (err) {
      if (attempt === 0 && eventsSeen === 0 && err instanceof Error && /stream|network|fetch|connection/i.test(err.message)) {
        await new Promise((r) => setTimeout(r, 600));
        continue;
      }
      throw err;
    }
  }
  throw new Error('Stream failed after retry');
}

async function _streamOnce(
  payload: AskPayload,
  onEvent: (e: StreamEvent) => void,
): Promise<ApiAskResponse> {
  const res = await fetch(buildApiUrl('/api/ask/stream'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'text/event-stream', ...authHeaders() },
    body: JSON.stringify(payload),
  });
  if (res.status === 401) {
    clearToken();
    window.location.reload();
    throw new Error('Session expired. Please sign in again.');
  }
  if (!res.ok || !res.body) {
    throw new Error(await parseErrorBody(res));
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let final: ApiAskResponse | null = null;
  let errorMsg: string | null = null;

  // Parse one SSE block (event: ...\ndata: ...\n\n) at a time.
  const handleBlock = (block: string) => {
    let name = 'message';
    const dataLines: string[] = [];
    for (const line of block.split('\n')) {
      if (line.startsWith('event:')) name = line.slice(6).trim();
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
    }
    if (!dataLines.length) return;
    let payload: Record<string, unknown> = {};
    try {
      payload = JSON.parse(dataLines.join('\n'));
    } catch {
      payload = { raw: dataLines.join('\n') };
    }
    if (name === 'done') {
      final = payload as unknown as ApiAskResponse;
      return;
    }
    if (name === 'error') {
      errorMsg = typeof payload.detail === 'string' ? payload.detail : 'stream error';
      return;
    }
    onEvent({ name, payload });
  };

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let sep = buffer.indexOf('\n\n');
    while (sep !== -1) {
      const block = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      if (block.trim()) handleBlock(block);
      sep = buffer.indexOf('\n\n');
    }
  }
  if (errorMsg) throw new Error(errorMsg);
  if (!final) throw new Error('Stream ended without a final answer');
  return final;
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
