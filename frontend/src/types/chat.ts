export type ChatRole = 'user' | 'assistant';

export interface HistoryMessage {
  role: ChatRole;
  content: string;
}

export interface ApiAskRequest {
  question: string;
  thread_id?: string;
  history?: HistoryMessage[];
}

export interface ApiAskResponse {
  answer?: string;
  sql?: string | null;
  data?: Record<string, unknown>[];
  row_count?: number;
  error?: string;
  chart?: Record<string, unknown>;
  thread_id?: string;
  user_message_id?: string;
  assistant_message_id?: string;
  request_id?: string;
}

export interface HealthSummary {
  status: string;
  service?: string;
  version?: string;
  checks?: {
    manifest_present?: boolean;
    registered_table_count?: number;
    frontend_built?: boolean;
  };
}

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  ts: string;
  sqlQuery?: string | null;
  data?: Record<string, unknown>[];
  rowCount?: number;
  chart?: Record<string, unknown>;
  error?: string | null;
}

export interface ChatThread {
  id: string;
  title: string;
  datasetId: string;
  createdAt?: string;
  updatedAt: string;
  messages: ChatMessage[];
}

export interface UserProfile {
  id: number;
  name: string;
  email: string;
}

export interface AuthResponse {
  token: string;
  user: UserProfile;
}
