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
  mapIntent?: ChatbotMapIntent | null;
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
  mapIntent?: ChatbotMapIntent | null;
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

export type ChatbotMapType =
  | 'atlas-single-metric'
  | 'atlas-comparison'
  | 'atlas-within-state'
  | 'single-state-spotlight'
  | 'single-state-ranked-subregions'
  | 'single-state-agency'
  | 'agency-choropleth'
  | 'top-n-highlight'
  | 'spending-breakdown'
  | 'flow-map'
  | 'flow-state-focused'
  | 'flow-pair'
  | 'flow-within-state'
  | 'none';

export interface ChatbotMapIntent {
  enabled: boolean;
  mapType: ChatbotMapType;
  defaultView?: 'heat' | 'comparison' | 'state-zoom' | 'subdivision' | 'subdivision-zoom';
  buttonLabel?: string;
  dataset?: 'census' | 'gov_spending' | 'finra' | 'contract_static' | 'contract_agency' | 'spending_breakdown';
  level?: 'state' | 'county' | 'congress';
  year?: string;
  metric?: string;
  agency?: string;
  state?: string;
  focusIds?: string[];
  comparisonIds?: string[];
  comparisonLabels?: string[];
  topN?: number | null;
  title?: string;
  subtitle?: string;
  reason?: string;
  showLegend?: boolean;
}

export interface DatasetTableDownload {
  parquet?: string;
  xlsx?: string;
}

export interface DatasetTableCatalogEntry {
  tableName: string;
  label: string;
  grain: string;
  summary: string;
  rows: number;
  columns: string[];
  sourceFile?: string;
  runtimePath?: string;
  downloads: DatasetTableDownload;
}

export interface DatasetCatalogEntry {
  id: string;
  name: string;
  description: string;
  helper: string;
  notes?: string[];
  tables: DatasetTableCatalogEntry[];
}
