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
  charts?: ChartBlock[];
  evidence?: EvidenceBlock;
  resolution?: 'answered' | 'answered_with_assumptions' | 'partially_answered' | 'needs_clarification' | 'unsupported';
  mapIntent?: ChatbotMapIntent | null;
  resultPackage?: ResultPackage;
  contract?: QueryContract;
  pipelineTrace?: PipelineTrace;
  quality?: PipelineQuality;
  thread_id?: string;
  user_message_id?: string;
  assistant_message_id?: string;
  request_id?: string;
}

export interface QueryContract {
  contract_type?: string | null;
  family?: string | null;
  metric?: string | null;
  unit?: string | null;
  geography_level?: string | null;
  year?: string | number | null;
  focus_state?: string | null;
  focus_entity?: string | null;
  compare_entities?: string[];
  sort_direction?: string | null;
  top_k?: number | null;
  tables?: string[];
  supported?: boolean;
  missing_slots?: string[];
  assumptions?: string[];
  validation_message?: string | null;
}

export interface PipelineStage {
  name: string;
  status: string;
  detail?: string;
  data?: Record<string, unknown>;
}

export interface PipelineTrace {
  version: string;
  stages: PipelineStage[];
  prompt_profile?: Record<string, unknown>;
  visual_decision?: Record<string, unknown>;
}

export interface PipelineQuality {
  status: 'ok' | 'warning';
  warnings: string[];
}

export interface ResultPackage {
  status?: string;
  contract_type?: string | null;
  family?: string | null;
  metric?: string | null;
  unit?: string | null;
  scope?: Record<string, unknown>;
  assumptions?: string[];
  sql?: string | null;
  rows?: Record<string, unknown>[];
  statistics?: Record<string, unknown>;
  ranking_context?: Record<string, unknown> | null;
  methodology_notes?: string[];
  map_intent?: ChatbotMapIntent | null;
  chart_intent?: Record<string, unknown>;
  alternatives?: Record<string, unknown>[];
  contract?: QueryContract;
  pipeline?: PipelineTrace;
  quality?: PipelineQuality;
}

export interface HealthSummary {
  status: string;
  service?: string;
  version?: string;
  checks?: {
    manifest_present?: boolean;
    registered_table_count?: number;
    frontend_built?: boolean;
    pipeline_ready?: boolean;
  };
  pipeline?: {
    status?: string;
    version?: string;
    required_table_count?: number;
    manifest_table_count?: number;
    registered_table_count?: number | null;
    missing_required_manifest_tables?: string[];
    missing_required_registered_tables?: string[];
    documented_not_loaded_tables?: string[];
    checks?: Record<string, boolean>;
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
  charts?: ChartBlock[];
  evidence?: EvidenceBlock;
  resolution?: 'answered' | 'answered_with_assumptions' | 'partially_answered' | 'needs_clarification' | 'unsupported';
  error?: string | null;
  mapIntent?: ChatbotMapIntent | null;
  resultPackage?: ResultPackage;
  contract?: QueryContract;
  pipelineTrace?: PipelineTrace;
  quality?: PipelineQuality;
}

export interface ChartBlock {
  title: string;
  subtitle?: string;
  spec: Record<string, unknown>;
}

export interface EvidenceCard {
  label: string;
  value: string;
  meta?: string;
}

export interface EvidenceSection {
  title: string;
  subtitle?: string;
  cards?: EvidenceCard[];
  items?: string[];
  rows?: EvidenceCard[];
}

export interface EvidenceBlock {
  cards?: EvidenceCard[];
  sections?: EvidenceSection[];
  note?: string;
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
  dataset?: 'census' | 'gov_spending' | 'finra' | 'contract_static' | 'contract_agency' | 'spending_breakdown' | 'fund_flow';
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
