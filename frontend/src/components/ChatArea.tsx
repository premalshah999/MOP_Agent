import { Loader2, PanelLeft, SendHorizontal } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { askAgent, getHealthSummary } from '@/lib/api';
import type { DatasetGuide } from '@/lib/content';
import type { ChatMessage, ChatThread } from '@/types/chat';
import { DetailPanel } from './DetailPanel';
import { Message } from './Message';

type DetailState = { messageId: string; tab: 'sql' | 'data' } | null;

function makeId() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

interface ChatAreaProps {
  datasets: DatasetGuide[];
  selectedDataset: DatasetGuide;
  selectedDatasetId: string;
  thread?: ChatThread;
  onOpenSidebar?: () => void;
  onMessagesChange: (threadId: string, messages: ChatMessage[]) => void;
  onUpdateTitle: (threadId: string, title: string) => void;
  onSelectDataset: (id: string) => void;
  onEnsureThread: () => Promise<string | null>;
}

export function ChatArea({
  datasets,
  selectedDataset,
  selectedDatasetId,
  thread,
  onOpenSidebar,
  onMessagesChange,
  onUpdateTitle,
  onSelectDataset,
  onEnsureThread,
}: ChatAreaProps) {
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [healthStatus, setHealthStatus] = useState<'checking' | 'offline' | 'ok'>('checking');
  const [detail, setDetail] = useState<DetailState>(null);
  const endRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const messages = thread?.messages ?? [];

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [messages, isLoading]);

  useEffect(() => { setInput(''); setDetail(null); }, [thread?.id]);

  // Health polling
  useEffect(() => {
    let active = true;
    const check = async () => {
      try {
        const s = await getHealthSummary();
        if (active) setHealthStatus(s.status === 'ok' ? 'ok' : 'offline');
      } catch {
        if (active) setHealthStatus('offline');
      }
    };
    void check();
    const id = setInterval(() => void check(), 15_000);
    return () => { active = false; clearInterval(id); };
  }, []);

  const send = async () => {
    const q = input.trim();
    if (!q || isLoading) return;

    // Ensure we have a thread (creates one on server if needed)
    let threadId = thread?.id ?? null;
    if (!threadId) {
      threadId = await onEnsureThread();
      if (!threadId) return; // failed to create thread
    }

    const userMsg: ChatMessage = { id: makeId(), role: 'user', content: q, ts: new Date().toISOString() };
    const next = [...messages, userMsg];
    onMessagesChange(threadId, next);
    setInput('');
    setIsLoading(true);

    try {
      const response = await askAgent({ question: q, thread_id: threadId });

      // Use server-provided IDs if available
      const assistantId = response.assistant_message_id || makeId();
      const hasSQL = Boolean(response.sql);
      const hasData = Array.isArray(response.data) && response.data.length > 0;

      // Update user message ID with server-provided one
      const updatedNext = next.map((m) =>
        m.id === userMsg.id && response.user_message_id
          ? { ...m, id: response.user_message_id }
          : m,
      );

      const assistantMsg: ChatMessage = {
        id: assistantId,
        role: 'assistant',
        ts: new Date().toISOString(),
        content: response.answer || 'No answer returned.',
        sqlQuery: response.sql ?? undefined,
        data: response.data ?? [],
        rowCount: response.row_count ?? 0,
        chart: response.chart ?? undefined,
        error: response.error ?? undefined,
        mapIntent: response.mapIntent ?? undefined,
      };

      onMessagesChange(threadId, [...updatedNext, assistantMsg]);

      // Update thread title from first user message
      if (messages.length === 0 && threadId) {
        const title = q.length > 60 ? q.slice(0, 60).trim() + '...' : q;
        onUpdateTitle(threadId, title);
      }

      // Auto-open detail panel for new responses with SQL or data
      if (hasData) {
        setDetail({ messageId: assistantId, tab: 'data' });
      } else if (hasSQL) {
        setDetail({ messageId: assistantId, tab: 'sql' });
      }
    } catch (err) {
      console.error('[MOP] Query failed:', err);
      const raw = err instanceof Error ? err.message : 'Request failed';
      const msg = raw.startsWith('Failed to fetch')
        ? 'Cannot reach the server. Check your connection and try again.'
        : raw;
      onMessagesChange(threadId, [
        ...next,
        { id: makeId(), role: 'assistant', ts: new Date().toISOString(), content: msg, error: msg },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const statusColor = healthStatus === 'ok' ? 'bg-emerald-500' : healthStatus === 'offline' ? 'bg-red-500' : 'bg-amber-500';

  const detailMsg = detail ? messages.find((m) => m.id === detail.messageId) : undefined;

  return (
    <div className="flex h-full flex-1 overflow-hidden">
      {/* Chat column */}
      <main className="flex h-full min-w-0 flex-1 flex-col overflow-hidden bg-[var(--bg)]">
        {/* Header */}
        <header className="shrink-0 border-b border-black/5 bg-[var(--bg)]/95 backdrop-blur">
          <div className="mx-auto flex max-w-4xl items-center justify-between gap-3 px-5 py-2.5">
            <div className="flex min-w-0 items-center gap-2.5">
              {onOpenSidebar && (
                <button type="button" onClick={onOpenSidebar} aria-label="Open sidebar" className="p-1.5 text-[var(--muted)] hover:text-[var(--ink)] lg:hidden">
                  <PanelLeft size={16} />
                </button>
              )}
              <span className="truncate text-[13px] font-medium text-[var(--ink)]">
                {messages.length ? thread?.title : 'New chat'}
              </span>
            </div>
            <div className="flex items-center gap-1.5 text-[10px] text-[var(--muted)]">
                <span className={`h-1.5 w-1.5 rounded-full ${statusColor}`} />
                {healthStatus === 'ok' ? 'Connected' : healthStatus === 'offline' ? 'Offline' : '...'}
            </div>
          </div>
        </header>

        {messages.length === 0 ? (
          /* Empty state */
          <div className="flex flex-1 items-center justify-center overflow-y-auto px-4">
            <div className="w-full max-w-6xl py-8">
              <h1 className="text-center font-display text-3xl font-semibold tracking-tight text-[var(--ink)] sm:text-4xl">
                What would you like to know?
              </h1>
              <p className="mx-auto mt-3 max-w-md text-center text-[13px] leading-6 text-[var(--muted)]">
                Ask about government finances, demographics, federal spending, financial literacy, or fund flows across U.S. geographies.
              </p>

              {/* Dataset selector */}
              <div className="mx-auto mt-8 flex max-w-2xl flex-wrap justify-center gap-1.5">
                {datasets.map((ds) => (
                  <button
                    key={ds.id}
                    type="button"
                    onClick={() => onSelectDataset(ds.id)}
                    className={`px-2.5 py-1 text-[11px] font-medium transition ${
                      ds.id === selectedDatasetId
                        ? 'bg-[var(--ink)] text-white'
                        : 'bg-[var(--surface)] text-[var(--muted)] border border-[var(--line)] hover:text-[var(--ink)]'
                    }`}
                  >
                    {ds.shortLabel}
                  </button>
                ))}
              </div>

              {/* Composer */}
              <div className="mx-auto mt-6 max-w-2xl">
                <Composer
                  input={input}
                  isLoading={isLoading}
                  placeholder={`Ask about ${selectedDataset.name.toLowerCase()}...`}
                  textareaRef={textareaRef}
                  onChangeInput={setInput}
                  onSend={() => void send()}
                />
              </div>
            </div>
          </div>
        ) : (
          /* Chat view */
          <>
            <div className="flex-1 overflow-y-auto px-4 pb-10 pt-6">
              <div className="mx-auto w-full max-w-4xl space-y-5">
                {messages.map((msg) => (
                  <Message
                    key={msg.id}
                    {...msg}
                    datasetId={thread?.datasetId ?? selectedDatasetId}
                    activeDetailTab={detail?.messageId === msg.id ? detail.tab : null}
                    onOpenDetail={(tab) =>
                      setDetail((prev) =>
                        prev?.messageId === msg.id && prev.tab === tab ? null : { messageId: msg.id, tab },
                      )
                    }
                  />
                ))}
                {isLoading && (
                  <div className="flex items-center gap-2 px-1 text-[12px] text-[var(--muted)]">
                    <Loader2 size={13} className="animate-spin" />
                    Analyzing...
                  </div>
                )}
                <div ref={endRef} />
              </div>
            </div>

            {/* Bottom composer */}
            <div className="shrink-0 border-t border-black/5 bg-[var(--surface)]/96 backdrop-blur">
              <div className="mx-auto max-w-4xl px-4 py-2.5">
                <Composer
                  input={input}
                  isLoading={isLoading}
                  placeholder="Follow up..."
                  textareaRef={textareaRef}
                  onChangeInput={setInput}
                  onSend={() => void send()}
                  compact
                />
              </div>
            </div>
          </>
        )}
      </main>

      {/* Right detail panel */}
      {detail && detailMsg && (
        <div className="relative shrink-0">
          <DetailPanel
            tab={detail.tab}
            sql={detailMsg.sqlQuery}
            data={detailMsg.data}
            rowCount={detailMsg.rowCount}
            onChangeTab={(tab) => setDetail({ ...detail, tab })}
            onClose={() => setDetail(null)}
          />
        </div>
      )}
    </div>
  );
}

/* ── Composer ── */
interface ComposerProps {
  input: string;
  isLoading: boolean;
  placeholder: string;
  textareaRef: React.RefObject<HTMLTextAreaElement | null>;
  onChangeInput: (v: string) => void;
  onSend: () => void;
  compact?: boolean;
}

function Composer({ input, isLoading, placeholder, textareaRef, onChangeInput, onSend, compact }: ComposerProps) {
  return (
    <form
      onSubmit={(e) => { e.preventDefault(); onSend(); }}
      className={`flex items-end gap-2 border border-black/6 bg-[var(--surface)] px-3 ${compact ? 'py-1.5' : 'py-2.5'}`}
    >
      <textarea
        ref={textareaRef}
        value={input}
        onChange={(e) => onChangeInput(e.target.value)}
        onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(); } }}
        placeholder={placeholder}
        disabled={isLoading}
        rows={1}
        className={`flex-1 resize-none border-0 bg-transparent text-[14px] leading-6 text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] ${compact ? 'min-h-[32px] py-1' : 'min-h-[44px] py-2'}`}
      />
      <button
        type="submit"
        disabled={!input.trim() || isLoading}
        aria-label={isLoading ? 'Sending...' : 'Send message'}
        className="shrink-0 p-2 text-[var(--muted)] transition hover:text-[var(--ink)] disabled:opacity-30"
      >
        {isLoading ? <Loader2 size={16} className="animate-spin" /> : <SendHorizontal size={16} />}
      </button>
    </form>
  );
}
