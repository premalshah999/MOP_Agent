import { ArrowUp, Loader2, PanelLeft, Lightbulb, Share2, Check, Sparkle } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { askAgentStream, createShareToken, getHealthSummary, type StreamEvent } from '@/lib/api';
import { getSettings } from '@/lib/settings';

/* Quiet, human status words — never pipeline jargon. Only a handful of
   moments deserve a label change; everything else stays "Thinking". */
function describeStreamEvent(evt: StreamEvent): string | null {
  const p = evt.payload as Record<string, unknown>;
  if (evt.name === 'stage') {
    const name = String(p?.name ?? '');
    const map: Record<string, string> = {
      stage1_intent: 'Thinking',
      stage4_sql_generation: 'Querying the data',
      stage4_answer_generation: 'Writing',
      non_analytical_responder: 'Writing',
    };
    return map[name] ?? null;
  }
  if (evt.name === 'tool_start') {
    const tool = String(p?.name ?? '');
    if (tool === 'run_sql' || tool === 'peer_stats') return 'Querying the data';
    if (tool === 'get_schema' || tool === 'distinct_values') return 'Looking at the data';
    return null;
  }
  return null;
}
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
  datasets: _datasets,
  selectedDataset,
  selectedDatasetId,
  thread,
  onOpenSidebar,
  onMessagesChange,
  onUpdateTitle,
  onSelectDataset: _onSelectDataset,
  onEnsureThread,
}: ChatAreaProps) {
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  // The Settings preference is the default; the composer toggle is a
  // session-only override (persisting it here would shadow the setting).
  const [reasoning, setReasoning] = useState<boolean>(() => getSettings().extendedByDefault);

  // Live status word driven by streamed pipeline events (latest wins).
  const [statusWord, setStatusWord] = useState<string>('Thinking');
  // Share-link state
  const [shareUrl, setShareUrl] = useState<string | null>(null);
  const [shareCopied, setShareCopied] = useState(false);

  const handleShare = async () => {
    if (!thread?.id) return;
    try {
      const { token } = await createShareToken(thread.id);
      const url = `${window.location.origin}/share/${token}`;
      setShareUrl(url);
      try {
        await navigator.clipboard.writeText(url);
        setShareCopied(true);
        setTimeout(() => setShareCopied(false), 1500);
      } catch { /* clipboard may be blocked; URL still shown */ }
    } catch (e) {
      console.warn('[share]', e);
    }
  };
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

  const send = async (overrideText?: string, overrideMode?: 'normal' | 'reasoning') => {
    const q = (overrideText ?? input).trim();
    if (!q || isLoading) return;
    const effectiveMode = overrideMode ?? (reasoning ? 'reasoning' : 'normal');

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

    setStatusWord('Thinking');
    let previewMsg: ChatMessage | null = null;
    let workingNext = next;
    try {
      const response = await askAgentStream(
        { question: q, thread_id: threadId, mode: effectiveMode },
        (evt) => {
          // Render the answer the instant the backend emits it (faithfulness
          // and suggested follow-ups attach as subsequent events without
          // blocking the answer text/chart).
          if (evt.name === 'answer_preview') {
            const p = evt.payload as Record<string, unknown>;
            previewMsg = {
              id: makeId(),
              role: 'assistant',
              ts: new Date().toISOString(),
              content: (p.answer as string) || '',
              sqlQuery: (p.sql as string | null | undefined) ?? undefined,
              data: (p.data as Record<string, unknown>[]) ?? [],
              rowCount: (p.row_count as number) ?? 0,
              chart: (p.chart as Record<string, unknown> | undefined) ?? undefined,
              charts: (p.charts as ChatMessage['charts']) ?? undefined,
              mapIntent: (p.mapIntent as ChatMessage['mapIntent']) ?? undefined,
              resolution: (p.resolution as ChatMessage['resolution']) ?? undefined,
            };
            onMessagesChange(threadId!, [...workingNext, previewMsg]);
            return;
          }
          if (evt.name === 'suggested_followups' && previewMsg) {
            const items = ((evt.payload as Record<string, unknown>).items as string[]) || [];
            previewMsg = { ...previewMsg, suggestedFollowups: items };
            onMessagesChange(threadId!, [...workingNext, previewMsg]);
            return;
          }
          if (evt.name === 'faithfulness') {
            // Verdict attaches as part of the final envelope; nothing extra to render now.
            return;
          }
          const label = describeStreamEvent(evt);
          if (label) setStatusWord(label);
        },
      );

      // Reuse the preview id when it exists so React updates in place instead
      // of unmounting/remounting (avoids a flicker between preview and final).
      const assistantId = (previewMsg && (previewMsg as ChatMessage).id) || response.assistant_message_id || makeId();

      // Update user message ID with server-provided one
      const updatedNext = next.map((m) =>
        m.id === userMsg.id && response.user_message_id
          ? { ...m, id: response.user_message_id }
          : m,
      );

      const contractType = (response.contract as { contract_type?: string } | undefined)?.contract_type;
      const shouldSuggestReasoning =
        effectiveMode === 'normal' &&
        contractType === 'ANALYTICAL' &&
        response.resolution === 'answered' &&
        (response.row_count ?? 0) <= 1;

      const assistantMsg: ChatMessage = {
        id: assistantId,
        role: 'assistant',
        ts: new Date().toISOString(),
        content: response.answer || 'No answer returned.',
        sqlQuery: response.sql ?? undefined,
        data: response.data ?? [],
        rowCount: response.row_count ?? 0,
        chart: response.chart ?? undefined,
        charts: response.charts ?? undefined,
        evidence: response.evidence ?? undefined,
        resolution: response.resolution ?? undefined,
        error: response.error ?? undefined,
        mapIntent: response.mapIntent ?? undefined,
        resultPackage: response.resultPackage,
        contract: response.contract,
        pipelineTrace: response.pipelineTrace,
        quality: response.quality,
        suggestedFollowups: response.suggested_followups ?? undefined,
        suggestReasoningQuestion: shouldSuggestReasoning ? q : undefined,
        keyNumbers: response.key_numbers ?? undefined,
        caveats: response.caveats ?? undefined,
        confidence: response.confidence ?? undefined,
        glossary: response.glossary ?? undefined,
        verifiedQuery: response.verified_query ?? undefined,
      };

      onMessagesChange(threadId, [...updatedNext, assistantMsg]);

      // Update thread title from first user message
      if (messages.length === 0 && threadId) {
        const title = q.length > 60 ? q.slice(0, 60).trim() + '...' : q;
        onUpdateTitle(threadId, title);
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

  const detailMsg = detail ? messages.find((m) => m.id === detail.messageId) : undefined;
  // Thinking indicator shows until the answer preview lands (last msg = user).
  const showLivePlaceholder = isLoading && messages[messages.length - 1]?.role === 'user';

  return (
    <div className="flex h-full flex-1 overflow-hidden">
      {/* Chat column */}
      <main className="flex h-full min-w-0 flex-1 flex-col overflow-hidden bg-[var(--bg)]">
        {/* Header — quiet: thread title + share. Connection state only shows
            when something is actually wrong. */}
        <header className="shrink-0">
          <div className="mx-auto flex max-w-3xl items-center justify-between gap-3 px-5 py-2.5">
            <div className="flex min-w-0 items-center gap-2.5">
              {onOpenSidebar && (
                <button type="button" onClick={onOpenSidebar} aria-label="Open sidebar" className="rounded-md p-1.5 text-[var(--muted)] hover:text-[var(--ink)] lg:hidden">
                  <PanelLeft size={16} />
                </button>
              )}
              <span className="truncate text-[13px] font-medium text-[var(--muted)]">
                {messages.length ? thread?.title : ''}
              </span>
            </div>
            <div className="flex items-center gap-3">
              {healthStatus === 'offline' && (
                <span className="flex items-center gap-1.5 text-[11px] text-[var(--danger)]">
                  <span className="h-1.5 w-1.5 rounded-full bg-[var(--danger)]" />
                  Offline
                </span>
              )}
              {thread?.id && messages.length > 0 && (
                <button
                  type="button"
                  onClick={handleShare}
                  title="Get a read-only link to this chat"
                  className="inline-flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-[12px] text-[var(--muted)] transition hover:bg-[var(--surface-2)] hover:text-[var(--ink)]"
                >
                  {shareCopied ? <Check size={13} /> : <Share2 size={13} />}
                  {shareCopied ? 'Copied' : 'Share'}
                </button>
              )}
            </div>
          </div>
          {shareUrl && (
            <div className="mx-auto flex max-w-3xl items-center justify-between gap-3 px-5 pb-2 text-[11px]">
              <span className="text-[var(--muted)]">Read-only link:</span>
              <code className="flex-1 truncate rounded-lg border border-[var(--line-soft)] bg-[var(--surface)] px-2 py-1 text-[var(--ink-soft)]">{shareUrl}</code>
              <button type="button" onClick={() => setShareUrl(null)} className="text-[var(--muted)] hover:text-[var(--ink)]">×</button>
            </div>
          )}
        </header>

        {messages.length === 0 ? (
          /* Empty state — serif greeting, centered composer, starter chips */
          <div className="flex flex-1 items-center justify-center overflow-y-auto px-4">
            <div className="w-full max-w-2xl pb-16">
              <div className="flex items-center justify-center gap-3">
                <Sparkle size={26} className="shrink-0 text-[var(--accent)]" fill="currentColor" />
                <h1 className="font-display text-[32px] font-medium tracking-tight text-[var(--ink)] sm:text-[36px]">
                  What would you like to know?
                </h1>
              </div>

              {/* Composer */}
              <div className="mt-8">
                <Composer
                  input={input}
                  isLoading={isLoading}
                  placeholder="Ask about US public-policy data…"
                  textareaRef={textareaRef}
                  onChangeInput={setInput}
                  onSend={() => void send()}
                  reasoning={reasoning}
                  onToggleReasoning={() => setReasoning((v) => !v)}
                />
              </div>

              {/* Starter questions */}
              {selectedDataset.starterQuestions.length > 0 && (
                <div className="mt-6 flex flex-wrap justify-center gap-2">
                  {selectedDataset.starterQuestions.slice(0, 4).map((q, i) => (
                    <button
                      key={`${selectedDataset.id}-${i}`}
                      type="button"
                      onClick={() => void send(q)}
                      disabled={isLoading}
                      className="rounded-full border border-[var(--line)] bg-[var(--surface)] px-3.5 py-1.5 text-[12.5px] text-[var(--muted)] transition hover:border-[var(--muted-2)] hover:text-[var(--ink)] disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
        ) : (
          /* Chat view */
          <>
            <div className="flex-1 overflow-y-auto px-4 pb-10 pt-4">
              <div className="mx-auto w-full max-w-3xl space-y-7">
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
                    onAskFollowup={(text) => void send(text)}
                    onRerunReasoning={(text) => void send(text, 'reasoning')}
                  />
                ))}
                {showLivePlaceholder && (
                  <div className="flex items-center gap-2.5 px-1 py-1">
                    <Sparkle size={15} className="thinking-spark" fill="currentColor" />
                    <span className="thinking-label text-[13.5px] font-medium">{statusWord}…</span>
                  </div>
                )}
                <div ref={endRef} />
              </div>
            </div>

            {/* Bottom composer — floats on the bg like Claude, no separator bar */}
            <div className="shrink-0 bg-gradient-to-t from-[var(--bg)] via-[var(--bg)] to-transparent">
              <div className="mx-auto max-w-3xl px-4 pb-4 pt-1">
                <Composer
                  input={input}
                  isLoading={isLoading}
                  placeholder="Reply…"
                  textareaRef={textareaRef}
                  onChangeInput={setInput}
                  onSend={() => void send()}
                  compact
                  reasoning={reasoning}
                  onToggleReasoning={() => setReasoning((v) => !v)}
                />
              </div>
            </div>
          </>
        )}
      </main>

      {/* Right detail panel */}
      {detail && detailMsg && (
        <DetailPanel
          tab={detail.tab}
          sql={detailMsg.sqlQuery}
          data={detailMsg.data}
          rowCount={detailMsg.rowCount}
          onChangeTab={(tab) => setDetail({ ...detail, tab })}
          onClose={() => setDetail(null)}
        />
      )}
    </div>
  );
}

/* ── Composer — Claude-style rounded card with actions inside ── */
interface ComposerProps {
  input: string;
  isLoading: boolean;
  placeholder: string;
  textareaRef: React.RefObject<HTMLTextAreaElement | null>;
  onChangeInput: (v: string) => void;
  onSend: () => void;
  compact?: boolean;
  reasoning?: boolean;
  onToggleReasoning?: () => void;
}

function Composer({ input, isLoading, placeholder, textareaRef, onChangeInput, onSend, compact, reasoning, onToggleReasoning }: ComposerProps) {
  return (
    <form
      onSubmit={(e) => { e.preventDefault(); onSend(); }}
      className="transition-within rounded-2xl border border-[var(--line)] bg-[var(--surface)] px-3.5 pb-2.5 pt-1 shadow-[0_2px_12px_rgba(31,30,29,0.05)]"
    >
      <textarea
        ref={textareaRef}
        value={input}
        onChange={(e) => onChangeInput(e.target.value)}
        onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); onSend(); } }}
        placeholder={placeholder}
        disabled={isLoading}
        rows={1}
        className={`block w-full resize-none border-0 bg-transparent text-[15px] leading-6 text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] ${compact ? 'max-h-36 min-h-[36px] py-2' : 'max-h-48 min-h-[52px] py-2.5'}`}
      />
      <div className="flex items-center justify-between pt-0.5">
        <div>
          {onToggleReasoning && (
            <button
              type="button"
              onClick={onToggleReasoning}
              disabled={isLoading}
              aria-pressed={!!reasoning}
              title={reasoning ? 'Extended analysis on — slower, digs deeper' : 'Extended analysis off'}
              className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[12px] font-medium transition ${
                reasoning
                  ? 'bg-[var(--accent-soft)] text-[var(--accent)]'
                  : 'text-[var(--muted-2)] hover:bg-[var(--surface-2)] hover:text-[var(--muted)]'
              } disabled:opacity-50`}
            >
              <Lightbulb size={13} />
              Extended
            </button>
          )}
        </div>
        <button
          type="submit"
          disabled={!input.trim() || isLoading}
          aria-label={isLoading ? 'Sending...' : 'Send message'}
          className="grid h-8 w-8 shrink-0 place-items-center rounded-lg bg-[var(--accent)] text-white transition hover:bg-[var(--accent-hover)] disabled:bg-[var(--surface-2)] disabled:text-[var(--muted-2)]"
        >
          {isLoading ? <Loader2 size={15} className="animate-spin" /> : <ArrowUp size={15} strokeWidth={2.4} />}
        </button>
      </div>
    </form>
  );
}
