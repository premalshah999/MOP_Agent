import { BadgeCheck, Copy, Database, TerminalSquare, Lightbulb, ThumbsUp, ThumbsDown } from 'lucide-react';
import { sendFeedback } from '@/lib/api';
import { useSettings } from '@/lib/settings';
import { motion } from 'motion/react';
import { Fragment, lazy, Suspense, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { ChatMessage, EvidenceBlock, EvidenceCard, EvidenceSection, KeyNumber } from '@/types/chat';
import type { ReactNode } from 'react';
import { ChatbotMapButton } from './ChatbotMapButton';

const VegaChart = lazy(() => import('./VegaChart').then((m) => ({ default: m.VegaChart })));
const MapView = lazy(() => import('./MapView').then((m) => ({ default: m.MapView })));

/* ── Helpers ── */

function normalize(text: string): string {
  const t = text.trim();
  if (!t) return t;
  if (/\n\s*\n/.test(t) || /^\s*[-*]\s+/m.test(t) || /^\s*\d+\.\s+/m.test(t)) return t;
  const sentences = t.replace(/\s+/g, ' ').split(/(?<=[.!?])\s+/).map(s => s.trim()).filter(Boolean);
  if (sentences.length <= 2) return t;
  const cue = /^(overall|however|for policy|a key limitation|a major caveat|in contrast|meanwhile|this suggests|across the full)/i;
  const paras: string[] = [];
  let cur: string[] = [];
  for (const s of sentences) {
    if (cur.length >= 2 || (cur.length > 0 && cue.test(s))) { paras.push(cur.join(' ')); cur = []; }
    cur.push(s);
  }
  if (cur.length) paras.push(cur.join(' '));
  return paras.join('\n\n');
}

function hasMd(t: string): boolean {
  return /(^|\n)\s*([-*]|\d+\.)\s+|(\*\*[^*\n]+\*\*)|(^|\n)#{1,6}\s+/m.test(t);
}

function toMd(text: string): string {
  const n = normalize(text);
  if (!n || hasMd(n)) return n;
  const paras = n.split(/\n{2,}/).map(s => s.trim()).filter(Boolean);
  if (!paras.length) return n;
  const [lead, ...rest] = paras;
  const sentences = lead.split(/(?<=[.!?])\s+/).filter(Boolean);
  const md: string[] = [];
  md.push(sentences.length > 1 ? `**${sentences[0]}** ${sentences.slice(1).join(' ')}` : `**${lead}**`);
  rest.forEach((p) => md.push(p));
  return md.join('\n\n');
}

function wrapGlossary(text: string, glossary: Record<string, string>): ReactNode {
  const terms = Object.keys(glossary).sort((a, b) => b.length - a.length);
  if (!terms.length) return text;
  const escaped = terms.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const re = new RegExp(`\\b(${escaped.join('|')})\\b`, 'g');
  const parts: ReactNode[] = [];
  let last = 0;
  for (const m of text.matchAll(re)) {
    const i = m.index ?? 0;
    if (i > last) parts.push(text.slice(last, i));
    const key = terms.find((t) => t.toLowerCase() === m[0].toLowerCase()) || m[0];
    parts.push(
      <abbr
        key={`${i}-${m[0]}`}
        title={glossary[key]}
        className="cursor-help border-b border-dotted border-[var(--muted-2)] no-underline"
      >
        {m[0]}
      </abbr>,
    );
    last = i + m[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return <>{parts}</>;
}

function withGlossary(children: ReactNode, glossary?: Record<string, string>): ReactNode {
  if (!glossary || Object.keys(glossary).length === 0) return children;
  const out: ReactNode[] = [];
  const visit = (n: ReactNode) => {
    if (typeof n === 'string') {
      out.push(<Fragment key={`g-${out.length}`}>{wrapGlossary(n, glossary)}</Fragment>);
    } else if (Array.isArray(n)) {
      n.forEach(visit);
    } else {
      out.push(<Fragment key={`g-${out.length}`}>{n}</Fragment>);
    }
  };
  visit(children);
  return <>{out}</>;
}

function Md({ content, tone, glossary }: { content: string; tone: 'user' | 'assistant'; glossary?: Record<string, string> }) {
  const cls = 'text-[var(--ink)]';
  const m = 'text-[var(--muted)]';
  return (
    <div className={`markdown-flow ${tone === 'user' ? 'markdown-flow-user' : ''}`}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({ children }) => <p className={`m-0 ${cls}`}>{withGlossary(children, glossary)}</p>,
          h1: ({ children }) => <h1 className={`m-0 text-[18px] font-semibold leading-7 ${cls}`}>{withGlossary(children, glossary)}</h1>,
          h2: ({ children }) => <h2 className={`m-0 text-[16px] font-semibold leading-7 ${cls}`}>{withGlossary(children, glossary)}</h2>,
          h3: ({ children }) => <h3 className={`m-0 text-[14px] font-semibold leading-7 ${cls}`}>{withGlossary(children, glossary)}</h3>,
          strong: ({ children }) => <strong className={`font-semibold ${cls}`}>{withGlossary(children, glossary)}</strong>,
          em: ({ children }) => <em className={`italic ${m}`}>{withGlossary(children, glossary)}</em>,
          ul: ({ children }) => <ul className={`m-0 list-disc space-y-1.5 pl-5 ${cls}`}>{children}</ul>,
          ol: ({ children }) => <ol className={`m-0 list-decimal space-y-1.5 pl-5 ${cls}`}>{children}</ol>,
          li: ({ children }) => <li className={cls}>{withGlossary(children, glossary)}</li>,
          code: ({ children }) => <code className={`rounded-[4px] bg-[var(--surface-2)] px-1 py-0.5 font-mono text-[0.9em] ${cls}`}>{children}</code>,
          table: ({ children }) => (
            <div className="overflow-x-auto rounded-xl border border-[var(--line)] bg-[var(--surface)]">
              <table className={`w-full border-collapse text-left text-[13px] leading-6 ${cls}`}>{children}</table>
            </div>
          ),
          th: ({ children }) => (
            <th className="border-b border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--muted)]">
              {children}
            </th>
          ),
          td: ({ children }) => <td className="border-b border-[var(--line)] px-3 py-2 align-top">{withGlossary(children, glossary)}</td>,
        }}
      >
        {toMd(content)}
      </ReactMarkdown>
    </div>
  );
}

/* ── Message ── */

interface MessageProps extends ChatMessage {
  onOpenDetail?: (tab: 'sql' | 'data') => void;
  onAskFollowup?: (text: string) => void;
  onRerunReasoning?: (text: string) => void;
  /** Which tab is currently active in the detail panel for this message (if any) */
  activeDetailTab?: 'sql' | 'data' | null;
  datasetId?: string;
}

export function Message({ id, role, content, sqlQuery, data, rowCount, chart, charts, evidence, resolution, error, ts, mapIntent, datasetId, onOpenDetail, activeDetailTab, suggestedFollowups, suggestReasoningQuestion, onAskFollowup, onRerunReasoning, keyNumbers, caveats, confidence, glossary, verifiedQuery }: MessageProps) {
  const settings = useSettings();
  const effectiveGlossary = settings.glossaryTooltips ? glossary : undefined;
  const [verdict, setVerdict] = useState<'up' | 'down' | null>(null);
  const [feedbackError, setFeedbackError] = useState<string | null>(null);

  const submitFeedback = async (v: 'up' | 'down') => {
    if (verdict) return; // already submitted; no toggle to keep it simple
    setVerdict(v);
    try {
      await sendFeedback({ message_id: id, verdict: v });
    } catch (e) {
      setVerdict(null);
      setFeedbackError(e instanceof Error ? e.message : 'Could not send feedback');
    }
  };
  const rows = data ?? [];
  const hasSql = Boolean(sqlQuery);
  const hasData = rows.length > 0;
  const hasChart = Boolean(chart);
  const chartBlocks = charts ?? [];
  const hasChartBlocks = chartBlocks.length > 0;
  const [mapOpen, setMapOpen] = useState(false);
  const effectiveMapIntent = mapIntent ?? null;
  const hasMap = Boolean(effectiveMapIntent?.enabled && effectiveMapIntent.mapType !== 'none' && !error);

  if (role === 'user') {
    return (
      <motion.div
        initial={{ opacity: 0, y: 4 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.15 }}
        className="flex justify-end"
      >
        <div className="max-w-xl rounded-2xl bg-[var(--surface-2)] px-4 py-2.5 text-[var(--ink)]">
          <p className="whitespace-pre-wrap text-[14.5px] leading-7">{content}</p>
        </div>
      </motion.div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.15 }}
      className="msg-group"
    >
      {/* Meta row — only when there is something meaningful to show */}
      <div className="mb-2 flex flex-wrap items-center gap-2">
        {confidence && confidence !== 'high' && (
          <ConfidenceChip level={confidence} />
        )}
        {verifiedQuery && (
          <span
            className="inline-flex items-center gap-1 rounded-full border border-[var(--line)] bg-[var(--surface)] px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.1em] text-[var(--ink-soft)]"
            title={`Matched analyst-verified query ${verifiedQuery.id}${verifiedQuery.score ? ` (similarity ${verifiedQuery.score.toFixed(2)})` : ''}`}
          >
            <BadgeCheck size={11} className="text-emerald-600" />
            Verified
          </span>
        )}

        {/* Toggle buttons */}
        <div className="ml-auto flex items-center gap-1">
          {hasSql && (
            <Btn onClick={() => onOpenDetail?.('sql')} active={activeDetailTab === 'sql'} label="View SQL">
              <TerminalSquare size={11} /> SQL
            </Btn>
          )}
          {hasData && (
            <Btn onClick={() => onOpenDetail?.('data')} active={activeDetailTab === 'data'} label="View data table">
              <Database size={11} /> Data
            </Btn>
          )}
          {hasMap && (
            <ChatbotMapButton onClick={() => setMapOpen(true)} label={effectiveMapIntent?.buttonLabel} />
          )}
        </div>
      </div>

      {/* Answer */}
      <div className="space-y-3">
        {keyNumbers && keyNumbers.length > 0 && !error && (
          <KeyNumberCallout items={keyNumbers} />
        )}
        <Md content={content} tone="assistant" glossary={effectiveGlossary} />
      </div>

      {/* Caveats footer */}
      {caveats && caveats.length > 0 && !error && (
        <CaveatsFooter items={caveats} glossary={effectiveGlossary} />
      )}

      {resolution && resolution !== 'answered' && (
        <div className="mt-3 inline-flex rounded-full bg-[var(--surface-2)] px-3 py-1 text-[11px] font-medium text-[var(--muted)]">
          {resolution === 'partially_answered' ? 'Partial coverage'
            : resolution === 'needs_clarification' ? 'Needs clarification'
            : resolution === 'no_data' ? 'No matching data'
            : resolution === 'unsupported' ? 'Not in catalog'
            : resolution === 'error' ? 'Could not answer'
            : resolution}
        </div>
      )}

      {evidence && <EvidencePanel evidence={evidence} />}

      {/* Inline chart */}
      {hasChartBlocks && (
        <div className="mt-4 space-y-3">
          {chartBlocks.map((block, index) => (
            <div key={`${block.title}-${index}`} className="space-y-1.5">
              <div className="space-y-0.5">
                <h4 className="text-[13px] font-medium text-[var(--ink-soft)]">
                  {block.title}
                </h4>
                {block.subtitle && <p className="text-[12px] text-[var(--muted)]">{block.subtitle}</p>}
              </div>
              <Suspense fallback={<div className="h-32 animate-pulse rounded-[8px] bg-[var(--surface-2)]" />}>
                <VegaChart spec={block.spec} ariaLabel={block.title} />
              </Suspense>
            </div>
          ))}
        </div>
      )}

      {!hasChartBlocks && hasChart && (
        <Suspense fallback={<div className="mt-3 h-32 animate-pulse rounded-[8px] bg-[var(--surface-2)]" />}>
          <VegaChart spec={chart!} ariaLabel="Answer visualization" />
        </Suspense>
      )}

      {/* Error */}
      {error && (
        <div className="mt-3 rounded-xl border border-red-200 bg-red-50 px-3.5 py-2.5 text-[13px] text-[var(--danger)]">{error}</div>
      )}

      {hasMap && effectiveMapIntent && (
        <Suspense fallback={null}>
          <MapView
            isOpen={mapOpen}
            onClose={() => setMapOpen(false)}
            mapIntent={effectiveMapIntent}
            rows={rows}
          />
        </Suspense>
      )}

      {role === 'assistant' && !error && suggestReasoningQuestion && onRerunReasoning && (
        <button
          type="button"
          onClick={() => onRerunReasoning(suggestReasoningQuestion)}
          className="mt-3 inline-flex items-center gap-1.5 text-[12px] text-[var(--muted)] hover:text-[var(--ink)] underline-offset-2 hover:underline"
        >
          <Lightbulb size={13} className="opacity-70" />
          Get peer context — re-run in reasoning mode
        </button>
      )}

      {role === 'assistant' && !error && settings.showFollowups && suggestedFollowups && suggestedFollowups.length > 0 && onAskFollowup && (
        <div className="mt-3 flex flex-wrap gap-1.5">
          {suggestedFollowups.slice(0, 3).map((s, i) => (
            <button
              key={`${i}-${s}`}
              type="button"
              onClick={() => onAskFollowup(s)}
              className="rounded-full border border-[var(--line)] bg-[var(--surface)] px-3.5 py-1.5 text-[12.5px] text-[var(--muted)] transition hover:border-[var(--muted-2)] hover:text-[var(--ink)]"
            >
              {s}
            </button>
          ))}
        </div>
      )}

      {role === 'assistant' && !error && (
        <div className={`mt-2 flex items-center gap-0.5 text-[var(--muted-2)] ${verdict ? '' : 'msg-actions'}`}>
          <IconAction label="Copy answer" onClick={() => void navigator.clipboard.writeText(content)}>
            <Copy size={13} />
          </IconAction>
          <IconAction
            label="Good answer"
            active={verdict === 'up'}
            disabled={verdict !== null}
            onClick={() => void submitFeedback('up')}
          >
            <ThumbsUp size={13} strokeWidth={verdict === 'up' ? 2.4 : 1.7} />
          </IconAction>
          <IconAction
            label="Bad answer"
            active={verdict === 'down'}
            disabled={verdict !== null}
            onClick={() => void submitFeedback('down')}
          >
            <ThumbsDown size={13} strokeWidth={verdict === 'down' ? 2.4 : 1.7} />
          </IconAction>
          {feedbackError && <span className="ml-1 text-[11px] text-[var(--danger)]">{feedbackError}</span>}
        </div>
      )}
    </motion.div>
  );
}

function EvidencePanel({ evidence }: { evidence: EvidenceBlock }) {
  const cards = evidence.cards ?? [];
  const sections = evidence.sections ?? [];
  if (cards.length === 0 && sections.length === 0 && !evidence.note) return null;

  return (
    <div className="mt-4 space-y-3">
      {cards.length > 0 && (
        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          {cards.map((card, index) => (
            <EvidenceCardView key={`${card.label}-${index}`} card={card} />
          ))}
        </div>
      )}
      {sections.length > 0 && (
        <div className="grid gap-3">
          {sections.map((section, index) => (
            <EvidenceSectionView key={`${section.title}-${index}`} section={section} />
          ))}
        </div>
      )}
      {evidence.note && (
        <div className="rounded-[8px] border border-[var(--line)] bg-[var(--surface)] px-4 py-3 text-[12px] leading-6 text-[var(--muted)]">
          {evidence.note}
        </div>
      )}
    </div>
  );
}

function EvidenceCardView({ card }: { card: EvidenceCard }) {
  return (
    <div className="rounded-[8px] border border-[var(--line)] bg-[var(--surface)] px-4 py-3">
      <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">{card.label}</div>
      <div className="mt-1 text-[20px] font-semibold tracking-tight text-[var(--ink)]">{card.value}</div>
      {card.meta && <div className="mt-1 text-[11px] text-[var(--muted-2)]">{card.meta}</div>}
    </div>
  );
}

function EvidenceSectionView({ section }: { section: EvidenceSection }) {
  const cards = section.cards ?? [];
  const items = section.items ?? [];
  const rows = section.rows ?? [];
  return (
    <section className="rounded-[8px] border border-[var(--line)] bg-[var(--surface)] px-4 py-4">
      <div className="space-y-0.5">
        <h4 className="text-[12px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">{section.title}</h4>
        {section.subtitle && <p className="text-[11px] text-[var(--muted-2)]">{section.subtitle}</p>}
      </div>
      {cards.length > 0 && (
        <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
          {cards.map((card, index) => (
            <EvidenceCardView key={`${section.title}-${card.label}-${index}`} card={card} />
          ))}
        </div>
      )}
      {rows.length > 0 && (
        <div className="mt-3 divide-y divide-[var(--line)] border-y border-[var(--line)]">
          {rows.map((row, index) => (
            <div key={`${section.title}-${row.label}-${index}`} className="grid grid-cols-[minmax(0,1fr)_auto] items-start gap-3 py-2.5">
              <div className="min-w-0">
                <div className="truncate text-[13px] font-medium leading-6 text-[var(--ink)]">{row.label}</div>
                {row.meta && <div className="text-[11px] text-[var(--muted-2)]">{row.meta}</div>}
              </div>
              <div className="text-right text-[13px] font-semibold leading-6 text-[var(--ink)]">{row.value}</div>
            </div>
          ))}
        </div>
      )}
      {items.length > 0 && (
        <ul className="mt-3 space-y-1.5 text-[13px] leading-6 text-[var(--ink)]">
          {items.map((item, index) => (
            <li key={`${section.title}-${index}`} className="border-t border-[var(--line)] pt-2 first:border-t-0 first:pt-0">
              {item}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function KeyNumberCallout({ items }: { items: KeyNumber[] }) {
  const shown = items.slice(0, 4);
  return (
    <div className={`grid gap-2 ${shown.length === 1 ? 'grid-cols-1' : shown.length === 2 ? 'sm:grid-cols-2' : 'sm:grid-cols-2 xl:grid-cols-4'}`}>
      {shown.map((k, i) => (
        <div key={`${k.label}-${i}`} className="elevate rounded-xl border border-[var(--line-soft)] bg-[var(--surface)] px-4 py-3">
          <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">{k.label}</div>
          <div className="tabular-nums mt-1 text-[22px] font-semibold tracking-tight text-[var(--ink)]">
            {String(k.value)}
            {k.unit ? <span className="ml-1 text-[12px] font-medium text-[var(--muted-2)]">{k.unit}</span> : null}
          </div>
        </div>
      ))}
    </div>
  );
}

function CaveatsFooter({ items, glossary }: { items: string[]; glossary?: Record<string, string> }) {
  const shown = items.filter(Boolean).slice(0, 5);
  if (shown.length === 0) return null;
  return (
    <div className="mt-3 rounded-xl bg-[var(--surface-2)]/60 px-4 py-3">
      <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--muted-2)]">Notes</div>
      <ul className="mt-1.5 space-y-1 text-[12px] leading-6 text-[var(--muted)]">
        {shown.map((c, i) => (
          <li key={`${i}-${c.slice(0, 24)}`} className="before:mr-2 before:text-[var(--muted-2)] before:content-['•']">
            {wrapGlossary(c, glossary || {})}
          </li>
        ))}
      </ul>
    </div>
  );
}

function ConfidenceChip({ level }: { level: string }) {
  const lc = level.toLowerCase();
  const label = lc === 'low' ? 'Low confidence' : lc === 'medium' ? 'Medium confidence' : `Confidence: ${level}`;
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full border border-[var(--line)] bg-[var(--surface)] px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.1em] text-[var(--muted)]"
      title="LLM-reported confidence in this answer"
    >
      <span className="h-1.5 w-1.5 rounded-full bg-[var(--muted-2)]" />
      {label}
    </span>
  );
}

function IconAction({ onClick, label, active, disabled, children }: { onClick: () => void; label: string; active?: boolean; disabled?: boolean; children: ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-label={label}
      title={label}
      className={`rounded-lg p-1.5 transition hover:bg-[var(--surface-2)] ${
        active ? 'text-[var(--accent)]' : 'hover:text-[var(--ink)]'
      } disabled:cursor-default disabled:hover:bg-transparent`}
    >
      {children}
    </button>
  );
}

function Btn({ onClick, active, label, children }: { onClick: () => void; active: boolean; label: string; children: ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={label}
      className={`inline-flex items-center gap-1 rounded-[6px] px-2 py-1 text-[10px] font-medium transition ${
        active ? 'bg-[var(--surface-2)] text-[var(--ink)]' : 'text-[var(--muted)] hover:text-[var(--ink)]'
      }`}
    >
      {children}
    </button>
  );
}
