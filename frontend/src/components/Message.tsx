import { Database, TerminalSquare } from 'lucide-react';
import { motion } from 'motion/react';
import { lazy, Suspense, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { ChatMessage, EvidenceBlock, EvidenceCard, EvidenceSection } from '@/types/chat';
import type { ReactNode } from 'react';
import { ChatbotMapButton } from './ChatbotMapButton';

const VegaChart = lazy(() => import('./VegaChart').then((m) => ({ default: m.VegaChart })));
const ChatbotMapModal = lazy(() => import('./ChatbotMapModal').then((m) => ({ default: m.ChatbotMapModal })));

/* ── Helpers ── */

function fmtTime(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

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

function Md({ content, tone }: { content: string; tone: 'user' | 'assistant' }) {
  const cls = tone === 'user' ? 'text-white' : 'text-[var(--ink)]';
  const m = tone === 'user' ? 'text-white/60' : 'text-[var(--muted)]';
  return (
    <div className={`markdown-flow ${tone === 'user' ? 'markdown-flow-user' : ''}`}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({ children }) => <p className={`m-0 text-[14px] leading-7 ${cls}`}>{children}</p>,
          h1: ({ children }) => <h1 className={`m-0 text-[18px] font-semibold leading-7 ${cls}`}>{children}</h1>,
          h2: ({ children }) => <h2 className={`m-0 text-[16px] font-semibold leading-7 ${cls}`}>{children}</h2>,
          h3: ({ children }) => <h3 className={`m-0 text-[14px] font-semibold leading-7 ${cls}`}>{children}</h3>,
          strong: ({ children }) => <strong className={`font-semibold ${cls}`}>{children}</strong>,
          em: ({ children }) => <em className={`italic ${m}`}>{children}</em>,
          ul: ({ children }) => <ul className={`m-0 list-disc space-y-1.5 pl-5 text-[14px] leading-7 ${cls}`}>{children}</ul>,
          ol: ({ children }) => <ol className={`m-0 list-decimal space-y-1.5 pl-5 text-[14px] leading-7 ${cls}`}>{children}</ol>,
          li: ({ children }) => <li className={cls}>{children}</li>,
          code: ({ children }) => <code className={`rounded-[4px] bg-[var(--surface-2)] px-1 py-0.5 font-mono text-[0.9em] ${cls}`}>{children}</code>,
          table: ({ children }) => (
            <div className="overflow-x-auto rounded-[8px] border border-[var(--line)]">
              <table className={`w-full border-collapse text-left text-[13px] leading-6 ${cls}`}>{children}</table>
            </div>
          ),
          th: ({ children }) => (
            <th className="border-b border-[var(--line)] bg-[var(--surface-2)] px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--muted)]">
              {children}
            </th>
          ),
          td: ({ children }) => <td className="border-b border-[var(--line)] px-3 py-2 align-top">{children}</td>,
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
  /** Which tab is currently active in the detail panel for this message (if any) */
  activeDetailTab?: 'sql' | 'data' | null;
  datasetId?: string;
}

export function Message({ role, content, sqlQuery, data, rowCount, chart, charts, evidence, resolution, error, ts, mapIntent, datasetId, onOpenDetail, activeDetailTab }: MessageProps) {
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
        <div className="max-w-xl rounded-[8px] bg-[var(--ink)] px-4 py-3 text-white">
          <p className="text-[14px] leading-6">{content}</p>
          <div className="mt-1 text-right text-[10px] text-white/40">{fmtTime(ts)}</div>
        </div>
      </motion.div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.15 }}
    >
      {/* Meta row */}
      <div className="mb-2 flex flex-wrap items-center gap-3">
        <span className="text-[10px] font-medium text-[var(--muted)]">{fmtTime(ts)}</span>
        {typeof rowCount === 'number' && rowCount > 0 && (
          <span className="font-mono text-[10px] text-[var(--muted-2)]">{rowCount} rows</span>
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
      <div className="border-l border-[var(--line)] pl-4">
        <Md content={content} tone="assistant" />
      </div>

      {resolution && resolution !== 'answered' && (
        <div className="mt-3 rounded-[8px] border border-[var(--line)] bg-[var(--surface)] px-4 py-2 text-[11px] font-medium uppercase tracking-[0.12em] text-[var(--muted)]">
          {resolution === 'partially_answered' ? 'Partial coverage' : resolution === 'needs_clarification' ? 'Needs clarification' : resolution}
        </div>
      )}

      {evidence && <EvidencePanel evidence={evidence} />}

      {/* Inline chart */}
      {hasChartBlocks && (
        <div className="mt-4 space-y-3">
          {chartBlocks.map((block, index) => (
            <div key={`${block.title}-${index}`} className="space-y-2">
              <div className="space-y-0.5">
                <h4 className="text-[12px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">
                  {block.title}
                </h4>
                {block.subtitle && <p className="text-[11px] text-[var(--muted-2)]">{block.subtitle}</p>}
              </div>
              <Suspense fallback={<div className="h-32 animate-pulse rounded-[8px] bg-[var(--surface-2)]" />}>
                <VegaChart spec={block.spec} />
              </Suspense>
            </div>
          ))}
        </div>
      )}

      {!hasChartBlocks && hasChart && (
        <Suspense fallback={<div className="mt-3 h-32 animate-pulse rounded-[8px] bg-[var(--surface-2)]" />}>
          <VegaChart spec={chart!} />
        </Suspense>
      )}

      {/* Error */}
      {error && (
        <div className="mt-3 rounded-[8px] border border-red-200 bg-red-50 px-3 py-2 text-[13px] text-[var(--danger)]">{error}</div>
      )}

      {hasMap && effectiveMapIntent && (
        <Suspense fallback={null}>
          <ChatbotMapModal
            isOpen={mapOpen}
            onClose={() => setMapOpen(false)}
            mapIntent={effectiveMapIntent}
            fallbackRows={rows}
          />
        </Suspense>
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
