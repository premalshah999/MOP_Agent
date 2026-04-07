import { BarChart3, Database, TerminalSquare } from 'lucide-react';
import { motion } from 'motion/react';
import { lazy, Suspense } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { ChatMessage } from '@/types/chat';
import type { ReactNode } from 'react';

const VegaChart = lazy(() => import('./VegaChart').then((m) => ({ default: m.VegaChart })));

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
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        p: ({ children }) => <p className={`m-0 text-[14px] leading-7 ${cls}`}>{children}</p>,
        strong: ({ children }) => <strong className={`font-semibold ${cls}`}>{children}</strong>,
        em: ({ children }) => <em className={`italic ${m}`}>{children}</em>,
        ul: ({ children }) => <ul className={`m-0 list-disc space-y-1.5 pl-5 text-[14px] leading-7 ${cls}`}>{children}</ul>,
        ol: ({ children }) => <ol className={`m-0 list-decimal space-y-1.5 pl-5 text-[14px] leading-7 ${cls}`}>{children}</ol>,
        li: ({ children }) => <li className={cls}>{children}</li>,
        code: ({ children }) => <code className={`bg-[var(--surface-2)] px-1 py-0.5 font-mono text-[0.9em] ${cls}`}>{children}</code>,
      }}
    >
      {toMd(content)}
    </ReactMarkdown>
  );
}

/* ── Message ── */

interface MessageProps extends ChatMessage {
  onOpenDetail?: (tab: 'sql' | 'data') => void;
  /** Which tab is currently active in the detail panel for this message (if any) */
  activeDetailTab?: 'sql' | 'data' | null;
}

export function Message({ role, content, sqlQuery, data, rowCount, chart, error, ts, onOpenDetail, activeDetailTab }: MessageProps) {
  const rows = data ?? [];
  const hasSql = Boolean(sqlQuery);
  const hasData = rows.length > 0;
  const hasChart = Boolean(chart);

  if (role === 'user') {
    return (
      <motion.div
        initial={{ opacity: 0, y: 4 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.15 }}
        className="flex justify-end"
      >
        <div className="max-w-xl rounded-2xl rounded-br-sm bg-[var(--ink)] px-4 py-3 text-white">
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
        </div>
      </div>

      {/* Answer */}
      <div className="space-y-3">
        <Md content={content} tone="assistant" />
      </div>

      {/* Inline chart */}
      {hasChart && (
        <Suspense fallback={<div className="mt-3 h-32 animate-pulse rounded-lg bg-[var(--surface-2)]" />}>
          <VegaChart spec={chart!} />
        </Suspense>
      )}

      {/* Error */}
      {error && (
        <div className="mt-3 border-l-2 border-red-300 bg-red-50 px-3 py-2 text-[13px] text-[var(--danger)]">{error}</div>
      )}
    </motion.div>
  );
}

function Btn({ onClick, active, label, children }: { onClick: () => void; active: boolean; label: string; children: ReactNode }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={label}
      className={`inline-flex items-center gap-1 rounded px-2 py-1 text-[10px] font-medium transition ${
        active ? 'bg-[var(--surface-2)] text-[var(--ink)]' : 'text-[var(--muted)] hover:text-[var(--ink)]'
      }`}
    >
      {children}
    </button>
  );
}
