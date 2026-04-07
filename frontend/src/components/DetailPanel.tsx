import { Check, Copy, Database, TerminalSquare, X } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { useResizable } from '@/hooks/useResizable';

type Tab = 'sql' | 'data';

interface DetailPanelProps {
  tab: Tab;
  sql?: string | null;
  data?: Record<string, unknown>[];
  rowCount?: number;
  onChangeTab: (tab: Tab) => void;
  onClose: () => void;
}

function fmtCell(v: unknown): string {
  if (v === null || v === undefined) return '--';
  if (typeof v === 'number') return Number.isFinite(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '--';
  return String(v);
}

export function DetailPanel({ tab, sql, data, rowCount, onChangeTab, onClose }: DetailPanelProps) {
  const [copied, setCopied] = useState(false);
  const [visibleRows, setVisibleRows] = useState(50);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const { width, onMouseDown } = useResizable({
    initial: 420, min: 280, max: 700, edge: 'left', storageKey: 'mop-detail-w',
  });

  const rows = data ?? [];
  const cols = useMemo(() => (rows.length ? Object.keys(rows[0]) : []), [rows]);

  const hasSql = Boolean(sql);
  const hasData = rows.length > 0;

  // Auto-switch tab if current tab has no content
  const effectiveTab = tab === 'sql' && !hasSql && hasData ? 'data'
    : tab === 'data' && !hasData && hasSql ? 'sql'
    : tab;

  useEffect(() => () => { clearTimeout(timerRef.current); }, []);

  const copy = async () => {
    if (!sql) return;
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => setCopied(false), 1500);
    } catch { /* clipboard denied */ }
  };

  return (
    <aside
      className="relative flex h-screen shrink-0 flex-col border-l border-[var(--line)] bg-[var(--surface)]"
      style={{ width }}
    >
      {/* Drag handle (left edge) */}
      <div
        onMouseDown={onMouseDown}
        className="absolute left-0 top-0 z-30 h-full w-1 cursor-col-resize hover:bg-[var(--accent)]/20 active:bg-[var(--accent)]/30"
      />

      {/* Header */}
      <div className="flex items-center justify-between border-b border-[var(--line)] px-4 py-2.5">
        <div className="flex items-center gap-1">
          {hasSql && (
            <button
              type="button"
              onClick={() => onChangeTab('sql')}
              aria-label="View SQL query"
              className={`inline-flex items-center gap-1.5 rounded px-2.5 py-1.5 text-[11px] font-medium transition ${
                effectiveTab === 'sql' ? 'bg-[var(--surface-2)] text-[var(--ink)]' : 'text-[var(--muted)] hover:text-[var(--ink)]'
              }`}
            >
              <TerminalSquare size={12} />
              SQL
            </button>
          )}
          {hasData && (
            <button
              type="button"
              onClick={() => onChangeTab('data')}
              aria-label="View data table"
              className={`inline-flex items-center gap-1.5 rounded px-2.5 py-1.5 text-[11px] font-medium transition ${
                effectiveTab === 'data' ? 'bg-[var(--surface-2)] text-[var(--ink)]' : 'text-[var(--muted)] hover:text-[var(--ink)]'
              }`}
            >
              <Database size={12} />
              Data
              {typeof rowCount === 'number' && <span className="font-mono text-[var(--muted-2)]">{rowCount}</span>}
            </button>
          )}
        </div>

        <div className="flex items-center gap-1">
          {effectiveTab === 'sql' && hasSql && (
            <button
              type="button"
              onClick={() => void copy()}
              aria-label={copied ? 'Copied to clipboard' : 'Copy SQL to clipboard'}
              className="inline-flex items-center gap-1 rounded px-2 py-1 text-[10px] text-[var(--muted)] hover:text-[var(--ink)]"
            >
              {copied ? <Check size={10} /> : <Copy size={10} />}
              {copied ? 'Copied' : 'Copy'}
            </button>
          )}
          <button
            type="button"
            onClick={onClose}
            aria-label="Close panel"
            className="rounded p-1 text-[var(--muted)] hover:text-[var(--ink)]"
          >
            <X size={14} />
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto">
        {effectiveTab === 'sql' && sql && (
          <div className="p-4">
            <pre className="overflow-x-auto whitespace-pre-wrap break-words rounded bg-[var(--ink)] p-4 font-mono text-[12px] leading-6 text-white/85">
              <code>{sql}</code>
            </pre>
          </div>
        )}

        {effectiveTab === 'data' && rows.length > 0 && (
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-[11px]">
              <thead className="sticky top-0 z-10">
                <tr className="border-b border-[var(--line)] bg-[var(--surface-2)]">
                  {cols.map((c) => (
                    <th
                      key={c}
                      className="whitespace-nowrap px-3 py-2 font-mono text-[10px] font-medium uppercase tracking-wide text-[var(--muted)]"
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.slice(0, visibleRows).map((row, i) => (
                  <tr key={i} className="border-b border-[var(--line-soft)] last:border-0 hover:bg-[var(--surface-2)]">
                    {cols.map((c) => (
                      <td
                        key={`${i}-${c}`}
                        className="max-w-[200px] whitespace-normal break-words px-3 py-1.5 text-[var(--ink)]"
                      >
                        {fmtCell(row[c])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            {visibleRows < rows.length && (
              <button
                type="button"
                onClick={() => setVisibleRows((v) => v + 50)}
                className="w-full border-t border-[var(--line)] py-2 text-[10px] text-[var(--muted)] hover:text-[var(--ink)]"
              >
                Show more rows ({rows.length - visibleRows} remaining)
              </button>
            )}
          </div>
        )}

        {/* Empty state */}
        {!hasSql && !hasData && (
          <div className="flex h-full items-center justify-center p-8 text-[12px] text-[var(--muted)]">
            No SQL or data available for this message.
          </div>
        )}
      </div>
    </aside>
  );
}
