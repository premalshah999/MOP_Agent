import { ChevronDown, ChevronRight, Database, Download, FileSpreadsheet, PlayCircle } from 'lucide-react';
import { useMemo, useState } from 'react';
import { buildApiUrl } from '@/lib/api';
import type { DatasetCatalogEntry } from '@/types/chat';


interface DatasetLibraryProps {
  catalog: DatasetCatalogEntry[];
  selectedDatasetId: string;
  onSelectDataset: (id: string) => void;
  compact?: boolean;
}

function formatNumber(value: number): string {
  return value.toLocaleString();
}

export function DatasetLibrary({ catalog, selectedDatasetId, onSelectDataset, compact = false }: DatasetLibraryProps) {
  const [expandedDatasets, setExpandedDatasets] = useState<Record<string, boolean>>({});
  const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({});

  const normalizedExpandedDatasets = useMemo(() => {
    if (Object.keys(expandedDatasets).length) return expandedDatasets;
    return Object.fromEntries(catalog.map((entry) => [entry.id, entry.id === selectedDatasetId]));
  }, [catalog, expandedDatasets, selectedDatasetId]);

  if (!catalog.length) {
    return (
      <div className="px-3 py-4 text-[12px] leading-6 text-[var(--sidebar-muted)]">
        Dataset library is loading.
      </div>
    );
  }

  if (compact) {
    return (
      <div className="space-y-3 px-2 pb-3">
        <div className="px-2">
          <div className="text-[10px] font-medium uppercase tracking-widest text-[var(--sidebar-muted)]">
            Dataset Navigator
          </div>
          <p className="mt-2 text-[11px] leading-5 text-[var(--sidebar-muted)]">
            Use the main workspace for full dataset cards, column previews, and downloads. The sidebar stays lightweight for switching modes quickly.
          </p>
        </div>

        <div className="space-y-2">
          {catalog.map((entry) => {
            const selected = entry.id === selectedDatasetId;
            return (
              <section
                key={entry.id}
                className={`rounded border px-3 py-3 transition ${
                  selected
                    ? 'border-white/20 bg-[var(--sidebar-active)]'
                    : 'border-[var(--sidebar-line)] bg-transparent'
                }`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-[12px] font-semibold text-[var(--sidebar-ink)]">{entry.name}</span>
                      {selected && (
                        <span className="rounded-full bg-white/10 px-1.5 py-0.5 text-[9px] uppercase tracking-wide text-white/80">
                          active
                        </span>
                      )}
                    </div>
                    <p className="mt-1 text-[11px] leading-5 text-[var(--sidebar-muted)]">{entry.description}</p>
                  </div>
                  <button
                    type="button"
                    onClick={() => onSelectDataset(entry.id)}
                    className="inline-flex shrink-0 items-center gap-1 rounded border border-[var(--sidebar-line)] px-2 py-1 text-[10px] font-medium text-[var(--sidebar-ink)] transition hover:bg-[var(--sidebar-hover)]"
                  >
                    <PlayCircle size={10} />
                    Use
                  </button>
                </div>
              </section>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2 px-2 pb-3">
      <div className="px-2 pb-2 text-[10px] font-medium uppercase tracking-widest text-[var(--sidebar-muted)]">
        Data Library
      </div>

      {catalog.map((entry) => {
        const expanded = normalizedExpandedDatasets[entry.id] ?? false;
        const selected = entry.id === selectedDatasetId;
        return (
          <section
            key={entry.id}
            className={`overflow-hidden rounded border transition ${
              selected
                ? 'border-white/20 bg-[var(--sidebar-active)]'
                : 'border-[var(--sidebar-line)] bg-transparent'
            }`}
          >
            <button
              type="button"
              onClick={() => setExpandedDatasets((prev) => ({ ...prev, [entry.id]: !expanded }))}
              className="flex w-full items-start justify-between gap-3 px-3 py-3 text-left"
            >
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-[12px] font-semibold text-[var(--sidebar-ink)]">{entry.name}</span>
                  {selected && (
                    <span className="rounded-full bg-white/10 px-1.5 py-0.5 text-[9px] uppercase tracking-wide text-white/80">
                      active
                    </span>
                  )}
                </div>
                <p className="mt-1 text-[11px] leading-5 text-[var(--sidebar-muted)]">{entry.description}</p>
              </div>
              {expanded ? <ChevronDown size={14} className="shrink-0 text-[var(--sidebar-muted)]" /> : <ChevronRight size={14} className="shrink-0 text-[var(--sidebar-muted)]" />}
            </button>

            {expanded && (
              <div className="space-y-3 border-t border-[var(--sidebar-line)] px-3 py-3">
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => onSelectDataset(entry.id)}
                    className="inline-flex items-center gap-1 rounded border border-[var(--sidebar-line)] px-2 py-1 text-[10px] font-medium text-[var(--sidebar-ink)] transition hover:bg-[var(--sidebar-hover)]"
                  >
                    <PlayCircle size={11} />
                    Use in chat
                  </button>
                </div>

                <p className="text-[11px] leading-5 text-[var(--sidebar-muted)]">{entry.helper}</p>

                {entry.notes && entry.notes.length > 0 && (
                  <div className="space-y-1">
                    {entry.notes.map((note) => (
                      <p key={note} className="text-[10px] leading-5 text-[var(--sidebar-muted)]">
                        {note}
                      </p>
                    ))}
                  </div>
                )}

                {entry.tables.length === 0 ? (
                  <div className="rounded border border-dashed border-[var(--sidebar-line)] px-3 py-2 text-[11px] leading-5 text-[var(--sidebar-muted)]">
                    This analysis mode combines the downloadable families above rather than exposing a single export file.
                  </div>
                ) : (
                  <div className="space-y-2">
                    {entry.tables.map((table) => {
                      const tableExpanded = expandedTables[table.tableName] ?? false;
                      const visibleColumns = tableExpanded ? table.columns : table.columns.slice(0, 8);
                      return (
                        <article key={table.tableName} className="rounded border border-[var(--sidebar-line)] bg-black/10 px-3 py-3">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <div className="flex items-center gap-2">
                                <Database size={12} className="shrink-0 text-[var(--sidebar-muted)]" />
                                <span className="truncate text-[11px] font-semibold text-[var(--sidebar-ink)]">{table.label}</span>
                              </div>
                              <p className="mt-1 text-[10px] leading-5 text-[var(--sidebar-muted)]">{table.summary}</p>
                              <div className="mt-1 text-[10px] text-[var(--sidebar-muted)]">
                                {table.grain} · {formatNumber(table.rows)} rows
                              </div>
                            </div>
                            <div className="flex shrink-0 items-center gap-1">
                              {table.downloads.parquet && (
                                <a
                                  href={buildApiUrl(table.downloads.parquet)}
                                  className="inline-flex items-center gap-1 rounded border border-[var(--sidebar-line)] px-2 py-1 text-[10px] font-medium text-[var(--sidebar-ink)] transition hover:bg-[var(--sidebar-hover)]"
                                >
                                  <Download size={10} />
                                  Parquet
                                </a>
                              )}
                              {table.downloads.xlsx && (
                                <a
                                  href={buildApiUrl(table.downloads.xlsx)}
                                  className="inline-flex items-center gap-1 rounded border border-[var(--sidebar-line)] px-2 py-1 text-[10px] font-medium text-[var(--sidebar-ink)] transition hover:bg-[var(--sidebar-hover)]"
                                >
                                  <FileSpreadsheet size={10} />
                                  XLSX
                                </a>
                              )}
                            </div>
                          </div>

                          <div className="mt-3">
                            <div className="mb-2 flex items-center justify-between gap-3">
                              <span className="text-[10px] uppercase tracking-wider text-[var(--sidebar-muted)]">Columns</span>
                              {table.columns.length > 8 && (
                                <button
                                  type="button"
                                  onClick={() => setExpandedTables((prev) => ({ ...prev, [table.tableName]: !tableExpanded }))}
                                  className="text-[10px] text-[var(--sidebar-muted)] hover:text-[var(--sidebar-ink)]"
                                >
                                  {tableExpanded ? 'Show fewer' : `Show all (${table.columns.length})`}
                                </button>
                              )}
                            </div>
                            <div className="flex flex-wrap gap-1.5">
                              {visibleColumns.map((column) => (
                                <span
                                  key={column}
                                  className="rounded-full border border-[var(--sidebar-line)] px-2 py-0.5 text-[10px] leading-5 text-[var(--sidebar-ink)]"
                                >
                                  {column}
                                </span>
                              ))}
                            </div>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                )}
              </div>
            )}
          </section>
        );
      })}
    </div>
  );
}
