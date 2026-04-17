import { ArrowRight, Database, Download, FileSpreadsheet, Layers3 } from 'lucide-react';
import { buildApiUrl } from '@/lib/api';
import type { DatasetGuide } from '@/lib/content';
import type { DatasetCatalogEntry } from '@/types/chat';

interface DatasetLibraryWorkspaceProps {
  datasets: DatasetGuide[];
  datasetCatalog: DatasetCatalogEntry[];
  selectedDatasetId: string;
  onSelectDataset: (id: string) => void;
  onUseInChat: (id: string) => void;
}

function formatNumber(value: number): string {
  return value.toLocaleString();
}

export function DatasetLibraryWorkspace({
  datasets,
  datasetCatalog,
  selectedDatasetId,
  onSelectDataset,
  onUseInChat,
}: DatasetLibraryWorkspaceProps) {
  const selectedGuide = datasets.find((entry) => entry.id === selectedDatasetId) ?? datasets[0];
  const selectedCatalog = datasetCatalog.find((entry) => entry.id === selectedGuide.id);
  const totalTables = selectedCatalog?.tables.length ?? 0;
  const totalRows = selectedCatalog?.tables.reduce((sum, table) => sum + table.rows, 0) ?? 0;

  return (
    <div className="h-full overflow-y-auto bg-[var(--bg)]">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-5 py-5 lg:px-8 lg:py-6">
        <section className="overflow-hidden rounded-[18px] border border-[var(--line)] bg-[var(--surface)] shadow-[0_10px_30px_rgba(17,19,24,0.05)]">
          <div className="grid gap-5 px-5 py-6 lg:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.85fr)] lg:px-7">
            <div>
              <p className="text-[11px] font-medium uppercase tracking-[0.28em] text-[var(--muted)]">
                Data Library
              </p>
              <h1 className="mt-3 max-w-2xl font-display text-3xl font-semibold tracking-tight text-[var(--ink)] lg:text-[2.4rem]">
                Download the actual datasets behind the chatbot.
              </h1>
              <p className="mt-4 max-w-3xl text-[14px] leading-7 text-[var(--ink-soft)]">
                This view is for browsing files, understanding what each dataset contains, and downloading the exact tables
                you want. The chat screen stays focused on asking and answering questions.
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-1 xl:grid-cols-3">
              <div className="rounded-[12px] border border-[var(--line)] bg-[var(--surface-2)]/65 px-4 py-3.5">
                <p className="text-[10px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">Dataset family</p>
                <p className="mt-2.5 text-[18px] font-semibold text-[var(--ink)]">{selectedGuide.name}</p>
              </div>
              <div className="rounded-[12px] border border-[var(--line)] bg-[var(--surface-2)]/65 px-4 py-3.5">
                <p className="text-[10px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">Tables available</p>
                <p className="mt-2.5 text-[18px] font-semibold text-[var(--ink)]">{formatNumber(totalTables)}</p>
              </div>
              <div className="rounded-[12px] border border-[var(--line)] bg-[var(--surface-2)]/65 px-4 py-3.5">
                <p className="text-[10px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">Rows in scope</p>
                <p className="mt-2.5 text-[18px] font-semibold text-[var(--ink)]">{formatNumber(totalRows)}</p>
              </div>
            </div>
          </div>
        </section>

        <div className="grid gap-5 lg:grid-cols-[280px_minmax(0,1fr)] xl:grid-cols-[320px_minmax(0,1fr)]">
          <aside className="h-fit rounded-[16px] border border-[var(--line)] bg-[var(--surface)] p-3 shadow-[0_6px_20px_rgba(17,19,24,0.04)] lg:sticky lg:top-6">
            <div className="px-2 pb-3 pt-1">
              <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">Browse families</p>
              <p className="mt-2 text-[12px] leading-6 text-[var(--muted)]">
                Pick a dataset family to see the files, columns, coverage, and downloads.
              </p>
            </div>

            <div className="space-y-2">
              {datasets.map((dataset) => {
                const active = dataset.id === selectedDatasetId;
                return (
                  <button
                    key={dataset.id}
                    type="button"
                    onClick={() => onSelectDataset(dataset.id)}
                    className={`w-full rounded-[10px] border px-3.5 py-3 text-left transition ${
                      active
                        ? 'border-[var(--ink)] bg-[var(--ink)] text-white'
                        : 'border-[var(--line)] bg-[var(--surface)] hover:border-[var(--ink)]/15 hover:bg-[var(--surface-2)]'
                    }`}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <p className={`truncate text-[13px] font-semibold ${active ? 'text-white' : 'text-[var(--ink)]'}`}>
                          {dataset.name}
                        </p>
                        <p className={`mt-1 text-[11px] leading-5 ${active ? 'text-white/72' : 'text-[var(--muted)]'}`}>
                          {dataset.helper}
                        </p>
                      </div>
                      <ArrowRight size={14} className={active ? 'text-white/80' : 'text-[var(--muted)]'} />
                    </div>
                  </button>
                );
              })}
            </div>
          </aside>

          <section className="space-y-4">
            <article className="rounded-[16px] border border-[var(--line)] bg-[var(--surface)] p-5 shadow-[0_6px_20px_rgba(17,19,24,0.04)]">
              <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
                <div className="max-w-3xl">
                  <div className="inline-flex items-center gap-2 rounded-[8px] border border-[var(--line)] bg-[var(--surface-2)] px-3 py-1.5 text-[10px] font-medium uppercase tracking-[0.2em] text-[var(--muted)]">
                    <Layers3 size={12} />
                    {selectedGuide.shortLabel}
                  </div>
                  <h2 className="mt-4 font-display text-[2rem] font-semibold tracking-tight text-[var(--ink)]">
                    {selectedGuide.name}
                  </h2>
                  <p className="mt-3 text-[14px] leading-7 text-[var(--ink-soft)]">{selectedGuide.description}</p>
                  <p className="mt-4 text-[13px] leading-6 text-[var(--muted)]">{selectedGuide.helper}</p>
                </div>

                <button
                  type="button"
                  onClick={() => onUseInChat(selectedGuide.id)}
                  className="inline-flex items-center gap-2 self-start rounded-[10px] border border-[var(--ink)] bg-[var(--ink)] px-4 py-2 text-[12px] font-medium text-white transition hover:opacity-92"
                >
                  Ask about this dataset
                  <ArrowRight size={13} />
                </button>
              </div>

              <div className="mt-5 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(280px,0.85fr)]">
                <div className="rounded-[12px] border border-[var(--line)] bg-[var(--surface-2)]/50 p-4">
                  <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">
                    Good for questions like
                  </p>
                  <div className="mt-3 grid gap-2">
                    {selectedGuide.starterQuestions.slice(0, 4).map((question) => (
                      <div
                        key={question}
                        className="rounded-[10px] border border-[var(--line-soft)] bg-white px-3 py-2.5 text-[12px] leading-6 text-[var(--ink)]"
                      >
                        {question}
                      </div>
                    ))}
                  </div>
                </div>

                <div className="rounded-[12px] border border-[var(--line)] bg-white p-4">
                  <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">
                    What you’re downloading
                  </p>
                  <div className="mt-3 space-y-2.5 text-[12px] leading-6 text-[var(--ink-soft)]">
                    <p>The cards below show the actual files exposed by this app for this dataset family.</p>
                    <p>Each table includes its grain, approximate row count, a short summary, and the main columns you should expect.</p>
                    <p>If a family is analysis-only, we say that directly instead of pretending there is a single export file.</p>
                  </div>
                </div>
              </div>
            </article>

            {selectedCatalog?.notes?.length ? (
              <section className="rounded-[14px] border border-[var(--line)] bg-[var(--surface)] p-4 shadow-[0_6px_18px_rgba(17,19,24,0.035)]">
                <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">Notes</p>
                <div className="mt-3 space-y-2">
                  {selectedCatalog.notes.map((note) => (
                    <p key={note} className="text-[12px] leading-6 text-[var(--muted)]">
                      {note}
                    </p>
                  ))}
                </div>
              </section>
            ) : null}

            {selectedCatalog?.tables?.length ? (
              <div className="grid gap-4 xl:grid-cols-2">
                {selectedCatalog.tables.map((table) => (
                  <article key={table.tableName} className="rounded-[14px] border border-[var(--line)] bg-[var(--surface)] p-4 shadow-[0_6px_18px_rgba(17,19,24,0.035)]">
                    <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <Database size={14} className="text-[var(--muted)]" />
                          <h3 className="text-[15px] font-semibold text-[var(--ink)]">{table.label}</h3>
                        </div>
                        <p className="mt-2 text-[12px] leading-6 text-[var(--muted)]">{table.summary}</p>
                      </div>

                      <div className="flex shrink-0 flex-wrap gap-2">
                        {table.downloads.parquet ? (
                          <a
                            href={buildApiUrl(table.downloads.parquet)}
                            className="inline-flex items-center gap-1.5 rounded-[8px] border border-[var(--line)] bg-white px-3 py-1.5 text-[11px] font-medium text-[var(--ink)] transition hover:bg-[var(--surface-2)]"
                          >
                            <Download size={11} />
                            Parquet
                          </a>
                        ) : null}
                        {table.downloads.xlsx ? (
                          <a
                            href={buildApiUrl(table.downloads.xlsx)}
                            className="inline-flex items-center gap-1.5 rounded-[8px] border border-[var(--line)] bg-white px-3 py-1.5 text-[11px] font-medium text-[var(--ink)] transition hover:bg-[var(--surface-2)]"
                          >
                            <FileSpreadsheet size={11} />
                            XLSX
                          </a>
                        ) : null}
                      </div>
                    </div>

                    <div className="mt-4 grid gap-3 sm:grid-cols-2">
                      <div className="rounded-[10px] bg-[var(--surface-2)] px-3 py-2.5">
                        <p className="text-[10px] font-medium uppercase tracking-[0.18em] text-[var(--muted)]">Grain</p>
                        <p className="mt-2 text-[12px] font-medium text-[var(--ink)]">{table.grain}</p>
                      </div>
                      <div className="rounded-[10px] bg-[var(--surface-2)] px-3 py-2.5">
                        <p className="text-[10px] font-medium uppercase tracking-[0.18em] text-[var(--muted)]">Rows</p>
                        <p className="mt-2 text-[12px] font-medium text-[var(--ink)]">{formatNumber(table.rows)}</p>
                      </div>
                    </div>

                    <div className="mt-4">
                      <p className="text-[11px] font-medium uppercase tracking-[0.22em] text-[var(--muted)]">Columns included</p>
                      <div className="mt-3 flex flex-wrap gap-1.5">
                        {table.columns.slice(0, 12).map((column) => (
                          <span
                            key={column}
                            className="rounded-[8px] border border-[var(--line)] bg-[var(--surface-2)]/45 px-2.5 py-1 text-[10px] font-medium text-[var(--muted)]"
                          >
                            {column}
                          </span>
                        ))}
                        {table.columns.length > 12 ? (
                          <span className="rounded-[8px] border border-[var(--line)] bg-[var(--surface-2)]/45 px-2.5 py-1 text-[10px] font-medium text-[var(--muted)]">
                            +{table.columns.length - 12} more
                          </span>
                        ) : null}
                      </div>
                    </div>
                  </article>
                ))}
              </div>
            ) : (
              <section className="rounded-[14px] border border-[var(--line)] bg-[var(--surface)] p-5 text-[13px] leading-7 text-[var(--muted)] shadow-[0_6px_18px_rgba(17,19,24,0.035)]">
                This analysis mode combines multiple downloadable families rather than exposing one standalone export file.
                Use the adjacent dataset families for the underlying data downloads.
              </section>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}
