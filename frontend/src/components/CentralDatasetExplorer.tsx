import { Database, Download, FileSpreadsheet } from 'lucide-react';
import { buildApiUrl } from '@/lib/api';
import type { DatasetGuide } from '@/lib/content';
import type { DatasetCatalogEntry } from '@/types/chat';


interface CentralDatasetExplorerProps {
  datasets: DatasetGuide[];
  datasetCatalog: DatasetCatalogEntry[];
  selectedDatasetId: string;
  onSelectDataset: (id: string) => void;
}

function formatRows(rows: number): string {
  return rows.toLocaleString();
}

export function CentralDatasetExplorer({
  datasets,
  datasetCatalog,
  selectedDatasetId,
  onSelectDataset,
}: CentralDatasetExplorerProps) {
  const selectedGuide = datasets.find((entry) => entry.id === selectedDatasetId) ?? datasets[0];
  const selectedCatalog = datasetCatalog.find((entry) => entry.id === selectedGuide.id);

  return (
    <section className="mt-8 rounded-[28px] border border-[var(--line)] bg-[var(--surface)]/70 p-5 shadow-sm sm:p-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-[var(--muted)]">
            Dataset Library
          </p>
          <h2 className="mt-2 font-display text-2xl font-semibold tracking-tight text-[var(--ink)]">
            Browse the data before you ask
          </h2>
          <p className="mt-2 max-w-2xl text-[13px] leading-6 text-[var(--muted)]">
            Pick a dataset family, skim the tables and columns, and download the exact files behind the chatbot.
          </p>
        </div>
      </div>

      <div className="mt-5 flex flex-wrap gap-2">
        {datasets.map((dataset) => (
          <button
            key={dataset.id}
            type="button"
            onClick={() => onSelectDataset(dataset.id)}
            className={`rounded-full border px-3 py-1.5 text-[11px] font-medium transition ${
              dataset.id === selectedDatasetId
                ? 'border-[var(--ink)] bg-[var(--ink)] text-white'
                : 'border-[var(--line)] bg-white text-[var(--muted)] hover:text-[var(--ink)]'
            }`}
          >
            {dataset.name}
          </button>
        ))}
      </div>

      <div className="mt-6 grid gap-5 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1.4fr)]">
        <div className="rounded-2xl border border-[var(--line)] bg-white/80 p-4">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h3 className="text-[16px] font-semibold text-[var(--ink)]">{selectedGuide.name}</h3>
              <p className="mt-2 text-[13px] leading-6 text-[var(--muted)]">{selectedGuide.description}</p>
            </div>
            <span className="rounded-full bg-[var(--surface-2)] px-2 py-1 text-[10px] font-medium uppercase tracking-wide text-[var(--muted)]">
              {selectedGuide.shortLabel}
            </span>
          </div>

          <p className="mt-4 text-[12px] leading-6 text-[var(--ink)]">{selectedGuide.helper}</p>

          <div className="mt-5">
            <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-[var(--muted)]">
              Starter questions
            </p>
            <div className="mt-3 space-y-2">
              {selectedGuide.starterQuestions.slice(0, 4).map((question) => (
                <div
                  key={question}
                  className="rounded-2xl border border-[var(--line)] bg-[var(--surface)] px-3 py-2 text-[12px] leading-6 text-[var(--ink)]"
                >
                  {question}
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="space-y-3">
          {selectedCatalog?.tables?.length ? (
            selectedCatalog.tables.map((table) => (
              <article key={table.tableName} className="rounded-2xl border border-[var(--line)] bg-white/80 p-4">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <Database size={14} className="text-[var(--muted)]" />
                      <h4 className="text-[14px] font-semibold text-[var(--ink)]">{table.label}</h4>
                    </div>
                    <p className="mt-2 text-[12px] leading-6 text-[var(--muted)]">{table.summary}</p>
                    <p className="mt-1 text-[11px] text-[var(--muted)]">
                      {table.grain} · {formatRows(table.rows)} rows
                    </p>
                  </div>

                  <div className="flex shrink-0 gap-2">
                    {table.downloads.parquet && (
                      <a
                        href={buildApiUrl(table.downloads.parquet)}
                        className="inline-flex items-center gap-1 rounded-full border border-[var(--line)] px-3 py-1.5 text-[11px] font-medium text-[var(--ink)] transition hover:bg-[var(--surface)]"
                      >
                        <Download size={11} />
                        Parquet
                      </a>
                    )}
                    {table.downloads.xlsx && (
                      <a
                        href={buildApiUrl(table.downloads.xlsx)}
                        className="inline-flex items-center gap-1 rounded-full border border-[var(--line)] px-3 py-1.5 text-[11px] font-medium text-[var(--ink)] transition hover:bg-[var(--surface)]"
                      >
                        <FileSpreadsheet size={11} />
                        XLSX
                      </a>
                    )}
                  </div>
                </div>

                <div className="mt-4 flex flex-wrap gap-1.5">
                  {table.columns.slice(0, 14).map((column) => (
                    <span
                      key={column}
                      className="rounded-full bg-[var(--surface)] px-2.5 py-1 text-[10px] font-medium text-[var(--muted)]"
                    >
                      {column}
                    </span>
                  ))}
                  {table.columns.length > 14 && (
                    <span className="rounded-full bg-[var(--surface)] px-2.5 py-1 text-[10px] font-medium text-[var(--muted)]">
                      +{table.columns.length - 14} more
                    </span>
                  )}
                </div>
              </article>
            ))
          ) : (
            <div className="rounded-2xl border border-[var(--line)] bg-white/80 p-4 text-[12px] leading-6 text-[var(--muted)]">
              This analysis mode combines multiple families rather than exposing one single file. Use the related dataset families above to download the underlying data.
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
