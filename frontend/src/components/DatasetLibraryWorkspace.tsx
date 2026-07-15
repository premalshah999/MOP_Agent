import { ArrowUpRight, Download } from 'lucide-react';
import { useMemo, useState } from 'react';
import { buildApiUrl } from '@/lib/api';
import type { DatasetGuide } from '@/lib/content';
import type { DatasetCatalogEntry } from '@/types/chat';

/* Data library — deliberately quiet. One list of dataset families on the
   left, the selected family's tables on the right. No stat walls, no
   marketing copy: name, what it's good for, the files, a download. */

interface DatasetLibraryWorkspaceProps {
  datasets: DatasetGuide[];
  datasetCatalog: DatasetCatalogEntry[];
  selectedDatasetId: string;
  onSelectDataset: (id: string) => void;
  onUseInChat: (id: string, question?: string) => void;
}

function compactRows(rows: number): string {
  if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(1)}M rows`;
  if (rows >= 1_000) return `${Math.round(rows / 1_000)}k rows`;
  return `${rows.toLocaleString()} rows`;
}

/* Guide families (UI grouping) and catalog entries (file grouping) use
   different ids — map explicitly. fund_flow spans three catalog entries;
   cross_dataset is analysis-only and ships no files. */
const GUIDE_TO_CATALOG: Record<string, string[]> = {
  government_finance: ['gov'],
  acs: ['acs'],
  federal_spending: ['contract'],
  federal_spending_agency: ['spending'],
  finra: ['finra'],
  fund_flow: ['state', 'county', 'congress'],
  cross_dataset: [],
};

export function DatasetLibraryWorkspace({
  datasets,
  datasetCatalog,
  selectedDatasetId,
  onSelectDataset,
  onUseInChat,
}: DatasetLibraryWorkspaceProps) {
  const [activeId, setActiveId] = useState(selectedDatasetId);
  const guide = datasets.find((d) => d.id === activeId) ?? datasets[0];
  const tables = useMemo(() => {
    const catalogIds = GUIDE_TO_CATALOG[guide.id] ?? [guide.id];
    return catalogIds.flatMap((cid) => datasetCatalog.find((c) => c.id === cid)?.tables ?? []);
  }, [guide.id, datasetCatalog]);

  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto w-full max-w-5xl px-5 py-8 lg:px-8">
        <h1 className="font-display text-[26px] font-medium tracking-tight text-[var(--ink)]">Data library</h1>
        <p className="mt-1.5 max-w-xl text-[13.5px] leading-6 text-[var(--muted)]">
          The datasets behind analytical answers. Browse a family, download the exact files, or take a question straight to the chat.
        </p>

        <div className="mt-7 grid gap-8 lg:grid-cols-[240px_minmax(0,1fr)]">
          {/* Families */}
          <nav className="flex flex-row flex-wrap gap-1 lg:flex-col lg:gap-0.5">
            {datasets.map((d) => {
              const active = d.id === guide.id;
              return (
                <button
                  key={d.id}
                  type="button"
                  onClick={() => { setActiveId(d.id); onSelectDataset(d.id); }}
                  className={`rounded-lg px-3 py-2 text-left text-[13.5px] transition ${
                    active
                      ? 'bg-[var(--surface-2)] font-medium text-[var(--ink)]'
                      : 'text-[var(--muted)] hover:bg-[var(--surface-2)]/60 hover:text-[var(--ink)]'
                  }`}
                >
                  {d.name}
                </button>
              );
            })}
          </nav>

          {/* Selected family */}
          <section className="min-w-0">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0">
                <h2 className="font-display text-[20px] font-medium text-[var(--ink)]">{guide.name}</h2>
                <p className="mt-1 max-w-lg text-[13px] leading-6 text-[var(--muted)]">{guide.helper}</p>
              </div>
              <button
                type="button"
                onClick={() => onUseInChat(guide.id)}
                className="inline-flex shrink-0 items-center gap-1.5 rounded-full bg-[var(--accent)] px-3.5 py-1.5 text-[12.5px] font-medium text-white transition hover:bg-[var(--accent-hover)]"
              >
                Ask about this data
                <ArrowUpRight size={13} />
              </button>
            </div>

            {/* Tables */}
            <div className="mt-5 overflow-hidden rounded-2xl border border-[var(--line-soft)] bg-[var(--surface)]">
              {tables.length === 0 ? (
                <div className="px-5 py-6 text-[13px] leading-6 text-[var(--muted)]">
                  This family is analysis-only — it queries across every other dataset rather than shipping files of its own.
                </div>
              ) : (
                tables.map((t, i) => (
                  <div
                    key={t.tableName}
                    className={`flex items-center gap-4 px-5 py-3.5 ${i > 0 ? 'border-t border-[var(--line-soft)]' : ''}`}
                  >
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-baseline gap-2">
                        <span className="text-[13.5px] font-medium text-[var(--ink)]">{t.label}</span>
                        <span className="text-[11px] text-[var(--muted-2)]">{t.grain}</span>
                      </div>
                      <p className="mt-0.5 truncate text-[12px] leading-5 text-[var(--muted)]">{t.summary}</p>
                    </div>
                    <span className="tabular-nums shrink-0 text-[11.5px] text-[var(--muted-2)]">{compactRows(t.rows)}</span>
                    {(t.downloads.parquet || t.downloads.xlsx) && (
                      <a
                        href={buildApiUrl(t.downloads.xlsx || t.downloads.parquet || '')}
                        download
                        title={`Download ${t.label}`}
                        className="shrink-0 rounded-lg p-2 text-[var(--muted)] transition hover:bg-[var(--surface-2)] hover:text-[var(--ink)]"
                      >
                        <Download size={15} />
                      </a>
                    )}
                  </div>
                ))
              )}
            </div>

            {/* Example questions */}
            {guide.starterQuestions.length > 0 && (
              <div className="mt-5">
                <div className="text-[11px] font-medium text-[var(--muted-2)]">Try asking</div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {guide.starterQuestions.slice(0, 3).map((q, i) => (
                    <button
                      key={`${guide.id}-${i}`}
                      type="button"
                      onClick={() => onUseInChat(guide.id, q)}
                      className="rounded-full border border-[var(--line)] bg-[var(--surface)] px-3.5 py-1.5 text-left text-[12.5px] text-[var(--muted)] transition hover:border-[var(--muted-2)] hover:text-[var(--ink)]"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}
