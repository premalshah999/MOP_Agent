import { ArrowUpRight, ChevronRight, Download, Search } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
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
  const [activeTableId, setActiveTableId] = useState('');
  const [dictionarySearch, setDictionarySearch] = useState('');
  const [variableRole, setVariableRole] = useState<'all' | 'measure' | 'dimension'>('all');
  const guide = datasets.find((d) => d.id === activeId) ?? datasets[0];
  const tables = useMemo(() => {
    const catalogIds = GUIDE_TO_CATALOG[guide.id] ?? [guide.id];
    return catalogIds.flatMap((cid) => datasetCatalog.find((c) => c.id === cid)?.tables ?? []);
  }, [guide.id, datasetCatalog]);
  const activeTable = tables.find((table) => table.tableName === activeTableId) ?? tables[0];
  const activeVariables = activeTable?.variables ?? [];
  const filteredVariables = useMemo(() => {
    if (!activeTable) return [];
    const needle = dictionarySearch.trim().toLocaleLowerCase();
    return activeVariables.filter((variable) => {
      if (variableRole !== 'all' && variable.role !== variableRole) return false;
      if (!needle) return true;
      return [
        variable.name,
        variable.label,
        variable.description,
        variable.unit ?? '',
        ...(variable.synonyms ?? []),
      ].some((value) => value.toLocaleLowerCase().includes(needle));
    });
  }, [activeTable, activeVariables, dictionarySearch, variableRole]);

  useEffect(() => {
    if (tables.length > 0 && !tables.some((table) => table.tableName === activeTableId)) {
      setActiveTableId(tables[0].tableName);
    }
    setDictionarySearch('');
    setVariableRole('all');
  }, [guide.id, tables, activeTableId]);

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
                    className={`flex items-center gap-2 ${i > 0 ? 'border-t border-[var(--line-soft)]' : ''} ${activeTable?.tableName === t.tableName ? 'bg-[var(--surface-2)]/60' : ''}`}
                  >
                    <button
                      type="button"
                      onClick={() => { setActiveTableId(t.tableName); setDictionarySearch(''); }}
                      className="flex min-w-0 flex-1 items-center gap-4 px-5 py-3.5 text-left"
                      aria-pressed={activeTable?.tableName === t.tableName}
                    >
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-baseline gap-2">
                          <span className="text-[13.5px] font-medium text-[var(--ink)]">{t.label}</span>
                          <span className="text-[11px] text-[var(--muted-2)]">{t.grain}</span>
                        </div>
                        <p className="mt-0.5 truncate text-[12px] leading-5 text-[var(--muted)]">{t.summary}</p>
                      </div>
                      <span className="tabular-nums hidden shrink-0 text-[11.5px] text-[var(--muted-2)] sm:block">{compactRows(t.rows)}</span>
                      <ChevronRight size={14} className="shrink-0 text-[var(--muted-2)]" />
                    </button>
                    {(t.downloads.parquet || t.downloads.xlsx) && (
                      <a
                        href={buildApiUrl(t.downloads.xlsx || t.downloads.parquet || '')}
                        download
                        title={`Download ${t.label}`}
                        className="mr-3 shrink-0 rounded-lg p-2 text-[var(--muted)] transition hover:bg-[var(--surface)] hover:text-[var(--ink)]"
                      >
                        <Download size={15} />
                      </a>
                    )}
                  </div>
                ))
              )}
            </div>

            {/* Variable dictionary */}
            {activeTable && (
              <section className="mt-5 overflow-hidden rounded-2xl border border-[var(--line-soft)] bg-[var(--surface)]">
                <div className="border-b border-[var(--line-soft)] px-5 py-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <h3 className="text-[14px] font-semibold text-[var(--ink)]">Data dictionary</h3>
                      <p className="mt-1 text-[12px] leading-5 text-[var(--muted)]">
                        {activeVariables.length} documented variables in {activeTable.label}
                      </p>
                    </div>
                    <div className="text-right text-[11px] leading-5 text-[var(--muted-2)]">
                      {activeTable.source && <div>Source: {activeTable.source}</div>}
                      {activeTable.periodLabel && <div>Coverage: {activeTable.periodLabel}</div>}
                    </div>
                  </div>

                  <div className="mt-3 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                    <div className="flex gap-1 rounded-lg bg-[var(--surface-2)] p-0.5">
                      {(['all', 'measure', 'dimension'] as const).map((role) => (
                        <button
                          key={role}
                          type="button"
                          onClick={() => setVariableRole(role)}
                          className={`rounded-md px-2.5 py-1 text-[11.5px] capitalize transition ${
                            variableRole === role ? 'bg-[var(--surface)] font-medium text-[var(--ink)] shadow-sm' : 'text-[var(--muted)] hover:text-[var(--ink)]'
                          }`}
                        >
                          {role === 'all' ? 'All variables' : `${role}s`}
                        </button>
                      ))}
                    </div>
                    <label className="relative block sm:w-64">
                      <Search size={13} className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-[var(--muted-2)]" />
                      <span className="sr-only">Search variables</span>
                      <input
                        value={dictionarySearch}
                        onChange={(event) => setDictionarySearch(event.target.value)}
                        placeholder="Search variables and terms"
                        className="w-full rounded-lg border border-[var(--line)] bg-[var(--bg)] py-1.5 pl-8 pr-3 text-[12px] text-[var(--ink)] outline-none transition placeholder:text-[var(--muted-2)] focus:border-[var(--accent)]"
                      />
                    </label>
                  </div>
                </div>

                <div className="max-h-[560px] overflow-y-auto">
                  {filteredVariables.length > 0 ? filteredVariables.map((variable, index) => (
                    <article
                      key={variable.name}
                      className={`px-5 py-4 ${index > 0 ? 'border-t border-[var(--line-soft)]' : ''}`}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <h4 className="text-[13px] font-medium text-[var(--ink)]">{variable.label}</h4>
                            <span className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${
                              variable.role === 'measure'
                                ? 'bg-[var(--accent-soft)] text-[var(--accent)]'
                                : 'bg-[var(--surface-2)] text-[var(--muted)]'
                            }`}>
                              {variable.role}
                            </span>
                          </div>
                          <code className="mt-1 block break-all text-[10.5px] text-[var(--muted-2)]">{variable.name}</code>
                          <p className="mt-2 max-w-2xl text-[12.5px] leading-5 text-[var(--muted)]">{variable.description}</p>
                          <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[10.5px] text-[var(--muted-2)]">
                            {variable.dataType && <span>Type: {variable.dataType}</span>}
                            {variable.unit && <span>Unit: {variable.unit}</span>}
                            {variable.aggregation && <span>Default summary: {variable.aggregation}</span>}
                            {variable.sampleValues && variable.sampleValues.length > 0 && (
                              <span>Examples: {variable.sampleValues.slice(0, 3).join(', ')}</span>
                            )}
                          </div>
                          {variable.synonyms && variable.synonyms.length > 1 && (
                            <p className="mt-1.5 text-[10.5px] text-[var(--muted-2)]">
                              Also understood as: {variable.synonyms.slice(0, 4).join(', ')}
                            </p>
                          )}
                        </div>
                        {variable.exampleQuestion && (
                          <button
                            type="button"
                            onClick={() => onUseInChat(guide.id, variable.exampleQuestion)}
                            className="inline-flex shrink-0 items-center gap-1 rounded-full border border-[var(--line)] px-2.5 py-1 text-[11px] font-medium text-[var(--muted)] transition hover:border-[var(--accent)] hover:text-[var(--accent)]"
                          >
                            Ask about it
                            <ArrowUpRight size={11} />
                          </button>
                        )}
                      </div>
                    </article>
                  )) : (
                    <div className="px-5 py-8 text-center">
                      <p className="text-[13px] font-medium text-[var(--ink)]">No exact variable match</p>
                      <p className="mx-auto mt-1 max-w-sm text-[12px] leading-5 text-[var(--muted)]">
                        The assistant can find the closest supported concept and guide you to a question this data can answer.
                      </p>
                      <button
                        type="button"
                        onClick={() => onUseInChat(guide.id, `I couldn't find "${dictionarySearch.trim()}" in this dataset. What is the closest measure I can use?`)}
                        className="mt-3 inline-flex items-center gap-1.5 rounded-full bg-[var(--accent)] px-3.5 py-1.5 text-[11.5px] font-medium text-white transition hover:bg-[var(--accent-hover)]"
                      >
                        Ask for the closest measure
                        <ArrowUpRight size={12} />
                      </button>
                    </div>
                  )}
                </div>
              </section>
            )}

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
