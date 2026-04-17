import { X } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { getMapValues } from '@/lib/api';
import type { ChatbotMapIntent } from '@/types/chat';
import { ChatbotMapRenderer } from './ChatbotMapRenderer';


interface ChatbotMapModalProps {
  isOpen: boolean;
  onClose: () => void;
  mapIntent: ChatbotMapIntent;
  fallbackRows: Record<string, unknown>[];
}

const FETCHABLE_DATASETS = new Set(['census', 'gov_spending', 'finra', 'contract_static', 'contract_agency', 'spending_breakdown']);

export function ChatbotMapModal({ isOpen, onClose, mapIntent, fallbackRows }: ChatbotMapModalProps) {
  const [rows, setRows] = useState<Record<string, unknown>[]>(fallbackRows);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const canFetch = useMemo(
    () =>
      Boolean(
        mapIntent.dataset &&
        mapIntent.level &&
        mapIntent.metric &&
        FETCHABLE_DATASETS.has(mapIntent.dataset),
      ),
    [mapIntent.dataset, mapIntent.level, mapIntent.metric],
  );

  const effectiveStateFilter = useMemo(() => {
    if (!mapIntent.state) return undefined;
    if (mapIntent.level === 'state' && mapIntent.mapType !== 'single-state-spotlight') return undefined;
    return mapIntent.state;
  }, [mapIntent.level, mapIntent.mapType, mapIntent.state]);

  useEffect(() => {
    if (!isOpen) return undefined;

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };

    document.addEventListener('keydown', onKeyDown);
    const priorOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      document.body.style.overflow = priorOverflow;
    };
  }, [isOpen, onClose]);

  useEffect(() => {
    let active = true;

    if (!isOpen) return () => { active = false; };

    setRows(fallbackRows);
    setError(null);

    if (!canFetch || !mapIntent.dataset || !mapIntent.level || !mapIntent.metric) {
      return () => { active = false; };
    }

    setLoading(true);
    void getMapValues({
      dataset: mapIntent.dataset,
      level: mapIntent.level,
      variable: mapIntent.metric,
      year: mapIntent.year,
      state: effectiveStateFilter,
      agency: mapIntent.agency,
    })
      .then((nextRows) => {
        if (!active) return;
        setRows(nextRows.length ? nextRows : fallbackRows);
      })
      .catch((err) => {
        if (!active) return;
        if (fallbackRows.length) {
          setRows(fallbackRows);
          setError(null);
          return;
        }
        setError(err instanceof Error ? err.message : 'Unable to load map data.');
      })
      .finally(() => {
        if (active) setLoading(false);
      });

    return () => {
      active = false;
    };
  }, [isOpen, canFetch, mapIntent.dataset, mapIntent.level, mapIntent.metric, mapIntent.year, mapIntent.agency, effectiveStateFilter, fallbackRows]);

  if (!isOpen || !mapIntent.enabled) return null;

  return (
    <div className="fixed inset-0 z-[120] bg-slate-950/28 backdrop-blur-[2px]">
      <div className="absolute inset-0" onClick={onClose} />
      <div className="absolute inset-3 overflow-hidden border border-[var(--line)] bg-[var(--surface)] shadow-[0_18px_48px_rgba(15,23,42,0.08)] sm:inset-5">
        <header className="flex items-start justify-between gap-4 border-b border-[var(--line)] bg-[var(--surface)] px-5 py-5 sm:px-6">
          <div className="min-w-0">
            <div className="text-[11px] uppercase tracking-[0.28em] text-[var(--muted)]">
              Maryland Opportunity Analytics Platform
            </div>
            <h2 className="mt-2 truncate font-display text-[29px] leading-none text-[var(--ink)] sm:text-[34px]">
              {mapIntent.title || 'Map View'}
            </h2>
            {mapIntent.subtitle && (
              <p className="mt-3 text-sm tracking-[0.08em] text-[var(--muted)]">{mapIntent.subtitle}</p>
            )}
            {mapIntent.reason && (
              <p className="mt-4 max-w-4xl text-[13px] leading-7 text-[var(--muted)]">{mapIntent.reason}</p>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close map view"
            className="border border-[var(--line)] bg-[var(--surface)] p-2 text-[var(--muted)] transition hover:text-[var(--ink)]"
          >
            <X size={16} />
          </button>
        </header>

        <main className="h-[calc(100%-132px)] overflow-auto bg-[var(--bg)] px-5 py-5 sm:px-6">
          <ChatbotMapRenderer mapIntent={mapIntent} rows={rows} loading={loading} error={error} />
        </main>
      </div>
    </div>
  );
}
