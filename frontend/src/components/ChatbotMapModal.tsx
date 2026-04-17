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
    <div className="fixed inset-0 z-[120] bg-slate-950/55 backdrop-blur-md">
      <div className="absolute inset-0" onClick={onClose} />
      <div className="absolute inset-3 overflow-hidden rounded-[12px] border border-black/6 bg-[var(--bg)] shadow-[0_22px_64px_rgba(15,23,42,0.18)] sm:inset-5">
        <header className="flex items-start justify-between gap-4 border-b border-black/4 bg-white/78 px-5 py-4 backdrop-blur sm:px-6">
          <div className="min-w-0">
            <h2 className="truncate text-lg font-semibold text-[var(--ink)] sm:text-xl">
              {mapIntent.title || 'Map View'}
            </h2>
            {mapIntent.subtitle && (
              <p className="mt-1 text-sm text-[var(--muted)]">{mapIntent.subtitle}</p>
            )}
            {mapIntent.reason && (
              <p className="mt-2 max-w-3xl text-[12px] leading-6 text-[var(--muted)]">{mapIntent.reason}</p>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close map view"
            className="rounded-[6px] border border-black/6 bg-white/92 p-2 text-[var(--muted)] shadow-sm transition hover:text-[var(--ink)]"
          >
            <X size={16} />
          </button>
        </header>

        <main className="h-[calc(100%-96px)] overflow-auto bg-[radial-gradient(circle_at_top_left,_rgba(248,250,252,0.97),_rgba(241,245,249,0.94))] px-5 py-5 sm:px-6">
          <ChatbotMapRenderer mapIntent={mapIntent} rows={rows} loading={loading} error={error} />
        </main>
      </div>
    </div>
  );
}
