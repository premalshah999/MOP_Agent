import { useEffect, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { getSharedThread, type SharedThreadPayload } from '@/lib/api';
import { Message } from './Message';
import type { ChatMessage } from '@/types/chat';

interface SharedThreadProps {
  token: string;
}

export function SharedThread({ token }: SharedThreadProps) {
  const [payload, setPayload] = useState<SharedThreadPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const p = await getSharedThread(token);
        if (!cancelled) setPayload(p);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load shared thread');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [token]);

  return (
    <div className="min-h-screen bg-[var(--bg)]">
      <header className="border-b border-[var(--line)] bg-[var(--bg)]/94 backdrop-blur">
        <div className="mx-auto flex h-12 w-full max-w-4xl items-center justify-between px-4 lg:px-6">
          <span className="text-[12px] font-semibold tracking-tight text-[var(--ink)]">MOP Agent · Shared thread</span>
          <span className="text-[11px] text-[var(--muted)]">read-only</span>
        </div>
      </header>

      <main className="mx-auto w-full max-w-4xl px-4 py-6 lg:px-6">
        {loading && (
          <div className="flex items-center gap-2 text-[13px] text-[var(--muted)]">
            <Loader2 size={14} className="animate-spin opacity-70" />
            Loading…
          </div>
        )}
        {error && (
          <div className="rounded-[8px] border border-red-200 bg-red-50 px-4 py-3 text-[13px] text-[var(--danger)]">{error}</div>
        )}
        {payload && (
          <>
            <h1 className="mb-5 text-[18px] font-semibold text-[var(--ink)]">{payload.thread.title}</h1>
            <div className="space-y-5">
              {payload.messages.map((m, i) => (
                <Message key={(m.id as string) ?? `m-${i}`} {...(m as unknown as ChatMessage)} />
              ))}
            </div>
            <footer className="mt-10 border-t border-[var(--line-soft)] pt-4 text-center text-[11px] text-[var(--muted-2)]">
              Snapshot of an MOP Agent conversation. Not interactive.
            </footer>
          </>
        )}
      </main>
    </div>
  );
}
