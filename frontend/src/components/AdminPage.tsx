import { useEffect, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { getAdminFeedback, getAdminQuestions, getAdminUsage, type AdminFeedback, type AdminQuestion, type AdminUsage } from '@/lib/api';

export function AdminPage() {
  const [usage, setUsage] = useState<AdminUsage | null>(null);
  const [questions, setQuestions] = useState<AdminQuestion[]>([]);
  const [feedback, setFeedback] = useState<AdminFeedback[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [u, q, f] = await Promise.all([
          getAdminUsage(),
          getAdminQuestions(40),
          getAdminFeedback(40),
        ]);
        if (cancelled) return;
        setUsage(u);
        setQuestions(q.items);
        setFeedback(f.items);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load admin data');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    void load();
    return () => { cancelled = true; };
  }, []);

  if (loading) {
    return (
      <div className="flex flex-1 items-center justify-center text-[13px] text-[var(--muted)]">
        <Loader2 size={14} className="mr-2 animate-spin opacity-70" />
        Loading admin data…
      </div>
    );
  }
  if (error) {
    return <div className="m-8 rounded-[8px] border border-red-200 bg-red-50 px-4 py-3 text-[13px] text-[var(--danger)]">{error}</div>;
  }

  return (
    <div className="flex-1 overflow-y-auto px-6 pb-12 pt-6">
      <div className="mx-auto w-full max-w-6xl space-y-8">
        <header>
          <h1 className="text-[18px] font-semibold text-[var(--ink)]">Usage overview</h1>
          <p className="mt-1 text-[12px] text-[var(--muted)]">Recent activity, failures, and feedback. Read from the runtime jsonl logs.</p>
        </header>

        {usage && (
          <section className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <Stat label="Questions" value={usage.total_questions} />
            <Stat label="Unique users" value={usage.unique_users} />
            <Stat label="Feedback up" value={usage.feedback.up} />
            <Stat label="Feedback down" value={usage.feedback.down} />
          </section>
        )}

        {usage && Object.keys(usage.by_resolution).length > 0 && (
          <section>
            <h2 className="mb-2 text-[12px] font-medium uppercase tracking-[0.14em] text-[var(--muted)]">By resolution</h2>
            <KeyValueRow data={usage.by_resolution} />
          </section>
        )}

        {usage && Object.keys(usage.by_intent).length > 0 && (
          <section>
            <h2 className="mb-2 text-[12px] font-medium uppercase tracking-[0.14em] text-[var(--muted)]">By intent</h2>
            <KeyValueRow data={usage.by_intent} />
          </section>
        )}

        <section>
          <h2 className="mb-2 text-[12px] font-medium uppercase tracking-[0.14em] text-[var(--muted)]">Recent questions</h2>
          {questions.length === 0 ? (
            <Empty>No questions logged yet.</Empty>
          ) : (
            <div className="overflow-x-auto rounded-[8px] border border-[var(--line-soft)]">
              <table className="w-full min-w-[640px] text-[12px]">
                <thead className="bg-[var(--surface-2)] text-left text-[11px] uppercase tracking-[0.1em] text-[var(--muted)]">
                  <tr>
                    <th className="px-3 py-2 font-medium">When</th>
                    <th className="px-3 py-2 font-medium">User</th>
                    <th className="px-3 py-2 font-medium">Question</th>
                    <th className="px-3 py-2 font-medium">Intent</th>
                    <th className="px-3 py-2 font-medium">Resolution</th>
                    <th className="px-3 py-2 font-medium">Rows</th>
                  </tr>
                </thead>
                <tbody>
                  {questions.map((q, i) => (
                    <tr key={i} className="border-t border-[var(--line-soft)]">
                      <td className="whitespace-nowrap px-3 py-1.5 text-[var(--muted)]">{shortTime(q.timestamp)}</td>
                      <td className="px-3 py-1.5 text-[var(--muted)]">{q.user_id ?? '—'}</td>
                      <td className="max-w-[420px] truncate px-3 py-1.5 text-[var(--ink-soft)]">{q.question || '—'}</td>
                      <td className="px-3 py-1.5 text-[var(--muted)]">{q.intent ?? '—'}</td>
                      <td className="px-3 py-1.5"><Pill verdict={q.resolution} /></td>
                      <td className="px-3 py-1.5 text-right tabular-nums text-[var(--muted)]">{q.row_count ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section>
          <h2 className="mb-2 text-[12px] font-medium uppercase tracking-[0.14em] text-[var(--muted)]">Recent feedback</h2>
          {feedback.length === 0 ? (
            <Empty>No feedback yet.</Empty>
          ) : (
            <div className="overflow-x-auto rounded-[8px] border border-[var(--line-soft)]">
              <table className="w-full min-w-[640px] text-[12px]">
                <thead className="bg-[var(--surface-2)] text-left text-[11px] uppercase tracking-[0.1em] text-[var(--muted)]">
                  <tr>
                    <th className="px-3 py-2 font-medium">When</th>
                    <th className="px-3 py-2 font-medium">User</th>
                    <th className="px-3 py-2 font-medium">Verdict</th>
                    <th className="px-3 py-2 font-medium">Note</th>
                  </tr>
                </thead>
                <tbody>
                  {feedback.map((f, i) => (
                    <tr key={i} className="border-t border-[var(--line-soft)]">
                      <td className="whitespace-nowrap px-3 py-1.5 text-[var(--muted)]">{shortTime(f.timestamp)}</td>
                      <td className="px-3 py-1.5 text-[var(--muted)]">{f.user_email ?? '—'}</td>
                      <td className="px-3 py-1.5"><Pill verdict={f.verdict} /></td>
                      <td className="px-3 py-1.5 text-[var(--ink-soft)]">{f.note || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="rounded-[8px] border border-[var(--line-soft)] bg-[var(--surface)] px-4 py-3">
      <div className="text-[11px] uppercase tracking-[0.12em] text-[var(--muted)]">{label}</div>
      <div className="mt-1 text-[20px] font-semibold tabular-nums text-[var(--ink)]">{value}</div>
    </div>
  );
}

function KeyValueRow({ data }: { data: Record<string, number> }) {
  const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
  return (
    <div className="flex flex-wrap gap-1.5">
      {entries.map(([k, v]) => (
        <span key={k} className="rounded-full border border-[var(--line)] bg-[var(--surface)] px-3 py-1 text-[12px] text-[var(--muted)]">
          {k} <span className="tabular-nums text-[var(--ink)]">{v}</span>
        </span>
      ))}
    </div>
  );
}

function Pill({ verdict }: { verdict?: string }) {
  if (!verdict) return <span className="text-[var(--muted-2)]">—</span>;
  const danger = ['error', 'no_data', 'down', 'unsupported'].includes(verdict);
  return (
    <span className={`inline-flex rounded-full px-2 py-0.5 text-[11px] ${
      danger ? 'bg-red-50 text-[var(--danger)]' : 'bg-[var(--surface-2)] text-[var(--ink-soft)]'
    }`}>{verdict}</span>
  );
}

function Empty({ children }: { children: React.ReactNode }) {
  return <div className="rounded-[8px] border border-dashed border-[var(--line)] bg-[var(--surface)] px-4 py-6 text-center text-[12px] text-[var(--muted)]">{children}</div>;
}

function shortTime(ts?: string): string {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  } catch { return ts; }
}
