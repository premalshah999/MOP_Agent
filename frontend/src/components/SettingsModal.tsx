import { Check, Download, Loader2, Trash2, X } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useAuth } from '@/hooks/useAuth';
import { apiListThreads, apiUpdateProfile } from '@/lib/api';
import { updateSettings, useSettings, type AppSettings } from '@/lib/settings';

interface SettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  /** Called after "Clear all chats" so the thread store can refresh. */
  onThreadsCleared?: () => void;
}

export function SettingsModal({ isOpen, onClose, onThreadsCleared }: SettingsModalProps) {
  const { user, refreshUser } = useAuth();
  const settings = useSettings();
  const [name, setName] = useState(user?.name ?? '');
  const [nameState, setNameState] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [clearState, setClearState] = useState<'idle' | 'confirm' | 'working' | 'done'>('idle');

  useEffect(() => { setName(user?.name ?? ''); }, [user?.name]);
  useEffect(() => {
    if (!isOpen) return undefined;
    setNameState('idle'); setClearState('idle');
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const saveName = async () => {
    if (!name.trim() || name.trim() === user?.name) return;
    setNameState('saving');
    try {
      await apiUpdateProfile({ name: name.trim() });
      await refreshUser();
      setNameState('saved');
      setTimeout(() => setNameState('idle'), 1600);
    } catch {
      setNameState('error');
    }
  };

  const exportChats = async () => {
    try {
      const threads = await apiListThreads();
      const blob = new Blob([JSON.stringify(threads, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `mop-agent-conversations-${new Date().toISOString().slice(0, 10)}.json`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      console.warn('[settings] export failed', e);
    }
  };

  const clearChats = async () => {
    if (clearState === 'idle') { setClearState('confirm'); return; }
    if (clearState !== 'confirm') return;
    setClearState('working');
    onThreadsCleared?.(); // thread store clears server + local state
    setClearState('done');
    setTimeout(() => setClearState('idle'), 1600);
  };

  return (
    <div className="fixed inset-0 z-[130] grid place-items-center bg-[#1f1e1d]/30 p-4 backdrop-blur-[2px]">
      <div className="absolute inset-0" onClick={onClose} />
      <div className="relative flex max-h-[85vh] w-full max-w-lg flex-col overflow-hidden rounded-2xl bg-[var(--surface)] shadow-[0_24px_64px_rgba(31,30,29,0.2)]">
        <header className="flex items-center justify-between px-6 pb-2 pt-5">
          <h2 className="font-display text-[19px] font-medium text-[var(--ink)]">Settings</h2>
          <button type="button" onClick={onClose} aria-label="Close settings" className="rounded-lg p-1.5 text-[var(--muted)] transition hover:bg-[var(--surface-2)] hover:text-[var(--ink)]">
            <X size={16} />
          </button>
        </header>

        <div className="min-h-0 flex-1 space-y-6 overflow-y-auto px-6 pb-6 pt-2">
          {/* Account */}
          <section>
            <SectionLabel>Account</SectionLabel>
            <div className="mt-2 space-y-3 rounded-xl border border-[var(--line-soft)] p-4">
              <div>
                <label htmlFor="settings-name" className="text-[12px] font-medium text-[var(--muted)]">Display name</label>
                <div className="mt-1 flex gap-2">
                  <input
                    id="settings-name"
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    maxLength={80}
                    className="min-w-0 flex-1 rounded-lg border border-[var(--line)] bg-[var(--surface)] px-3 py-1.5 text-[13.5px] text-[var(--ink)] outline-none focus:border-[var(--muted-2)]"
                  />
                  <button
                    type="button"
                    onClick={() => void saveName()}
                    disabled={!name.trim() || name.trim() === user?.name || nameState === 'saving'}
                    className="inline-flex items-center gap-1.5 rounded-lg bg-[var(--accent)] px-3 py-1.5 text-[12.5px] font-medium text-white transition hover:bg-[var(--accent-hover)] disabled:bg-[var(--surface-2)] disabled:text-[var(--muted-2)]"
                  >
                    {nameState === 'saving' ? <Loader2 size={13} className="animate-spin" /> : nameState === 'saved' ? <Check size={13} /> : null}
                    {nameState === 'saved' ? 'Saved' : 'Save'}
                  </button>
                </div>
                {nameState === 'error' && <p className="mt-1 text-[11.5px] text-[var(--danger)]">Couldn't save — try again.</p>}
              </div>
              <div className="text-[12px] text-[var(--muted)]">
                Signed in as <span className="font-medium text-[var(--ink)]">{user?.email}</span>
              </div>
            </div>
          </section>

          {/* Chat preferences */}
          <section>
            <SectionLabel>Chat</SectionLabel>
            <div className="mt-2 divide-y divide-[var(--line-soft)] rounded-xl border border-[var(--line-soft)]">
              <div className="flex items-center justify-between gap-4 p-4">
                <div>
                  <div className="text-[13.5px] font-medium text-[var(--ink)]">Answer text size</div>
                  <div className="mt-0.5 text-[12px] text-[var(--muted)]">How large the assistant's replies render.</div>
                </div>
                <div className="flex rounded-full bg-[var(--surface-2)] p-0.5">
                  {(['compact', 'default', 'large'] as const).map((size) => (
                    <button
                      key={size}
                      type="button"
                      onClick={() => updateSettings({ textSize: size })}
                      className={`rounded-full px-2.5 py-1 text-[11.5px] font-medium capitalize transition ${
                        settings.textSize === size ? 'bg-[var(--surface)] text-[var(--ink)] shadow-sm' : 'text-[var(--muted)]'
                      }`}
                    >
                      {size === 'default' ? 'Normal' : size}
                    </button>
                  ))}
                </div>
              </div>
              <Toggle
                label="Extended analysis by default"
                hint="New questions start with additional evidence checks and comparative analysis."
                value={settings.extendedByDefault}
                onChange={(v) => updateSettings({ extendedByDefault: v })}
              />
              <Toggle
                label="Suggested follow-ups"
                hint="Show tappable follow-up questions under each answer."
                value={settings.showFollowups}
                onChange={(v) => updateSettings({ showFollowups: v })}
              />
              <Toggle
                label="Term definitions"
                hint="Underline terms like ACS or FY with hover definitions."
                value={settings.glossaryTooltips}
                onChange={(v) => updateSettings({ glossaryTooltips: v })}
              />
            </div>
          </section>

          {/* Data controls */}
          <section>
            <SectionLabel>Data</SectionLabel>
            <div className="mt-2 divide-y divide-[var(--line-soft)] rounded-xl border border-[var(--line-soft)]">
              <div className="flex items-center justify-between gap-4 p-4">
                <div>
                  <div className="text-[13.5px] font-medium text-[var(--ink)]">Export conversations</div>
                  <div className="mt-0.5 text-[12px] text-[var(--muted)]">Download every chat as JSON.</div>
                </div>
                <button
                  type="button"
                  onClick={() => void exportChats()}
                  className="inline-flex items-center gap-1.5 rounded-lg border border-[var(--line)] px-3 py-1.5 text-[12.5px] font-medium text-[var(--ink)] transition hover:bg-[var(--surface-2)]"
                >
                  <Download size={13} />
                  Export
                </button>
              </div>
              <div className="flex items-center justify-between gap-4 p-4">
                <div>
                  <div className="text-[13.5px] font-medium text-[var(--ink)]">Clear all chats</div>
                  <div className="mt-0.5 text-[12px] text-[var(--muted)]">Deletes every conversation permanently.</div>
                </div>
                <button
                  type="button"
                  onClick={() => void clearChats()}
                  disabled={clearState === 'working'}
                  className={`inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-[12.5px] font-medium transition ${
                    clearState === 'confirm'
                      ? 'bg-[var(--danger)] text-white'
                      : 'border border-[var(--line)] text-[var(--danger)] hover:bg-red-50'
                  }`}
                >
                  {clearState === 'working' ? <Loader2 size={13} className="animate-spin" /> : clearState === 'done' ? <Check size={13} /> : <Trash2 size={13} />}
                  {clearState === 'confirm' ? 'Really delete?' : clearState === 'done' ? 'Cleared' : 'Clear'}
                </button>
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return <div className="text-[11px] font-semibold uppercase tracking-[0.1em] text-[var(--muted-2)]">{children}</div>;
}

function Toggle({ label, hint, value, onChange }: { label: string; hint: string; value: boolean; onChange: (v: boolean) => void }) {
  return (
    <div className="flex items-center justify-between gap-4 p-4">
      <div>
        <div className="text-[13.5px] font-medium text-[var(--ink)]">{label}</div>
        <div className="mt-0.5 text-[12px] text-[var(--muted)]">{hint}</div>
      </div>
      <button
        type="button"
        role="switch"
        aria-checked={value}
        aria-label={label}
        onClick={() => onChange(!value)}
        className={`relative h-6 w-10 shrink-0 rounded-full transition ${value ? 'bg-[var(--accent)]' : 'bg-[var(--line)]'}`}
      >
        <span className={`absolute top-0.5 h-5 w-5 rounded-full bg-white shadow transition-all ${value ? 'left-[18px]' : 'left-0.5'}`} />
      </button>
    </div>
  );
}
