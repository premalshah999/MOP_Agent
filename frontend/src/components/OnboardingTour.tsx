import { useEffect, useState } from 'react';
import { Lightbulb, MessagesSquare, ScrollText } from 'lucide-react';

const STORAGE_KEY = 'mop-onboarding-seen-v1';

interface Step {
  icon: React.ReactNode;
  title: string;
  body: string;
}

const STEPS: Step[] = [
  {
    icon: <MessagesSquare size={22} className="text-[var(--ink)]" />,
    title: 'Ask questions grounded in the catalog',
    body:
      'Type a question — or click one of the starter chips. The agent picks the right table, writes the SQL, runs it, and writes a grounded answer. No SQL knowledge required.',
  },
  {
    icon: <ScrollText size={22} className="text-[var(--ink)]" />,
    title: 'Inspect the evidence behind answers',
    body:
      'You see the headline number, the prose, a Notes footer with caveats (year, methodology), and one-click access to the underlying SQL + raw rows. Charts and maps render inline when they help.',
  },
  {
    icon: <Lightbulb size={22} className="text-[var(--ink)]" />,
    title: 'Extended analysis for deeper questions',
    body:
      'Tap the lightbulb in the composer for cross-dataset comparisons, peer rankings, and distributional questions. The assistant can run additional evidence checks before answering.',
  },
];

export function OnboardingTour() {
  const [open, setOpen] = useState(false);
  const [step, setStep] = useState(0);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      if (!window.localStorage.getItem(STORAGE_KEY)) {
        // Show after a tiny delay so the first frame paints first.
        const t = window.setTimeout(() => setOpen(true), 250);
        return () => window.clearTimeout(t);
      }
    } catch {
      // Private browsing / SecurityError — skip onboarding rather than crash.
    }
  }, []);

  const dismiss = () => {
    try {
      window.localStorage.setItem(STORAGE_KEY, '1');
    } catch {
      /* ignore */
    }
    setOpen(false);
  };

  const advance = () => {
    if (step < STEPS.length - 1) {
      setStep(step + 1);
    } else {
      dismiss();
    }
  };

  if (!open) return null;

  const current = STEPS[step];
  const isLast = step === STEPS.length - 1;

  return (
    <div
      className="fixed inset-0 z-[200] flex items-center justify-center bg-black/30 px-4 backdrop-blur-sm"
      onClick={dismiss}
      role="dialog"
      aria-modal="true"
      aria-label="Welcome tour"
    >
      <div
        className="w-full max-w-md rounded-[10px] border border-[var(--line)] bg-[var(--bg)] p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start gap-3">
          <div className="rounded-[8px] border border-[var(--line)] bg-[var(--surface)] p-2.5">
            {current.icon}
          </div>
          <div className="min-w-0 flex-1">
            <div className="text-[10px] font-medium uppercase tracking-[0.14em] text-[var(--muted)]">
              Step {step + 1} of {STEPS.length}
            </div>
            <h2 className="mt-0.5 text-[16px] font-semibold leading-snug text-[var(--ink)]">
              {current.title}
            </h2>
          </div>
        </div>

        <p className="mt-3 text-[13px] leading-6 text-[var(--muted)]">{current.body}</p>

        {/* Step dots */}
        <div className="mt-5 flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            {STEPS.map((_, i) => (
              <span
                key={i}
                className={`h-1.5 w-1.5 rounded-full transition ${
                  i === step ? 'bg-[var(--ink)]' : 'bg-[var(--muted-2)] opacity-50'
                }`}
              />
            ))}
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={dismiss}
              className="text-[12px] text-[var(--muted)] hover:text-[var(--ink)]"
            >
              Skip
            </button>
            <button
              type="button"
              onClick={advance}
              className="rounded-md bg-[var(--ink)] px-3 py-1.5 text-[12px] font-medium text-white hover:opacity-90"
            >
              {isLast ? 'Get started' : 'Next'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
