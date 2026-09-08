import { useState } from 'react';
import { useAuth } from '@/hooks/useAuth';

type Mode = 'login' | 'register';

export function AuthScreen() {
  const { register, login } = useAuth();
  const [mode, setMode] = useState<Mode>('login');
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (mode === 'register' && !name.trim()) return setError('Name is required');
    if (!email.trim()) return setError('Email is required');
    if (!password) return setError('Password is required');
    if (mode === 'register' && password.length < 8) return setError('Password must be at least 8 characters');

    setSubmitting(true);
    try {
      if (mode === 'register') {
        await register(name.trim(), email.trim(), password);
      } else {
        await login(email.trim(), password);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="min-h-screen bg-[var(--bg)]">
      <div className="flex h-8 items-center justify-between bg-[#101010] px-5 text-[9px] font-semibold tracking-[0.05em] text-white/55 sm:px-10">
        <span>Robert H. Smith School of Business</span>
        <span>University of Maryland</span>
      </div>
      <div className="flex min-h-[calc(100vh-2rem)] items-center justify-center px-4 py-12">
        <div className="w-full max-w-md border border-[var(--line)] bg-[var(--surface)] p-8 sm:p-10">
          <div>
            <div className="flex items-center gap-2.5">
              <span className="grid h-8 w-8 place-items-center bg-[var(--ink)] text-[13px] font-bold text-white">
                M<span className="text-[var(--brand-red)]">.</span>
              </span>
              <span className="mop-wordmark text-[15px] text-[var(--ink)]">Maryland Opportunity Project</span>
            </div>
            <div className="mop-kicker mt-9">Research data assistant</div>
            <h1 className="mt-2 font-display text-[32px] font-medium leading-tight tracking-tight text-[var(--ink)]">
              {mode === 'login' ? 'Welcome back.' : 'Create your account.'}
            </h1>
            <p className="mt-2 text-[13px] text-[var(--muted)]">
              {mode === 'login' ? 'Sign in to continue your research.' : 'Start asking questions grounded in the MOP data catalog.'}
            </p>
          </div>

          <form onSubmit={(e) => void handleSubmit(e)} className="mt-8 space-y-4">
            {mode === 'register' && (
              <div>
                <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">
                  Name
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Your name"
                  className="w-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2.5 text-[14px] text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] focus:border-[var(--brand-red)]"
                  autoComplete="name"
                  autoFocus={mode === 'register'}
                />
              </div>
            )}

            <div>
              <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">
                Email
              </label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@university.edu"
                className="w-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2.5 text-[14px] text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] focus:border-[var(--brand-red)]"
                autoComplete="email"
                autoFocus={mode === 'login'}
              />
            </div>

            <div>
              <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--muted)]">
                Password
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder={mode === 'register' ? 'At least 8 characters' : 'Your password'}
                className="w-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2.5 text-[14px] text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] focus:border-[var(--brand-red)]"
                autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
              />
            </div>

            {error && (
              <p className="text-[12px] text-[var(--danger)]">{error}</p>
            )}

            <button
              type="submit"
              disabled={submitting}
              className="mop-primary-button w-full py-3 disabled:opacity-50"
            >
              {submitting ? 'Please wait...' : mode === 'login' ? 'Sign in' : 'Create account'}
            </button>
          </form>

          <p className="mt-6 text-center text-[12px] text-[var(--muted)]">
            {mode === 'login' ? (
              <>
                Don&apos;t have an account?{' '}
                <button
                  type="button"
                  onClick={() => { setMode('register'); setError(''); }}
                  className="font-semibold text-[var(--ink)] underline decoration-[var(--brand-red)] underline-offset-4"
                >
                  Sign up
                </button>
              </>
            ) : (
              <>
                Already have an account?{' '}
                <button
                  type="button"
                  onClick={() => { setMode('login'); setError(''); }}
                  className="font-semibold text-[var(--ink)] underline decoration-[var(--brand-red)] underline-offset-4"
                >
                  Sign in
                </button>
              </>
            )}
          </p>
        </div>
      </div>
    </div>
  );
}
