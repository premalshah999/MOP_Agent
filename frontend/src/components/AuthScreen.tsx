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
    if (!password || password.length < 6) return setError('Password must be at least 6 characters');

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
    <div className="flex min-h-screen items-center justify-center bg-[var(--bg)] px-4">
      <div className="w-full max-w-sm">
        <div className="text-center">
          <h1 className="font-display text-2xl font-semibold tracking-tight text-[var(--ink)]">
            Maryland Opportunity<span className="text-[var(--accent)]">.</span>
          </h1>
          <p className="mt-2 text-[13px] text-[var(--muted)]">
            {mode === 'login' ? 'Sign in to your account' : 'Create a new account'}
          </p>
        </div>

        <form onSubmit={(e) => void handleSubmit(e)} className="mt-8 space-y-4">
          {mode === 'register' && (
            <div>
              <label className="block text-[11px] font-medium uppercase tracking-wider text-[var(--muted)] mb-1.5">
                Name
              </label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Your name"
                className="w-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2.5 text-[14px] text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] focus:border-[var(--ink)]"
                autoFocus={mode === 'register'}
              />
            </div>
          )}

          <div>
            <label className="block text-[11px] font-medium uppercase tracking-wider text-[var(--muted)] mb-1.5">
              Email
            </label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="you@university.edu"
              className="w-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2.5 text-[14px] text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] focus:border-[var(--ink)]"
              autoFocus={mode === 'login'}
            />
          </div>

          <div>
            <label className="block text-[11px] font-medium uppercase tracking-wider text-[var(--muted)] mb-1.5">
              Password
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder={mode === 'register' ? 'At least 6 characters' : 'Your password'}
              className="w-full border border-[var(--line)] bg-[var(--surface)] px-3 py-2.5 text-[14px] text-[var(--ink)] outline-none placeholder:text-[var(--muted-2)] focus:border-[var(--ink)]"
            />
          </div>

          {error && (
            <p className="text-[12px] text-[var(--danger)]">{error}</p>
          )}

          <button
            type="submit"
            disabled={submitting}
            className="w-full bg-[var(--ink)] py-2.5 text-[12px] font-medium uppercase tracking-wider text-white transition hover:bg-[var(--ink-soft)] disabled:opacity-50"
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
                className="font-medium text-[var(--ink)] hover:underline"
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
                className="font-medium text-[var(--ink)] hover:underline"
              >
                Sign in
              </button>
            </>
          )}
        </p>
      </div>
    </div>
  );
}
