import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { apiGetMe, apiLogin, apiRegister, clearToken, getToken, setToken } from '@/lib/api';
import type { UserProfile } from '@/types/chat';

interface AuthContext {
  user: UserProfile | null;
  loading: boolean;
  register: (name: string, email: string, password: string) => Promise<void>;
  login: (email: string, password: string) => Promise<void>;
  signOut: () => void;
}

const Ctx = createContext<AuthContext>({
  user: null,
  loading: true,
  register: async () => {},
  login: async () => {},
  signOut: () => {},
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);

  // On mount, check if we have a valid token
  useEffect(() => {
    const token = getToken();
    if (!token) {
      setLoading(false);
      return;
    }
    apiGetMe()
      .then((res) => setUser(res.user))
      .catch(() => { clearToken(); })
      .finally(() => setLoading(false));
  }, []);

  const register = useCallback(async (name: string, email: string, password: string) => {
    const res = await apiRegister(name, email, password);
    setToken(res.token);
    setUser(res.user);
  }, []);

  const login = useCallback(async (email: string, password: string) => {
    const res = await apiLogin(email, password);
    setToken(res.token);
    setUser(res.user);
  }, []);

  const signOut = useCallback(() => {
    clearToken();
    setUser(null);
  }, []);

  return <Ctx.Provider value={{ user, loading, register, login, signOut }}>{children}</Ctx.Provider>;
}

export function useAuth() {
  return useContext(Ctx);
}
