import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import { getMe, isAuthenticated, login as apiLogin, logout as apiLogout, meStore, MeResponse, register as apiRegister } from '../services/auth';

type AuthState = {
  me: MeResponse | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (name: string, email: string, password: string, companyName?: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshMe: () => Promise<void>;
};

const Ctx = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [me, setMe] = useState<MeResponse | null>(meStore.get());
  const [loading, setLoading] = useState<boolean>(true);

  const refreshMe = useCallback(async () => {
    if (!isAuthenticated()) {
      setMe(null);
      return;
    }
    try {
      const data = await getMe();
      setMe(data);
    } catch {
      setMe(null);
    }
  }, []);

  useEffect(() => {
    (async () => {
      setLoading(true);
      await refreshMe();
      setLoading(false);
    })();
  }, [refreshMe]);

  const login = useCallback(async (email: string, password: string) => {
    await apiLogin({ email, password });
    await refreshMe();
  }, [refreshMe]);

  const register = useCallback(async (name: string, email: string, password: string, companyName?: string) => {
    // /api/register لا يعيد توكن، لذلك نسجّل ثم نعمل login تلقائيًا
    await apiRegister({ name, company_name: companyName || null, email, password, password_confirmation: password });
    await apiLogin({ email, password });
    await refreshMe();
  }, [refreshMe]);

  const logout = useCallback(async () => {
    await apiLogout();
    setMe(null);
  }, []);

  const value = useMemo<AuthState>(() => ({ me, loading, login, register, logout, refreshMe }), [me, loading, login, register, logout, refreshMe]);

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAuth() {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
