import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import {
  authMe,
  authUpdateLanguage,
  getAccessToken,
  setAccessToken,
  type PreferredLanguage,
  type UserMeDto,
} from '../lib/api';

const RECENT_KEY = 'ai_analytics_recent_accounts';

function loadRecent(): string[] {
  try {
    const raw = localStorage.getItem(RECENT_KEY);
    const arr = raw ? (JSON.parse(raw) as unknown) : [];
    return Array.isArray(arr) ? arr.filter((x): x is string => typeof x === 'string') : [];
  } catch {
    return [];
  }
}

function saveRecent(nicknames: string[]) {
  localStorage.setItem(RECENT_KEY, JSON.stringify(nicknames.slice(0, 8)));
}

type AuthState =
  | { status: 'loading' }
  | { status: 'anonymous' }
  | { status: 'authenticated'; user: UserMeDto };

type AuthContextValue = {
  state: AuthState;
  recentAccounts: string[];
  rememberAccount: (nickname: string) => void;
  setToken: (token: string | null) => Promise<void>;
  refreshMe: () => Promise<void>;
  setPreferredLanguage: (lang: PreferredLanguage) => Promise<void>;
  logout: () => void;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({ status: 'loading' });
  const [recentAccounts, setRecentAccounts] = useState<string[]>(() => loadRecent());

  const refreshMe = async () => {
    const token = getAccessToken();
    if (!token) {
      setState({ status: 'anonymous' });
      return;
    }
    try {
      const me = await authMe();
      setState({ status: 'authenticated', user: me });
      if (me.nickname) {
        rememberAccount(me.nickname);
      }
    } catch {
      setAccessToken(null);
      setState({ status: 'anonymous' });
    }
  };

  useEffect(() => {
    void refreshMe();
  }, []);

  const setToken = async (token: string | null) => {
    setAccessToken(token);
    await refreshMe();
  };

  const rememberAccount = (nickname: string) => {
    const n = nickname.trim();
    if (!n) return;
    setRecentAccounts((prev) => {
      const next = [n, ...prev.filter((x) => x !== n)];
      saveRecent(next);
      return next;
    });
  };

  const logout = () => {
    setAccessToken(null);
    setState({ status: 'anonymous' });
  };

  const setPreferredLanguage = async (lang: PreferredLanguage) => {
    if (state.status !== 'authenticated') return;
    const me = await authUpdateLanguage(lang);
    setState({ status: 'authenticated', user: me });
  };

  const value = useMemo<AuthContextValue>(
    () => ({ state, recentAccounts, rememberAccount, setToken, refreshMe, setPreferredLanguage, logout }),
    [state, recentAccounts]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}

