import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { messages, type Lang } from './messages';
import { useAuth } from '../auth-context';

const LANG_KEY = 'ai_analytics_language';

type I18nValue = {
  lang: Lang;
  setLang: (lang: Lang) => Promise<void>;
  t: (key: string, vars?: Record<string, string | number | null | undefined>) => string;
};

const I18nContext = createContext<I18nValue | null>(null);

function normalizeLang(v: string | null | undefined): Lang {
  if (v === 'en' || v === 'kk' || v === 'ru') return v;
  return 'ru';
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const { state, setPreferredLanguage } = useAuth();
  const [lang, setLangState] = useState<Lang>(() => {
    try {
      return normalizeLang(localStorage.getItem(LANG_KEY));
    } catch {
      return 'ru';
    }
  });

  useEffect(() => {
    if (state.status !== 'authenticated') return;
    const userLang = normalizeLang(state.user.preferred_language);
    setLangState(userLang);
    localStorage.setItem(LANG_KEY, userLang);
  }, [state]);

  const setLang = async (v: Lang) => {
    const next = normalizeLang(v);
    const previous = lang;
    setLangState(next);
    localStorage.setItem(LANG_KEY, next);
    if (state.status !== 'authenticated') return;
    try {
      await setPreferredLanguage(next);
    } catch (err) {
      setLangState(previous);
      localStorage.setItem(LANG_KEY, previous);
      throw err;
    }
  };

  const t = (key: string, vars?: Record<string, string | number | null | undefined>) => {
    let value = messages[lang][key] ?? messages.en[key] ?? messages.ru[key] ?? key;
    if (vars) {
      for (const [name, raw] of Object.entries(vars)) {
        value = value.replaceAll(`{${name}}`, String(raw ?? ''));
      }
    }
    return value;
  };

  const value = useMemo<I18nValue>(() => ({ lang, setLang, t }), [lang, state, setPreferredLanguage]);
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n() {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error('useI18n must be used within I18nProvider');
  return ctx;
}
