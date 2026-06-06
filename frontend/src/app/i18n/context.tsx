import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { messages, type Lang } from './messages';
import { useAuth } from '../auth-context';

const LANG_KEY = 'ai_analytics_language';

type I18nValue = {
  lang: Lang;
  setLang: (lang: Lang) => void;
  t: (key: string) => string;
};

const I18nContext = createContext<I18nValue | null>(null);

function normalizeLang(v: string | null | undefined): Lang {
  if (v === 'en' || v === 'kk' || v === 'ru') return v;
  return 'ru';
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const { state } = useAuth();
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

  const setLang = (v: Lang) => {
    const next = normalizeLang(v);
    setLangState(next);
    localStorage.setItem(LANG_KEY, next);
  };

  const t = (key: string) => messages[lang][key] ?? messages.ru[key] ?? key;

  const value = useMemo<I18nValue>(() => ({ lang, setLang, t }), [lang]);
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n() {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error('useI18n must be used within I18nProvider');
  return ctx;
}

