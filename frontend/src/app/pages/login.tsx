import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import { toast } from 'sonner';
import { authLogin, authRegister, authResendVerificationCode, authVerifyEmail } from '../../lib/api';
import { useAuth } from '../auth-context';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { useI18n } from '../i18n/context';

export function LoginPage() {
  const nav = useNavigate();
  const [sp] = useSearchParams();
  const { setToken, recentAccounts, rememberAccount } = useAuth();
  const { lang, t } = useI18n();
  const [mode, setMode] = useState<'login' | 'register'>('login');
  const [nickname, setNickname] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [preferredLanguage, setPreferredLanguage] = useState<'ru' | 'en' | 'kk'>(lang);
  const [awaitingVerification, setAwaitingVerification] = useState(false);
  const [verificationCode, setVerificationCode] = useState('');
  const [resendBusy, setResendBusy] = useState(false);
  const [busy, setBusy] = useState(false);

  const title = useMemo(
    () => (awaitingVerification ? 'Verify your email' : mode === 'login' ? t('auth.login') : t('auth.register')),
    [awaitingVerification, mode, t]
  );

  const switchMode = (nextMode: 'login' | 'register') => {
    setMode(nextMode);
    setAwaitingVerification(false);
    setVerificationCode('');
  };

  useEffect(() => {
    const nick = (sp.get('nick') || '').trim();
    if (nick) setNickname(nick);
    else if (!nickname && recentAccounts.length) setNickname(recentAccounts[0]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const submit = async () => {
    setBusy(true);
    try {
      const n = nickname.trim();
      if (!n) throw new Error(t('auth.nickname'));

      if (mode === 'register' && awaitingVerification) {
        const e = email.trim().toLowerCase();
        const code = verificationCode.trim();
        if (!e) throw new Error(t('auth.email'));
        if (!/^\d{6}$/.test(code)) throw new Error('Enter the 6-digit verification code.');
        await authVerifyEmail(e, code);
        toast.success('Email verified. You can sign in now.');
        setAwaitingVerification(false);
        setVerificationCode('');
        setMode('login');
        setPassword('');
        return;
      }

      if (!password) throw new Error(t('auth.password'));

      if (mode === 'register') {
        const e = email.trim().toLowerCase();
        if (!e) throw new Error(t('auth.email'));
        await authRegister(n, e, password, preferredLanguage);
        setEmail(e);
        setAwaitingVerification(true);
        setVerificationCode('');
        toast.success('Verification code sent. Check your email.');
        return;
      }
      const r = await authLogin(n, password);
      await setToken(r.access_token);
      rememberAccount(n);
      toast.success(t('common.done'));
      nav('/');
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('common.error'));
    } finally {
      setBusy(false);
    }
  };

  const resendCode = async () => {
    setResendBusy(true);
    try {
      const e = email.trim().toLowerCase();
      if (!e) throw new Error(t('auth.email'));
      await authResendVerificationCode(e);
      toast.success('If this email needs verification, a new code has been sent.');
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('common.error'));
    } finally {
      setResendBusy(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-background px-4">
      <div className="w-full max-w-md rounded-xl border border-border bg-card p-6 shadow-sm">
        <div className="mb-6">
          <div className="text-2xl font-semibold text-foreground">{title}</div>
          <div className="text-sm text-muted-foreground mt-1">
            {t('auth.subtitle')}
          </div>
        </div>

        <div className="space-y-3">
          {recentAccounts.length > 0 && mode === 'login' && (
            <div className="text-xs text-muted-foreground">
              {t('auth.latestAccounts')}{' '}
              {recentAccounts.slice(0, 4).map((a, idx) => (
                <button
                  key={a}
                  type="button"
                  className="text-primary hover:underline"
                  onClick={() => setNickname(a)}
                  disabled={busy}
                >
                  {idx > 0 ? ' · ' : ''}
                  {a}
                </button>
              ))}
            </div>
          )}
          {awaitingVerification ? (
            <>
              <Input
                type="email"
                placeholder={t('auth.email')}
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoComplete="email"
              />
              <Input
                type="text"
                inputMode="numeric"
                pattern="[0-9]*"
                maxLength={6}
                placeholder="6-digit code"
                value={verificationCode}
                onChange={(e) => setVerificationCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                autoComplete="one-time-code"
              />
              <Button className="w-full" onClick={submit} disabled={busy}>
                {busy ? '...' : 'Verify email'}
              </Button>
              <Button
                className="w-full"
                variant="outline"
                onClick={resendCode}
                disabled={busy || resendBusy}
              >
                {resendBusy ? '...' : 'Resend code'}
              </Button>
            </>
          ) : (
            <>
              <Input
                type="text"
                placeholder={t('auth.nickname')}
                value={nickname}
                onChange={(e) => setNickname(e.target.value)}
                autoComplete="username"
              />
              {mode === 'register' && (
                <>
                  <Input
                    type="email"
                    placeholder={t('auth.email')}
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    autoComplete="email"
                  />
                  <div className="space-y-1">
                    <label className="text-xs text-muted-foreground">{t('auth.prefLang')}</label>
                    <select
                      className="w-full rounded-md border border-border bg-card text-foreground px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                      value={preferredLanguage}
                      onChange={(e) => setPreferredLanguage(e.target.value as 'ru' | 'en' | 'kk')}
                    >
                      <option value="ru">{t('auth.lang.ru')}</option>
                      <option value="en">{t('auth.lang.en')}</option>
                      <option value="kk">{t('auth.lang.kk')}</option>
                    </select>
                  </div>
                </>
              )}
              <Input
                type="password"
                placeholder={t('auth.password')}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
              />
              <Button className="w-full" onClick={submit} disabled={busy}>
                {busy ? '...' : mode === 'login' ? t('auth.signIn') : t('auth.createAccount')}
              </Button>
            </>
          )}
        </div>

        <div className="mt-4 text-sm text-muted-foreground">
          {awaitingVerification ? 'Already verified?' : mode === 'login' ? t('auth.noAccount') : t('auth.haveAccount')}{' '}
          <button
            className="text-primary hover:underline"
            type="button"
            onClick={() => switchMode(mode === 'login' ? 'register' : 'login')}
            disabled={busy}
          >
            {mode === 'login' ? t('auth.register') : t('auth.signIn')}
          </button>
        </div>
      </div>
    </div>
  );
}
