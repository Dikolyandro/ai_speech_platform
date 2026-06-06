import { useCallback, useEffect, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router';
import {
  Plus,
  Mic,
  Database,
  HardDrive,
  Bookmark,
  BarChart3,
  MessageSquare,
  PanelLeftClose,
  PanelLeftOpen,
  MoreVertical,
  ChevronDown,
  ChevronUp,
  LogOut,
  UserPlus,
} from 'lucide-react';
import { toast } from 'sonner';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from './ui/dropdown-menu';
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from './ui/dialog';
import { Button } from './ui/button';
import { Input } from './ui/input';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from './ui/alert-dialog';
import {
  createChatSession,
  deleteChatSession,
  listChatSessions,
  renameChatSession,
  type ChatSessionDto,
} from '../../lib/api';
import { useAuth } from '../auth-context';
import { useI18n } from '../i18n/context';

interface AppSidebarProps {
  collapsed: boolean;
  onToggleCollapse: () => void;
}

export function AppSidebar({ collapsed, onToggleCollapse }: AppSidebarProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const { state: auth, recentAccounts, logout, setPreferredLanguage } = useAuth();
  const { t, lang, setLang } = useI18n();
  const [openSessions, setOpenSessions] = useState(true);
  const [sessions, setSessions] = useState<ChatSessionDto[]>([]);
  const [renameOpen, setRenameOpen] = useState(false);
  const [renameTitle, setRenameTitle] = useState('');
  const [renameId, setRenameId] = useState<number | null>(null);
  const [deleteId, setDeleteId] = useState<number | null>(null);

  const refreshSessions = useCallback(async () => {
    try {
      const r = await listChatSessions();
      setSessions(r.sessions);
    } catch {
      setSessions([]);
    }
  }, []);

  useEffect(() => {
    void refreshSessions();
  }, [location.pathname, refreshSessions]);

  const isActive = (path: string) => {
    if (path === '/') {
      return location.pathname === '/' || location.pathname.startsWith('/session/');
    }
    return location.pathname.startsWith(path);
  };

  const isVoiceSessionsActive =
    location.pathname === '/' || location.pathname.startsWith('/session/');

  const onNewSession = async (e: React.MouseEvent) => {
    e.preventDefault();
    try {
      const s = await createChatSession(t('chat.newChatTitle'));
      await refreshSessions();
      navigate(`/session/${s.id}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('common.error'));
    }
  };

  const openRename = (s: ChatSessionDto) => {
    setRenameId(s.id);
    setRenameTitle(s.title);
    setRenameOpen(true);
  };

  const submitRename = async () => {
    if (renameId == null || !renameTitle.trim()) return;
    try {
      await renameChatSession(renameId, renameTitle.trim());
      setRenameOpen(false);
      await refreshSessions();
      toast.success(t('common.save'));
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('common.error'));
    }
  };

  const confirmDelete = async () => {
    if (deleteId == null) return;
    try {
      await deleteChatSession(deleteId);
      if (location.pathname === `/session/${deleteId}`) navigate('/');
      setDeleteId(null);
      await refreshSessions();
      toast.success(t('common.delete'));
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('common.error'));
    }
  };

  if (collapsed) {
    return (
      <aside className="w-[84px] h-screen bg-[#0b0b14] border-r border-white/10 flex flex-col items-center py-5 transition-all duration-300">
        <div className="flex flex-col items-center gap-4 w-full">
          <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center shrink-0">
            <div className="h-4 w-4 rounded-full border-2 border-white/90" />
          </div>
          <button
            type="button"
            onClick={onToggleCollapse}
            className="h-10 w-10 rounded-xl bg-white/5 hover:bg-white/10 flex items-center justify-center transition"
          >
            <PanelLeftOpen className="h-5 w-5 text-white/70" />
          </button>
        </div>
        <div className="mt-8 flex flex-col items-center gap-3 w-full px-3">
          <button
            type="button"
            onClick={onNewSession}
            className="h-11 w-11 rounded-2xl bg-[#171726] hover:bg-[#1d1d30] border border-white/10 text-white flex items-center justify-center transition"
          >
            <Plus className="h-5 w-5 text-white/80" />
          </button>
          <Link
            to="/queries"
            className={`h-11 w-11 rounded-2xl transition flex items-center justify-center ${
              isActive('/queries')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/65 hover:text-white hover:bg-white/5'
            }`}
          >
            <Bookmark className="h-5 w-5" />
          </Link>
          <Link
            to="/visuals"
            className={`h-11 w-11 rounded-2xl transition flex items-center justify-center ${
              isActive('/visuals')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/65 hover:text-white hover:bg-white/5'
            }`}
          >
            <BarChart3 className="h-5 w-5" />
          </Link>
          <Link
            to="/datasets"
            className={`h-11 w-11 rounded-2xl transition flex items-center justify-center ${
              isActive('/datasets')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/65 hover:text-white hover:bg-white/5'
            }`}
          >
            <Database className="h-5 w-5" />
          </Link>
          <Link
            to="/bigdata"
            className={`h-11 w-11 rounded-2xl transition flex items-center justify-center ${
              isActive('/bigdata')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/65 hover:text-white hover:bg-white/5'
            }`}
          >
            <HardDrive className="h-5 w-5" />
          </Link>
          <div className="my-2 h-px w-10 bg-white/10" />
          <Link
            to="/"
            className={`h-11 w-11 rounded-2xl transition flex items-center justify-center ${
              isVoiceSessionsActive
                ? 'bg-violet-500/12 text-white'
                : 'text-white/65 hover:text-white hover:bg-white/5'
            }`}
          >
            <Mic className="h-5.5 w-5.5" />
          </Link>
        </div>
        <div className="mt-auto pb-2">
          <div className="h-11 w-11 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center text-white font-semibold">
            S
          </div>
        </div>
      </aside>
    );
  }

  return (
    <>
      <aside className="w-[290px] h-screen bg-[#0b0b14] border-r border-white/10 flex flex-col transition-all duration-300">
        <div className="px-6 pt-8 pb-5">
          <div className="flex items-center justify-between mb-7">
            <div className="h-9 w-9 rounded-xl bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center shrink-0">
              <div className="h-4 w-4 rounded-full border-2 border-white/90" />
            </div>
            <button
              type="button"
              onClick={onToggleCollapse}
              className="h-9 w-9 rounded-xl bg-white/5 hover:bg-white/10 flex items-center justify-center transition"
            >
              <PanelLeftClose className="h-5 w-5 text-white/70" />
            </button>
          </div>

          <button
            type="button"
            onClick={onNewSession}
            className="w-full flex items-center gap-3 rounded-2xl border border-violet-400/10 bg-gradient-to-br from-violet-500/18 to-fuchsia-500/10 px-4 py-3 text-white shadow-[0_10px_30px_rgba(139,92,246,0.18)] transition hover:from-violet-500/24 hover:to-fuchsia-500/14 text-left"
          >
            <Plus className="h-5 w-5 text-white/90" />
            <span className="text-sm font-medium">{t('sidebar.newSession')}</span>
          </button>
        </div>

        <div className="px-3 space-y-1">
          <Link
            to="/queries"
            className={`flex w-full items-center gap-3 rounded-2xl px-3 py-3 text-sm transition ${
              isActive('/queries')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/70 hover:bg-white/5 hover:text-white'
            }`}
          >
            <Bookmark className="h-4 w-4" />
            <span>{t('sidebar.savedQueries')}</span>
          </Link>
          <Link
            to="/visuals"
            className={`flex w-full items-center gap-3 rounded-2xl px-3 py-3 text-sm transition ${
              isActive('/visuals')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/70 hover:bg-white/5 hover:text-white'
            }`}
          >
            <BarChart3 className="h-4 w-4" />
            <span>{t('sidebar.visuals')}</span>
          </Link>
          <Link
            to="/datasets"
            className={`flex w-full items-center gap-3 rounded-2xl px-3 py-3 text-sm transition ${
              isActive('/datasets')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/70 hover:bg-white/5 hover:text-white'
            }`}
          >
            <Database className="h-4 w-4" />
            <span>{t('sidebar.uploadedDatasets')}</span>
          </Link>
          <Link
            to="/bigdata"
            className={`flex w-full items-center gap-3 rounded-2xl px-3 py-3 text-sm transition ${
              isActive('/bigdata')
                ? 'bg-violet-500/12 text-white'
                : 'text-white/70 hover:bg-white/5 hover:text-white'
            }`}
          >
            <HardDrive className="h-4 w-4" />
            <span>Big Data</span>
          </Link>
        </div>

        <div className="px-3 pt-4">
          <div className="mb-2 h-px w-full bg-white/8" />
          <button
            type="button"
            onClick={() => setOpenSessions((prev) => !prev)}
            className={`flex w-full items-center gap-3 rounded-2xl px-3 py-3 text-sm transition ${
              isVoiceSessionsActive
                ? 'bg-violet-500/12 text-white'
                : 'text-white/70 hover:bg-white/5 hover:text-white'
            }`}
          >
            <Mic className="h-[18px] w-[18px]" />
            <span className="flex-1 text-left">{t('sidebar.voiceSessions')}</span>
            {openSessions ? (
              <ChevronUp className="h-4 w-4 text-white/55" />
            ) : (
              <ChevronDown className="h-4 w-4 text-white/55" />
            )}
          </button>
        </div>

        {openSessions && (
          <div className="mt-2 px-3 pb-4 overflow-y-auto flex-1 min-h-0">
            <div className="space-y-2">
              {sessions.map((session) => (
                <div
                  key={session.id}
                  className={`group rounded-2xl px-3 py-3 transition ${
                    location.pathname === `/session/${session.id}`
                      ? 'bg-white/6 border border-white/8'
                      : 'hover:bg-white/5'
                  }`}
                >
                  <div className="flex items-start gap-3">
                    <MessageSquare className="mt-0.5 h-4 w-4 shrink-0 text-white/40" />
                    <Link to={`/session/${session.id}`} className="min-w-0 flex-1">
                      <p className="truncate text-sm text-white/82">{session.title}</p>
                    </Link>
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <button
                          type="button"
                          className="rounded-lg p-1 text-white/35 opacity-0 transition hover:bg-white/8 hover:text-white/70 group-hover:opacity-100"
                        >
                          <MoreVertical className="h-4 w-4" />
                        </button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent
                        align="end"
                        className="border-white/10 bg-[#141420] text-white"
                      >
                        <DropdownMenuItem
                          className="focus:bg-white/5 focus:text-white"
                          onClick={() => openRename(session)}
                        >
                          {t('sidebar.rename')}
                        </DropdownMenuItem>
                        <DropdownMenuItem
                          className="focus:bg-white/5 focus:text-red-400"
                          onClick={() => setDeleteId(session.id)}
                        >
                          {t('sidebar.delete')}
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="mt-auto p-4">
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="w-full flex items-center gap-3 rounded-2xl border border-white/8 bg-white/[0.03] px-3 py-3 hover:bg-white/[0.06] transition text-left"
              >
                <div className="h-10 w-10 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center text-white font-semibold">
                  {auth.status === 'authenticated' ? auth.user.nickname.slice(0, 1).toUpperCase() : 'U'}
                </div>
                <div className="min-w-0 flex-1">
                  <p className="truncate text-sm font-medium text-white">
                    {auth.status === 'authenticated' ? auth.user.nickname : t('sidebar.profile')}
                  </p>
                  <p className="truncate text-xs text-white/45">
                    {auth.status === 'authenticated' ? auth.user.email ?? '' : ''}
                  </p>
                </div>
                <MoreVertical className="h-4 w-4 text-white/40" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="border-white/10 bg-[#141420] text-white w-[260px]">
              <DropdownMenuItem
                className="focus:bg-white/5 focus:text-white"
                onClick={() => {
                  logout();
                  navigate('/login');
                }}
              >
                <LogOut className="h-4 w-4 mr-2" />
                {t('sidebar.logout')}
              </DropdownMenuItem>
              <DropdownMenuItem
                className="focus:bg-white/5 focus:text-white"
                onClick={() => {
                  logout();
                  navigate('/login');
                }}
              >
                <UserPlus className="h-4 w-4 mr-2" />
                {t('sidebar.addAccount')}
              </DropdownMenuItem>
              <DropdownMenuItem
                className="focus:bg-white/5 focus:text-white"
                onClick={async () => {
                  const next = lang === 'ru' ? 'en' : lang === 'en' ? 'kk' : 'ru';
                  try {
                    if (auth.status === 'authenticated') await setPreferredLanguage(next);
                    setLang(next);
                  } catch {
                    toast.error(t('common.error'));
                  }
                }}
              >
                {t('sidebar.language')}: {lang.toUpperCase()}
              </DropdownMenuItem>
              {recentAccounts.length > 0 && (
                <>
                  <div className="px-2 pt-2 pb-1 text-xs text-white/45">{t('sidebar.recent')}</div>
                  {recentAccounts.slice(0, 6).map((n) => (
                    <DropdownMenuItem
                      key={n}
                      className="focus:bg-white/5 focus:text-white"
                      onClick={() => {
                        logout();
                        navigate(`/login?nick=${encodeURIComponent(n)}`);
                      }}
                    >
                      {n}
                    </DropdownMenuItem>
                  ))}
                </>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </aside>

      <Dialog open={renameOpen} onOpenChange={setRenameOpen}>
        <DialogContent className="border-white/10 bg-[#141420] text-white sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{t('sidebar.renameChat')}</DialogTitle>
          </DialogHeader>
          <Input
            value={renameTitle}
            onChange={(e) => setRenameTitle(e.target.value)}
            className="bg-white/5 border-white/10"
            placeholder={t('sidebar.chatName')}
          />
          <DialogFooter>
            <Button variant="outline" onClick={() => setRenameOpen(false)}>
              {t('common.cancel')}
            </Button>
            <Button onClick={() => void submitRename()}>{t('common.save')}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog open={deleteId != null} onOpenChange={(o) => !o && setDeleteId(null)}>
        <AlertDialogContent className="border-white/10 bg-[#141420] text-white">
          <AlertDialogHeader>
            <AlertDialogTitle>{t('sidebar.delete')}?</AlertDialogTitle>
            <AlertDialogDescription className="text-white/60">
              {t('sidebar.deleteWarn')}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel className="border-white/10 bg-transparent text-white">{t('common.cancel')}</AlertDialogCancel>
            <AlertDialogAction
              className="bg-red-600 hover:bg-red-700"
              onClick={() => void confirmDelete()}
            >
              {t('common.delete')}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
