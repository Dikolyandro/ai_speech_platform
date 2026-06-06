import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { motion } from 'motion/react';
import { Search, Play, Trash2, Tag, MessageSquare } from 'lucide-react';
import { toast } from 'sonner';
import { formatDistanceToNow } from 'date-fns';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Badge } from './ui/badge';
import { Card } from './ui/card';
import { deleteSavedQuery, listSavedQueries, type SavedQueryDto } from '../../lib/api';
import { useI18n } from '../i18n/context';

const tagColors: Record<string, string> = {
  aggregation: 'bg-blue-500/10 text-blue-400 border-blue-500/20',
  analysis: 'bg-green-500/10 text-green-400 border-green-500/20',
  metrics: 'bg-purple-500/10 text-purple-400 border-purple-500/20',
  saved: 'bg-violet-500/10 text-violet-400 border-violet-500/20',
};

function savedAnswer(item: SavedQueryDto): Record<string, unknown> | null {
  return item.result_json && typeof item.result_json === 'object'
    ? (item.result_json as Record<string, unknown>)
    : null;
}

function savedSource(item: SavedQueryDto): { sessionId?: number; messageId?: string } {
  const ans = savedAnswer(item);
  const meta = ans?.saved_meta;
  if (!meta || typeof meta !== 'object') return {};
  const rec = meta as Record<string, unknown>;
  return {
    sessionId: typeof rec.session_id === 'number' ? rec.session_id : undefined,
    messageId: typeof rec.message_id === 'string' ? rec.message_id : undefined,
  };
}

export function SavedQueries() {
  const { t } = useI18n();
  const navigate = useNavigate();
  const [queries, setQueries] = useState<SavedQueryDto[]>([]);
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await listSavedQueries();
      setQueries(r.queries);
    } catch {
      setQueries([]);
      toast.error(t('common.error'));
    } finally {
      setLoading(false);
    }
  }, [t]);

  useEffect(() => {
    void load();
  }, [load]);

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return queries;
    return queries.filter(
      (x) =>
        x.title.toLowerCase().includes(q) ||
        x.query_text.toLowerCase().includes(q) ||
        (x.sql_text && x.sql_text.toLowerCase().includes(q))
    );
  }, [queries, filter]);

  const runQuery = (item: SavedQueryDto) => {
    sessionStorage.setItem(
      'runSavedQuery',
      JSON.stringify({ text: item.query_text, datasetId: item.dataset_id })
    );
    navigate('/');
    toast.message(t('common.done'));
  };

  const openInChat = (item: SavedQueryDto) => {
    const src = savedSource(item);
    if (!src.sessionId) {
      runQuery(item);
      return;
    }
    if (src.messageId) {
      sessionStorage.setItem('focusSavedMessageId', src.messageId);
    }
    navigate(`/session/${src.sessionId}`);
  };

  const remove = async (id: number) => {
    try {
      await deleteSavedQuery(id);
      await load();
      toast.success(t('common.delete'));
    } catch (e) {
      toast.error(e instanceof Error ? e.message : t('common.error'));
    }
  };

  return (
    <div className="h-full p-8">
      <div className="max-w-6xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="mb-8"
        >
          <h1 className="text-3xl font-semibold mb-2 bg-gradient-to-r from-purple-400 to-violet-400 bg-clip-text text-transparent">
            {t('saved.title')}
          </h1>
          <p className="text-muted-foreground">
            {t('saved.subtitle')}
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="mb-6"
        >
          <div className="relative max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder={t('saved.search')}
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="pl-10 bg-accent/50 border-border"
            />
          </div>
        </motion.div>

        {loading ? (
          <p className="text-muted-foreground text-sm">{t('common.loading')}</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {filtered.map((query, index) => {
              const src = savedSource(query);
              return (
              <motion.div
                key={query.id}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5, delay: 0.05 + index * 0.03 }}
              >
                <Card
                  className="p-4 hover:bg-accent/50 transition-all border-border group"
                  style={{ boxShadow: '0 4px 16px rgba(0, 0, 0, 0.1)' }}
                >
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1 min-w-0">
                      <h3 className="font-semibold mb-1 group-hover:text-primary transition-colors truncate">
                        {query.title}
                      </h3>
                      <Badge
                        variant="outline"
                        className={tagColors.saved || 'bg-muted text-muted-foreground'}
                      >
                        <Tag className="h-3 w-3 mr-1" />
                        {t('saved.badge')}
                      </Badge>
                    </div>
                  </div>

                  <div className="bg-background/50 rounded-lg p-3 mb-3 border border-border">
                    <code className="text-xs text-muted-foreground line-clamp-3 font-mono block">
                      {query.sql_text || query.query_text}
                    </code>
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">
                      {query.created_at
                        ? formatDistanceToNow(new Date(query.created_at), { addSuffix: true })
                        : ''}
                    </span>
                    <div className="flex gap-1">
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 hover:bg-violet-500/10 hover:text-violet-300"
                        onClick={() => openInChat(query)}
                        title={src.sessionId ? 'Open in chat' : t('saved.runInChat')}
                      >
                        <MessageSquare className="h-4 w-4" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 hover:bg-primary/10 hover:text-primary"
                        onClick={() => runQuery(query)}
                        title={t('saved.runInChat')}
                      >
                        <Play className="h-4 w-4" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 hover:bg-destructive/10 hover:text-destructive"
                        onClick={() => void remove(query.id)}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </Card>
              </motion.div>
              );
            })}
          </div>
        )}

        {!loading && filtered.length === 0 && (
          <p className="text-muted-foreground text-sm mt-8">
            {t('saved.empty')}
          </p>
        )}
      </div>
    </div>
  );
}
