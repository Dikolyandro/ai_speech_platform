import { useCallback, useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { motion } from 'motion/react';
import { BarChart3, MessageSquare, Search, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Badge } from './ui/badge';
import { Card } from './ui/card';
import { deleteSavedQuery, listSavedQueries, type SavedQueryDto } from '../../lib/api';
import { useI18n } from '../i18n/context';
import { AutoChart } from './analytics/AutoChart';

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

function rowsFromAnswer(answer: Record<string, unknown> | null): Record<string, unknown>[] {
  return Array.isArray(answer?.rows) ? (answer.rows as Record<string, unknown>[]) : [];
}

function hasVisual(item: SavedQueryDto): boolean {
  const answer = savedAnswer(item);
  const interpreted = answer?.interpreted as (Record<string, unknown> & { chart?: unknown }) | undefined;
  const chart = interpreted?.chart as { enabled?: boolean; chart_type?: string | null } | undefined;
  return Boolean(chart?.enabled && chart.chart_type && chart.chart_type !== 'table');
}

export function SavedVisuals() {
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
    const visualItems = queries.filter(hasVisual);
    if (!q) return visualItems;
    return visualItems.filter(
      (x) =>
        x.title.toLowerCase().includes(q) ||
        x.query_text.toLowerCase().includes(q) ||
        (x.answer_text && x.answer_text.toLowerCase().includes(q))
    );
  }, [queries, filter]);

  const openInChat = (item: SavedQueryDto) => {
    const src = savedSource(item);
    if (!src.sessionId) {
      sessionStorage.setItem(
        'runSavedQuery',
        JSON.stringify({ text: item.query_text, datasetId: item.dataset_id })
      );
      navigate('/');
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
    <div className="h-full p-6">
      <div className="max-w-[1800px] mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="mb-8"
        >
          <h1 className="text-3xl font-semibold mb-2 bg-gradient-to-r from-purple-400 to-violet-400 bg-clip-text text-transparent">
            {t('visuals.title')}
          </h1>
          <p className="text-muted-foreground">{t('visuals.subtitle')}</p>
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
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5 gap-4">
            {filtered.map((query, index) => {
              const answer = savedAnswer(query);
              const rows = rowsFromAnswer(answer);
              const interpreted = answer?.interpreted as
                | (Record<string, unknown> & { chart?: unknown })
                | undefined;
              const chart = interpreted?.chart as Parameters<typeof AutoChart>[0]['chart'];
              return (
                <motion.div
                  key={query.id}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.5, delay: 0.05 + index * 0.03 }}
                >
                  <Card className="p-3 transition-all border-border group overflow-hidden bg-card/80">
                    <div className="flex items-start justify-between gap-2 mb-2">
                      <div className="flex-1 min-w-0">
                        <h3 className="text-sm font-semibold mb-1 group-hover:text-primary transition-colors truncate">
                          {query.title}
                        </h3>
                        <Badge variant="outline" className="h-5 bg-violet-500/10 px-2 text-[11px] text-violet-300 border-violet-500/20">
                          <BarChart3 className="h-3 w-3 mr-1" />
                          {t('visuals.badge')}
                        </Badge>
                      </div>
                    </div>

                    <p className="text-xs text-white/75 line-clamp-2 mb-2">
                      {query.answer_text || query.query_text}
                    </p>

                    <div className="rounded-xl border border-white/10 bg-white/[0.025] p-2">
                      <AutoChart chart={chart} rows={rows} compact />
                    </div>

                    <div className="mt-2 flex items-center justify-end gap-1">
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        className="h-8 text-xs hover:bg-violet-500/10 hover:text-violet-300"
                        onClick={() => openInChat(query)}
                      >
                        <MessageSquare className="h-4 w-4 mr-2" />
                        {t('visuals.openChat')}
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
                  </Card>
                </motion.div>
              );
            })}
          </div>
        )}

        {!loading && filtered.length === 0 && (
          <p className="text-muted-foreground text-sm mt-8">{t('visuals.empty')}</p>
        )}
      </div>
    </div>
  );
}
