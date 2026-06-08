import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router';
import { motion, AnimatePresence } from 'motion/react';
import { Database, Mic, Square, Play, Send, Trash2, Paperclip, Bookmark, Loader2, ChevronUp, ChevronDown, Sparkles } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from './ui/button';
import { ScrollArea } from './ui/scroll-area';
import { Textarea } from './ui/textarea';
import { Input } from './ui/input';
import { Badge } from './ui/badge';
import { cn } from './ui/utils';
import { useDataset } from '../dataset-context';
import { useAuth } from '../auth-context';
import {
  appendChatMessage,
  createChatSession,
  postQueryAnswer,
  createSavedQuery,
  deleteSavedQuery,
  listSavedQueries,
  listChatMessages,
  importCsvFile,
  createDataset,
  getDatasetChatContext,
  getDatasetSuggestions,
  transcribeAudio,
  type ChatMessageDto,
  type DatasetChatContext,
  type DatasetColumnProfile,
  type DatasetSuggestion,
  type QueryAnswerChart,
} from '../../lib/api';
import { useI18n } from '../i18n/context';
import { AutoChart } from './analytics/AutoChart';
import { ResultTable } from './result-table';
import { AssistantMascot } from './assistant-mascot';

type RecordingState = 'idle' | 'recording' | 'recorded' | 'playing';
type ComposerMode = 'voice' | 'text';

interface Message {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  isAudio?: boolean;
  audioUrl?: string;
  transcriptText?: string;
  waveformData?: number[];
  duration?: number;
  timestamp: Date;
  /** полный ответ /api/v1/query/answer для сохранения */
  answerPayload?: Record<string, unknown>;
  userQuery?: string;
  conversationalIntent?: string;
  responseTimeMs?: number;
}

const WAVEFORM_BAR_COUNT = 54;
const CHAT_NAME_STORAGE_KEY = 'ai_analytics_chat_display_name';

type DatasetCapabilityGroups = {
  numeric: string[];
  categorical: string[];
  text: string[];
  date: string[];
};

type ConversationIntent =
  | { kind: 'none' }
  | { kind: 'name'; name: string }
  | { kind: 'greeting' }
  | { kind: 'smalltalk' }
  | { kind: 'help' }
  | { kind: 'capabilities' }
  | { kind: 'dataset_help' }
  | { kind: 'thanks' };

const SUGGESTION_CATEGORY_ORDER = [
  'Fare',
  'Distance',
  'Passengers',
  'Payments',
  'Time',
  'Locations',
  'Vendors',
  'Fees',
  'numeric',
  'categorical',
  'boolean',
  'date',
  'text',
];

function columnType(column: DatasetColumnProfile): string {
  return String(column.type ?? column.data_type ?? '').toLowerCase();
}

function columnName(column: DatasetColumnProfile): string {
  return String(column.name ?? '').trim();
}

function prettyColumnName(name: string): string {
  return name.replace(/[_-]+/g, ' ').trim() || name;
}

function profileNumber(column: DatasetColumnProfile, keys: string[]): number | null {
  const profile = column.profile;
  if (!profile || typeof profile !== 'object') return null;
  for (const key of keys) {
    const value = profile[key];
    if (typeof value === 'number' && Number.isFinite(value)) return value;
  }
  return null;
}

function isNumericColumn(column: DatasetColumnProfile): boolean {
  const type = columnType(column);
  return /\b(int|integer|bigint|long|double|float|real|decimal|numeric|number)\b/.test(type);
}

function isDateColumn(column: DatasetColumnProfile): boolean {
  const type = columnType(column);
  const name = columnName(column).toLowerCase();
  return /date|time|timestamp/.test(type) || /(^|_)(date|time|year|month|day)(_|$)/.test(name);
}

function isTextColumn(column: DatasetColumnProfile): boolean {
  const type = columnType(column);
  const name = columnName(column).toLowerCase();
  if (!/(text|string|varchar|char|object)/.test(type)) return false;
  return /review|summary|description|comment|text|message|content|title|name/.test(name);
}

function isCategoricalColumn(column: DatasetColumnProfile): boolean {
  if (isNumericColumn(column) || isDateColumn(column)) return false;
  const type = columnType(column);
  if (/bool|boolean/.test(type)) return true;
  const distinct = profileNumber(column, ['distinct_count', 'unique_count', 'n_unique']);
  const rowCount = profileNumber(column, ['row_count', 'count']);
  if (distinct != null && distinct <= 80) return true;
  if (distinct != null && rowCount != null && rowCount > 0 && distinct / rowCount <= 0.12) return true;
  return /(string|varchar|char|text|object)/.test(type) && !isTextColumn(column);
}

function classifyCapabilities(columns: DatasetColumnProfile[]): DatasetCapabilityGroups {
  const groups: DatasetCapabilityGroups = { numeric: [], categorical: [], text: [], date: [] };
  for (const column of columns) {
    const name = columnName(column);
    if (!name) continue;
    if (isDateColumn(column)) groups.date.push(name);
    else if (isNumericColumn(column)) groups.numeric.push(name);
    else if (isTextColumn(column)) groups.text.push(name);
    else if (isCategoricalColumn(column)) groups.categorical.push(name);
  }
  return {
    numeric: groups.numeric.slice(0, 8),
    categorical: groups.categorical.slice(0, 8),
    text: groups.text.slice(0, 6),
    date: groups.date.slice(0, 6),
  };
}

function groupSuggestions(suggestions: DatasetSuggestion[]) {
  const groups = new Map<string, DatasetSuggestion[]>();
  const seen = new Set<string>();
  for (const suggestion of suggestions) {
    const key = `${suggestion.title.trim().toLowerCase()}::${suggestion.query.trim().toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const category = suggestion.category || 'Suggested';
    groups.set(category, [...(groups.get(category) ?? []), suggestion]);
  }
  return [...groups.entries()].sort(([a], [b]) => {
    const ai = SUGGESTION_CATEGORY_ORDER.indexOf(a);
    const bi = SUGGESTION_CATEGORY_ORDER.indexOf(b);
    if (ai === -1 && bi === -1) return a.localeCompare(b);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });
}

function normalizeChatText(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[’']/g, "'")
    .replace(/[?!.,;:]+$/g, '')
    .replace(/\s+/g, ' ');
}

function extractIntroducedName(text: string): string | null {
  const patterns = [
    /\bmy name is\s+([a-zа-яёәғқңөұүһі][a-zа-яёәғқңөұүһі -]{1,38})/i,
    /\bcall me\s+([a-zа-яёәғқңөұүһі][a-zа-яёәғқңөұүһі -]{1,38})/i,
    /\bменя зовут\s+([a-zа-яёәғқңөұүһі][a-zа-яёәғқңөұүһі -]{1,38})/i,
    /\bзови меня\s+([a-zа-яёәғқңөұүһі][a-zа-яёәғқңөұүһі -]{1,38})/i,
    /\bменің атым\s+([a-zа-яёәғқңөұүһі][a-zа-яёәғқңөұүһі -]{1,38})/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    const raw = match?.[1]?.trim();
    if (!raw) continue;
    return raw
      .replace(/[?!.,;:]+$/g, '')
      .split(/\s+/)
      .slice(0, 3)
      .join(' ')
      .trim();
  }
  return null;
}

function detectConversationIntent(text: string): ConversationIntent {
  const q = normalizeChatText(text);
  if (!q) return { kind: 'none' };

  const introducedName = extractIntroducedName(text);
  if (introducedName) return { kind: 'name', name: introducedName };

  if (/^(hi|hello|hey|привет|салем|сәлем|здравствуй|здравствуйте|добрый день|доброе утро|добрый вечер)$/.test(q)) {
    return { kind: 'greeting' };
  }
  if (/\b(how are you|how r u|what's up|whats up)\b/.test(q) || /(как дела|как ты|қалайсың|калайсын)/.test(q)) {
    return { kind: 'smalltalk' };
  }
  if (
    /\b(what can you do|help|how to use|what can i ask|what can platform do)\b/.test(q) ||
    /(помощь|что ты умеешь|как пользоваться|что можно спросить|что может платформа|что умеет платформа)/.test(q)
  ) {
    if (/dataset|датасет|data set|данн|запрос/.test(q)) return { kind: 'dataset_help' };
    return { kind: 'capabilities' };
  }
  if (
    /\b(what can i ask about this dataset|suggest queries|suggest questions|help with this dataset)\b/.test(q) ||
    /(что можно спросить по этому датасету|помоги с этим датасетом|предложи запросы|предложи вопросы)/.test(q)
  ) {
    return { kind: 'dataset_help' };
  }
  if (/^(thanks|thank you|thx|спасибо|рахмет|спс)$/.test(q)) {
    return { kind: 'thanks' };
  }
  return { kind: 'none' };
}

function buildConversationResponse(
  intent: Exclude<ConversationIntent, { kind: 'none' }>,
  options: {
    name?: string;
    hasDataset: boolean;
    suggestions: DatasetSuggestion[];
  }
) {
  if (intent.kind === 'name') {
    return `Hi, ${intent.name}! Ready to analyze your data?`;
  }
  if (intent.kind === 'greeting') {
    return `${options.name ? `Hi, ${options.name}` : 'Hi'} 👋 I'm ready to help you explore this dataset. You can ask me about averages, counts, top values, distributions, or trends.`;
  }
  if (intent.kind === 'smalltalk') {
    return "I'm doing well and ready to analyze your data. Choose a suggested question below or ask your own.";
  }
  if (intent.kind === 'help') {
    return [
      'I can help you analyze your data without writing SQL. You can ask questions such as:',
      '• average values',
      '• counts by category',
      '• top records',
      '• distributions',
      '• trends over time',
      '• charts and visual insights',
      '',
      'For large datasets, process them with Spark first, create a smaller chat sample, and then analyze it here.',
    ].join('\n');
  }
  if (intent.kind === 'capabilities') {
    return [
      'I can act as your analytics assistant for this platform.',
      '',
      'I can help you upload datasets, process Big Data with Spark, create chat samples, ask natural-language questions, generate SQL-backed answers, build charts, save useful queries, and use voice input.',
      '',
      'When a dataset is selected, I also generate useful questions from its actual columns.',
    ].join('\n');
  }
  if (intent.kind === 'dataset_help') {
    if (!options.hasDataset) {
      return 'Please select a dataset first, and I will suggest useful questions based on its columns.';
    }
    if (!options.suggestions.length) {
      return 'I can help with this dataset, but I could not read enough column details yet. Try asking about averages, counts, top values, or trends.';
    }
    return 'I found useful questions for this dataset based on its columns. Check the suggestions panel below, or ask your own question in plain language.';
  }
  return "You're welcome! You can ask another question or choose one of the dataset suggestions.";
}

function FieldList({ label, values }: { label: string; values: string[] }) {
  if (!values.length) return null;
  return (
    <div className="min-w-0">
      <p className="mb-1 text-xs font-semibold uppercase tracking-normal text-white/45">{label}</p>
      <p className="truncate text-sm text-white/78">{values.map(prettyColumnName).join(', ')}</p>
    </div>
  );
}

function formatDatasetCount(value: number | null | undefined) {
  return typeof value === 'number' ? value.toLocaleString() : '-';
}

function datasetColumnCount(context: DatasetChatContext | null) {
  return context?.columns?.length ?? null;
}

function silentWaveform() {
  return Array.from({ length: WAVEFORM_BAR_COUNT }, () => 0.035);
}

/** Короткое уточнение («highest too») дополняет предыдущую реплику пользователя для NL→SQL. */
function mergeFollowUpDataQuery(messages: Message[], q: string): string {
  const s = q.trim();
  if (!s) return q;
  const looksLikeFollow =
    /^(and\s+)?(the\s+)?(highest|lowest)\b/i.test(s) ||
    /\b(highest|lowest)\s+too\s*$/i.test(s) ||
    /^also\b/i.test(s) ||
    /^(show\s+)?(me\s+)?(the\s+)?(highest|lowest)\b/i.test(s);
  if (!looksLikeFollow) return q;
  const prev = [...messages].reverse().find((m) => m.type === 'user');
  if (prev?.content?.trim()) {
    return `${prev.content.trim()} ${s}`;
  }
  return q;
}

/** Убирает полный дубликат фразы и повтор подряд того же предложения (типичный мусор ASR). */
function normalizeVoiceText(text: string): string {
  let t = text.replace(/\s+/g, ' ').trim();
  if (t.length > 24) {
    const h = Math.floor(t.length / 2);
    if (t.slice(0, h) === t.slice(h)) {
      t = t.slice(0, h).trim();
    }
  }
  const parts = t.split(/\.\s+/).map((p) => p.trim()).filter(Boolean);
  const out: string[] = [];
  for (const p of parts) {
    if (out.length && out[out.length - 1] === p) continue;
    out.push(p);
  }
  return out.join('. ').trim();
}

function chartFromAnswerPayload(payload: Record<string, unknown> | undefined): QueryAnswerChart | null {
  try {
    const interpreted = payload?.interpreted as
      | (Record<string, unknown> & { chart?: QueryAnswerChart | null })
      | undefined;
    return interpreted?.chart ?? null;
  } catch {
    return null;
  }
}

function hasVisibleChart(payload: Record<string, unknown> | undefined): boolean {
  const chart = chartFromAnswerPayload(payload);
  return Boolean(chart?.enabled !== false && chart?.chart_type && chart.chart_type !== 'table');
}

function formatResponseTime(ms: number): string {
  if (ms < 1000) return `Generated in ${Math.round(ms)} ms`;
  return `Generated in ${(ms / 1000).toFixed(2)} s`;
}

function mapDtoToMessage(m: ChatMessageDto): Message {
  const meta = m.meta_json as {
    answer?: Record<string, unknown>;
    userQuery?: string;
    conversational?: boolean;
    intent?: string;
    voice?: {
      transcribed_text?: string;
      waveform_levels?: number[];
      duration?: number;
    };
  } | null;
  const voice = meta?.voice;
  return {
    id: String(m.id),
    type: m.role === 'user' ? 'user' : 'assistant',
    content: m.content,
    isAudio: Boolean(voice),
    transcriptText: voice?.transcribed_text,
    waveformData: Array.isArray(voice?.waveform_levels) ? voice.waveform_levels : undefined,
    duration: typeof voice?.duration === 'number' ? voice.duration : undefined,
    timestamp: m.created_at ? new Date(m.created_at) : new Date(),
    answerPayload: meta?.answer,
    userQuery: meta?.userQuery,
    conversationalIntent: meta?.conversational ? meta.intent || 'conversation' : undefined,
  };
}

export function ChatSession() {
  const { t } = useI18n();
  const { sessionId: sessionIdParam } = useParams();
  const navigate = useNavigate();
  const auth = useAuth();
  const { datasetId, setDatasetId, refreshDatasets, datasets } = useDataset();

  const [composerMode, setComposerMode] = useState<ComposerMode>('voice');
  const [recordingState, setRecordingState] = useState<RecordingState>('idle');
  const [recordingDuration, setRecordingDuration] = useState(0);
  const [messages, setMessages] = useState<Message[]>([]);
  const [chatDisplayName, setChatDisplayName] = useState(() => localStorage.getItem(CHAT_NAME_STORAGE_KEY) || '');
  const [nameDraft, setNameDraft] = useState('');
  const [datasetContext, setDatasetContext] = useState<DatasetChatContext | null>(null);
  const [datasetContextLoading, setDatasetContextLoading] = useState(false);
  const [datasetSuggestions, setDatasetSuggestions] = useState<DatasetSuggestion[]>([]);
  const [datasetChangeNoticeId, setDatasetChangeNoticeId] = useState<number | 'none' | null>(null);
  const [suggestionsExpanded, setSuggestionsExpanded] = useState(false);
  const [audioTranscriptOpen, setAudioTranscriptOpen] = useState<Record<string, boolean>>({});
  const [savedMessageIds, setSavedMessageIds] = useState<Record<string, number>>({});
  const [waveformLevels, setWaveformLevels] = useState<number[]>(() => silentWaveform());
  const [textDraft, setTextDraft] = useState('');
  const [sending, setSending] = useState(false);
  const recordingIntervalRef = useRef<ReturnType<typeof setInterval>>();
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const mediaStreamRef = useRef<MediaStream | null>(null);
  const audioContextRef = useRef<AudioContext | null>(null);
  const analyserRef = useRef<AnalyserNode | null>(null);
  const waveformFrameRef = useRef<number | null>(null);
  const waveformDataRef = useRef<Uint8Array | null>(null);
  const waveformWriteIndexRef = useRef(0);
  const audioChunksRef = useRef<BlobPart[]>([]);
  const playbackAudioRef = useRef<HTMLAudioElement | null>(null);
  const playbackUrlRef = useRef<string | null>(null);
  const sentVoiceAudioUrlsRef = useRef<Map<string, string>>(new Map());
  const [recordedBlob, setRecordedBlob] = useState<Blob | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const ranSavedRef = useRef(false);
  const scrollRootRef = useRef<HTMLDivElement | null>(null);
  const lastSessionRef = useRef<string | null>(null);
  const previousDatasetIdRef = useRef<number | null | undefined>(undefined);
  const forceScrollRef = useRef(false);
  const bottomRef = useRef<HTMLDivElement | null>(null);

  const activeDatasetName = useMemo(
    () => datasets.find((item) => item.id === datasetId)?.name ?? datasetContext?.name ?? null,
    [datasets, datasetId, datasetContext]
  );
  const activeDatasetRows = datasetContext?.row_count ?? null;
  const activeDatasetColumns = datasetColumnCount(datasetContext);
  const activeDatasetType = datasetContext?.bigdata_sample ? 'Big Data sample' : 'Standard dataset';
  const capabilityGroups = useMemo(() => classifyCapabilities(datasetContext?.columns ?? []), [datasetContext]);
  const introSuggestionGroups = useMemo(() => groupSuggestions(datasetSuggestions), [datasetSuggestions]);
  const visibleNextSuggestions = suggestionsExpanded ? datasetSuggestions : datasetSuggestions.slice(0, 6);
  const nextSuggestionGroups = useMemo(() => groupSuggestions(visibleNextSuggestions), [visibleNextSuggestions]);
  const greetingName =
    chatDisplayName ||
    (auth.state.status === 'authenticated' ? auth.state.user.nickname : '');
  const shouldAskName = !chatDisplayName;

  const loadMessages = useCallback(async (sid: number) => {
    try {
      const { messages: rows } = await listChatMessages(sid);
      setMessages(
        rows.map((row) => {
          const mapped = mapDtoToMessage(row);
          const localUrl = sentVoiceAudioUrlsRef.current.get(String(row.id));
          return localUrl ? { ...mapped, audioUrl: localUrl } : mapped;
        })
      );
    } catch {
      setMessages([]);
      toast.error(t('chat.msgLoadError'));
    }
  }, [t]);

  const loadSavedMessageIds = useCallback(async () => {
    const sourceSessionId = sessionIdParam ? parseInt(sessionIdParam, 10) : null;
    if (!Number.isFinite(sourceSessionId)) {
      setSavedMessageIds({});
      return;
    }
    try {
      const { queries } = await listSavedQueries();
      const next: Record<string, number> = {};
      for (const query of queries) {
        const result = query.result_json;
        if (!result || typeof result !== 'object') continue;
        const meta = (result as Record<string, unknown>).saved_meta;
        if (!meta || typeof meta !== 'object') continue;
        const rec = meta as Record<string, unknown>;
        if (rec.session_id !== sourceSessionId || typeof rec.message_id !== 'string') continue;
        next[rec.message_id] = query.id;
      }
      setSavedMessageIds(next);
    } catch {
      setSavedMessageIds({});
    }
  }, [sessionIdParam]);

  useEffect(() => {
    if (!sessionIdParam) {
      setMessages([]);
      setSavedMessageIds({});
      return;
    }
    const sid = parseInt(sessionIdParam, 10);
    if (!Number.isFinite(sid)) return;
    void loadMessages(sid);
    void loadSavedMessageIds();
  }, [sessionIdParam, loadMessages, loadSavedMessageIds]);

  useEffect(() => {
    const previous = previousDatasetIdRef.current;
    if (previous !== undefined && previous !== datasetId && messages.length > 0) {
      setDatasetChangeNoticeId(datasetId ?? 'none');
    }
    previousDatasetIdRef.current = datasetId ?? null;
  }, [datasetId, messages.length]);

  useEffect(() => {
    return () => {
      if (recordingIntervalRef.current) clearInterval(recordingIntervalRef.current);
      if (waveformFrameRef.current != null) cancelAnimationFrame(waveformFrameRef.current);
      audioContextRef.current?.close().catch(() => undefined);
      playbackAudioRef.current?.pause();
      if (playbackUrlRef.current) URL.revokeObjectURL(playbackUrlRef.current);
      sentVoiceAudioUrlsRef.current.forEach((url) => URL.revokeObjectURL(url));
      mediaRecorderRef.current?.stop();
      mediaStreamRef.current?.getTracks().forEach((t) => t.stop());
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setDatasetContext(null);
    setDatasetSuggestions([]);
    setSuggestionsExpanded(false);
    if (datasetId == null) {
      setDatasetContextLoading(false);
      return;
    }
    setDatasetContextLoading(true);
    void Promise.allSettled([getDatasetChatContext(datasetId), getDatasetSuggestions(datasetId)])
      .then(([contextResult, suggestionsResult]) => {
        if (cancelled) return;
        setDatasetContext(contextResult.status === 'fulfilled' ? contextResult.value : null);
        setDatasetSuggestions(
          suggestionsResult.status === 'fulfilled' ? suggestionsResult.value.suggestions : []
        );
      })
      .catch(() => {
        if (!cancelled) {
          setDatasetContext(null);
          setDatasetSuggestions([]);
        }
      })
      .finally(() => {
        if (!cancelled) setDatasetContextLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [datasetId]);

  const saveChatDisplayName = useCallback((value: string) => {
    const clean = value.trim().slice(0, 40);
    if (!clean) return;
    localStorage.setItem(CHAT_NAME_STORAGE_KEY, clean);
    setChatDisplayName(clean);
    setNameDraft('');
    toast.success(`Nice to meet you, ${clean}`);
  }, []);

  const sendPipeline = useCallback(
    async (text: string, overrideDatasetId?: number | null) => {
      const q = text.trim();
      if (!q) return;
      const queryForApi = mergeFollowUpDataQuery(messages, q);
      const dsId = overrideDatasetId ?? datasetId;
      let conversationIntent = detectConversationIntent(q);
      const normalizedConversationText = normalizeChatText(q);
      if (/^(help|how to use)$/.test(normalizedConversationText)) {
        conversationIntent = { kind: 'help' };
      } else if (/\b(what can i ask|what can i ask about this dataset|suggest queries|suggest questions|help with this dataset)\b/.test(normalizedConversationText)) {
        conversationIntent = { kind: 'dataset_help' };
      }

      setSending(true);
      setSuggestionsExpanded(false);
      let activeSid: number | null = null;
      try {
        let sid = sessionIdParam ? parseInt(sessionIdParam, 10) : NaN;
        if (!Number.isFinite(sid)) {
          const title =
            conversationIntent.kind === 'none'
              ? q.slice(0, 80) || t('chat.newChatTitle')
              : activeDatasetName
                ? `${activeDatasetName} assistant`
                : 'AI assistant chat';
          const created = await createChatSession(title);
          sid = created.id;
          navigate(`/session/${sid}`, { replace: true });
        }
        activeSid = sid;

        await appendChatMessage(sid, { role: 'user', content: q });

        if (conversationIntent.kind !== 'none') {
          if (conversationIntent.kind === 'name') {
            localStorage.setItem(CHAT_NAME_STORAGE_KEY, conversationIntent.name);
            setChatDisplayName(conversationIntent.name);
            setNameDraft('');
          }
          const response = buildConversationResponse(conversationIntent, {
            name: greetingName,
            hasDataset: dsId != null,
            suggestions: datasetSuggestions,
          });
          await appendChatMessage(sid, {
            role: 'assistant',
            content: response,
            meta_json: {
              conversational: true,
              intent: conversationIntent.kind,
            },
          });
          await loadMessages(sid);
          return;
        }

        if (dsId == null) {
          toast.error(t('chat.selectDataset'));
          await loadMessages(sid);
          return;
        }

        const startedAt = performance.now();
        const answer = await postQueryAnswer({
          dataset_id: dsId,
          input: { type: 'text', text: queryForApi },
          options: { limit: 20, explain: true, confidence_threshold: 0.55 },
        });
        const responseTimeMs = performance.now() - startedAt;

        const assistantMessage = await appendChatMessage(sid, {
          role: 'assistant',
          content: answer.answer_text,
          meta_json: {
            answer: answer as unknown as Record<string, unknown>,
            userQuery: queryForApi,
            sql: answer.sql,
          },
        });

        await loadMessages(sid);
        setMessages((prev) =>
          prev.map((message) =>
            message.id === String(assistantMessage.id)
              ? { ...message, responseTimeMs }
              : message
          )
        );
      } catch (e) {
        if (activeSid != null) {
          await appendChatMessage(activeSid, {
            role: 'assistant',
            content:
              'I could not confidently understand this request. Try asking about averages, counts, top values, distributions, or select one of the suggested questions.',
            meta_json: {
              conversational: true,
              intent: 'unclear',
              error: e instanceof Error ? e.message : String(e),
            },
          }).catch(() => undefined);
          await loadMessages(activeSid).catch(() => undefined);
        }
        toast.error(e instanceof Error ? e.message : t('chat.requestError'));
      } finally {
        setSending(false);
        setTextDraft('');
      }
    },
    [datasetId, sessionIdParam, navigate, loadMessages, messages, t, greetingName, datasetSuggestions, activeDatasetName]
  );

  useEffect(() => {
    if (ranSavedRef.current) return;
    const raw = sessionStorage.getItem('runSavedQuery');
    if (!raw) return;
    ranSavedRef.current = true;
    sessionStorage.removeItem('runSavedQuery');
    try {
      const parsed = JSON.parse(raw) as { text: string; datasetId?: number | null };
      if (typeof parsed.datasetId === 'number') setDatasetId(parsed.datasetId);
      void sendPipeline(parsed.text, parsed.datasetId ?? undefined);
    } catch {
      /* ignore */
    }
  }, [sendPipeline, setDatasetId]);

  const scrollToBottom = useCallback((behavior: ScrollBehavior = 'auto') => {
    const el = bottomRef.current;
    if (!el) return;
    try {
      el.scrollIntoView({ block: 'end', behavior });
    } catch {
      // ignore
    }
  }, []);

  const isNearBottom = useCallback(() => {
    const root = scrollRootRef.current;
    if (!root) return true;
    const viewport = root.querySelector('[data-slot="scroll-area-viewport"]') as HTMLElement | null;
    if (!viewport) return true;
    const remaining = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight;
    return remaining < 120;
  }, []);

  // Mark that we must jump to bottom after session changes (after messages load).
  useEffect(() => {
    forceScrollRef.current = true;
  }, [sessionIdParam]);

  // Auto-scroll: after opening a session (first load) always; later only if user is near bottom.
  useEffect(() => {
    const sid = sessionIdParam ?? null;
    const isNewChatOpen = sid !== lastSessionRef.current;
    lastSessionRef.current = sid;

    const shouldForce = forceScrollRef.current && messages.length > 0;
    const shouldStick = shouldForce || isNewChatOpen || isNearBottom();
    if (!shouldStick) return;

    if (shouldForce) forceScrollRef.current = false;

    // Let layout settle (tables, async font metrics, etc), then jump.
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        setTimeout(() => {
          scrollToBottom(shouldForce || isNewChatOpen ? 'auto' : 'smooth');
        }, 60);
      });
    });
  }, [messages.length, sessionIdParam, isNearBottom, scrollToBottom]);

  useEffect(() => {
    if (!messages.length) return;
    const targetId = sessionStorage.getItem('focusSavedMessageId');
    if (!targetId) return;
    sessionStorage.removeItem('focusSavedMessageId');
    requestAnimationFrame(() => {
      const el = document.getElementById(`message-${targetId}`);
      if (!el) return;
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
      el.classList.add('ring-2', 'ring-violet-400/70', 'ring-offset-2', 'ring-offset-background');
      setTimeout(() => {
        el.classList.remove('ring-2', 'ring-violet-400/70', 'ring-offset-2', 'ring-offset-background');
      }, 1600);
    });
  }, [messages.length]);

  const onSubmitText = (e?: React.FormEvent) => {
    e?.preventDefault();
    if (sending) return;
    void sendPipeline(textDraft);
  };

  const uploadDatasetForSession = async (file: File) => {
    if (!file) return;
    const ok = /\.(csv|xlsx|json)$/i.test(file.name);
    if (!ok) {
      toast.error('Upload a CSV, XLSX, or JSON dataset');
      return;
    }
    setSending(true);
    setSuggestionsExpanded(false);
    try {
      const baseName = file.name.replace(/\.(csv|xlsx|json)$/i, '').trim() || t('datasets.importedCsvName');
      const created = await createDataset(baseName);
      await importCsvFile(created.dataset_id, file);
      await refreshDatasets();
      setDatasetId(created.dataset_id);
      toast.success(`${t('common.done')} #${created.dataset_id}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('chat.importFailed'));
    } finally {
      setSending(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const saveAssistant = async (m: Message) => {
    if (!m.answerPayload || !m.userQuery) {
      toast.error(t('chat.noAnswerData'));
      return;
    }
    try {
      const existingSavedId = savedMessageIds[m.id];
      if (existingSavedId) {
        await deleteSavedQuery(existingSavedId);
        setSavedMessageIds((prev) => {
          const next = { ...prev };
          delete next[m.id];
          return next;
        });
        toast.success(t('common.delete'));
        return;
      }
      const sql = (m.answerPayload as { sql?: string }).sql;
      const answerText = (m.answerPayload as { answer_text?: string }).answer_text ?? m.content;
      const sourceSessionId = sessionIdParam ? parseInt(sessionIdParam, 10) : null;
      const saved = await createSavedQuery({
        title: m.userQuery.slice(0, 120) || 'Saved query',
        query_text: m.userQuery,
        sql_text: sql ?? null,
        answer_text: answerText,
        result_json: {
          ...m.answerPayload,
          saved_meta: {
            session_id: Number.isFinite(sourceSessionId) ? sourceSessionId : null,
            message_id: m.id,
          },
        },
        dataset_id: datasetId,
      });
      setSavedMessageIds((prev) => ({ ...prev, [m.id]: saved.id }));
      toast.success(t('chat.savedQueriesSaved'));
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('chat.saveFailed'));
    }
  };

  const stopAudioVisualization = useCallback(() => {
    if (waveformFrameRef.current != null) {
      cancelAnimationFrame(waveformFrameRef.current);
      waveformFrameRef.current = null;
    }
    analyserRef.current = null;
    waveformDataRef.current = null;
    if (audioContextRef.current) {
      audioContextRef.current.close().catch(() => undefined);
      audioContextRef.current = null;
    }
  }, []);

  const stopPlayback = useCallback(() => {
    playbackAudioRef.current?.pause();
    playbackAudioRef.current = null;
    if (playbackUrlRef.current) {
      URL.revokeObjectURL(playbackUrlRef.current);
      playbackUrlRef.current = null;
    }
  }, []);

  const startAudioVisualization = useCallback((stream: MediaStream) => {
    stopAudioVisualization();
    waveformWriteIndexRef.current = 0;
    setWaveformLevels(silentWaveform());
    try {
      const win = window as typeof window & { webkitAudioContext?: typeof AudioContext };
      const AudioCtx = window.AudioContext || win.webkitAudioContext;
      if (!AudioCtx) return;

      const audioContext = new AudioCtx();
      const analyser = audioContext.createAnalyser();
      const source = audioContext.createMediaStreamSource(stream);
      analyser.fftSize = 256;
      analyser.smoothingTimeConstant = 0.84;
      source.connect(analyser);

      const data = new Uint8Array(analyser.fftSize);
      audioContextRef.current = audioContext;
      analyserRef.current = analyser;
      waveformDataRef.current = data;

      const tick = () => {
        const currentAnalyser = analyserRef.current;
        const currentData = waveformDataRef.current;
        if (!currentAnalyser || !currentData) return;

        currentAnalyser.getByteTimeDomainData(currentData);
        let sum = 0;
        for (let i = 0; i < currentData.length; i++) {
          const centered = (currentData[i] - 128) / 128;
          sum += centered * centered;
        }
        const rms = Math.sqrt(sum / currentData.length);
        const noiseFloor = 0.012;
        const normalized = rms <= noiseFloor ? 0 : Math.pow(Math.min(1, (rms - noiseFloor) / 0.16), 0.72);
        const level = Math.max(0.035, normalized);
        setWaveformLevels((prev) => {
          const next = prev.slice();
          const idx = waveformWriteIndexRef.current;
          if (idx < WAVEFORM_BAR_COUNT) {
            next[idx] = level;
            waveformWriteIndexRef.current = idx + 1;
            return next;
          }
          next.shift();
          next.push(level);
          return next;
        });
        waveformFrameRef.current = requestAnimationFrame(tick);
      };

      waveformFrameRef.current = requestAnimationFrame(tick);
    } catch {
      setWaveformLevels(silentWaveform());
    }
  }, [stopAudioVisualization]);

  const startRecording = async () => {
    if (!navigator.mediaDevices?.getUserMedia) {
      toast.error(t('chat.micUnsupported'));
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      mediaStreamRef.current = stream;
      audioChunksRef.current = [];
      startAudioVisualization(stream);
      const recorder = new MediaRecorder(stream, { mimeType: 'audio/webm' });
      mediaRecorderRef.current = recorder;
      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0) audioChunksRef.current.push(e.data);
      };
      recorder.onstop = () => {
        const blob = new Blob(audioChunksRef.current, { type: 'audio/webm' });
        setRecordedBlob(blob.size > 0 ? blob : null);
        mediaStreamRef.current?.getTracks().forEach((t) => t.stop());
        mediaStreamRef.current = null;
      };
      recorder.start();
      setRecordingState('recording');
      setRecordingDuration(0);
      if (recordingIntervalRef.current) clearInterval(recordingIntervalRef.current);
      recordingIntervalRef.current = setInterval(() => {
        setRecordingDuration((prev) => prev + 0.1);
      }, 100);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('chat.micAccessFailed'));
    }
  };

  const stopRecording = () => {
    if (recordingIntervalRef.current) clearInterval(recordingIntervalRef.current);
    stopAudioVisualization();
    mediaRecorderRef.current?.stop();
    setRecordingState('recorded');
  };

  const playRecording = () => {
    if (!recordedBlob) {
      toast.error(t('chat.noRecordedAudio'));
      return;
    }
    if (recordingState === 'playing') {
      stopPlayback();
      setRecordingState('recorded');
      return;
    }

    stopPlayback();
    const url = URL.createObjectURL(recordedBlob);
    const audio = new Audio(url);
    playbackUrlRef.current = url;
    playbackAudioRef.current = audio;
    audio.onended = () => {
      stopPlayback();
      setRecordingState('recorded');
    };
    audio.onerror = () => {
      stopPlayback();
      setRecordingState('recorded');
      toast.error(t('chat.voiceRequestFailed'));
    };
    setRecordingState('playing');
    void audio.play().catch((err) => {
      stopPlayback();
      setRecordingState('recorded');
      toast.error(err instanceof Error ? err.message : t('chat.voiceRequestFailed'));
    });
  };

  const playMessageAudio = (message: Message) => {
    if (!message.audioUrl) return;
    stopPlayback();
    const audio = new Audio(message.audioUrl);
    playbackAudioRef.current = audio;
    audio.onended = () => {
      playbackAudioRef.current = null;
    };
    audio.onerror = () => {
      playbackAudioRef.current = null;
      toast.error(t('chat.voiceRequestFailed'));
    };
    void audio.play().catch((err) => {
      playbackAudioRef.current = null;
      toast.error(err instanceof Error ? err.message : t('chat.voiceRequestFailed'));
    });
  };

  const deleteRecording = () => {
    stopAudioVisualization();
    stopPlayback();
    waveformWriteIndexRef.current = 0;
    setRecordingState('idle');
    setRecordingDuration(0);
    setRecordedBlob(null);
    audioChunksRef.current = [];
    setWaveformLevels(silentWaveform());
  };

  const sendRecording = async () => {
    stopPlayback();
    if (!recordedBlob) {
      toast.error(t('chat.noRecordedAudio'));
      return;
    }
    if (datasetId == null) {
      toast.error(t('chat.selectDataset'));
      return;
    }
    setSending(true);
    try {
      let sid = sessionIdParam ? parseInt(sessionIdParam, 10) : NaN;
      if (!Number.isFinite(sid)) {
        const created = await createChatSession(t('chat.voiceRequestTitle'));
        sid = created.id;
        navigate(`/session/${sid}`, { replace: true });
      }

      const asr = await transcribeAudio(recordedBlob);
      if (asr.status !== 'done' || typeof asr.job_id !== 'number') {
        const raw = asr.error || t('chat.asrFailed');
        const msg =
          /OPENAI_API_KEY|ASR is disabled/i.test(raw) ? t('chat.asrNeedApiKey') : raw;
        throw new Error(msg);
      }

      const rawAsr = (asr.text || '').trim();
      const voiceText = (rawAsr ? normalizeVoiceText(rawAsr) : '') || rawAsr || t('chat.voiceMessageFallback');
      const localAudioUrl = URL.createObjectURL(recordedBlob);
      const voiceMessage = await appendChatMessage(sid, {
        role: 'user',
        content: 'Voice message',
        meta_json: {
          userQuery: voiceText,
          voice: {
            transcribed_text: voiceText,
            waveform_levels: waveformLevels,
            duration: recordingDuration,
            asr_job_id: asr.job_id,
          },
        },
      });
      sentVoiceAudioUrlsRef.current.set(String(voiceMessage.id), localAudioUrl);

      let voiceConversationIntent = detectConversationIntent(voiceText);
      const normalizedVoiceText = normalizeChatText(voiceText);
      if (/^(help|how to use)$/.test(normalizedVoiceText)) {
        voiceConversationIntent = { kind: 'help' };
      } else if (/\b(what can i ask|what can i ask about this dataset|suggest queries|suggest questions|help with this dataset)\b/.test(normalizedVoiceText)) {
        voiceConversationIntent = { kind: 'dataset_help' };
      }
      if (voiceConversationIntent.kind !== 'none') {
        if (voiceConversationIntent.kind === 'name') {
          localStorage.setItem(CHAT_NAME_STORAGE_KEY, voiceConversationIntent.name);
          setChatDisplayName(voiceConversationIntent.name);
          setNameDraft('');
        }
        const response = buildConversationResponse(voiceConversationIntent, {
          name: greetingName,
          hasDataset: datasetId != null,
          suggestions: datasetSuggestions,
        });
        await appendChatMessage(sid, {
          role: 'assistant',
          content: response,
          meta_json: {
            conversational: true,
            intent: voiceConversationIntent.kind,
            userQuery: voiceText,
            asr_job_id: asr.job_id,
          },
        });
        await loadMessages(sid);
        setRecordingState('idle');
        setRecordingDuration(0);
        setRecordedBlob(null);
        waveformWriteIndexRef.current = 0;
        setWaveformLevels(silentWaveform());
        audioChunksRef.current = [];
        toast.success(t('chat.recordingSent'));
        return;
      }

      const startedAt = performance.now();
      const answer = await postQueryAnswer({
        dataset_id: datasetId,
        input: { type: 'voice', job_id: asr.job_id },
        options: { limit: 20, explain: true, confidence_threshold: 0.55 },
      });
      const responseTimeMs = performance.now() - startedAt;

      const assistantMessage = await appendChatMessage(sid, {
        role: 'assistant',
        content: answer.answer_text,
        meta_json: {
          answer: answer as unknown as Record<string, unknown>,
          userQuery: voiceText,
          sql: answer.sql,
          asr_job_id: asr.job_id,
        },
      });
      await loadMessages(sid);
      setMessages((prev) =>
        prev.map((message) =>
          message.id === String(assistantMessage.id)
            ? { ...message, responseTimeMs }
            : message
        )
      );
      setRecordingState('idle');
      setRecordingDuration(0);
      setRecordedBlob(null);
      waveformWriteIndexRef.current = 0;
      setWaveformLevels(silentWaveform());
      audioChunksRef.current = [];
      toast.success(t('chat.recordingSent'));
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t('chat.voiceRequestFailed'));
    } finally {
      setSending(false);
    }
  };

  const formatDuration = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return `${mins}:${secs.toString().padStart(2, '0')}`;
  };

  const runSuggestion = (suggestion: DatasetSuggestion) => {
    if (sending) return;
    setComposerMode('text');
    setSuggestionsExpanded(false);
    void sendPipeline(suggestion.query);
  };

  const isNewSession = !sessionIdParam && messages.length === 0;

  return (
    <div className="h-full flex flex-col">
      <AnimatePresence>
        {isNewSession && (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            transition={{ duration: 0.5 }}
            className="flex-1 overflow-auto p-6 sm:p-8"
          >
            <div className="mx-auto grid w-full max-w-5xl items-start gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(300px,0.9fr)]">
              <div className="min-w-0 pt-4 text-center lg:text-left">
                <h2 className="text-3xl font-semibold bg-gradient-to-r from-purple-400 to-violet-400 bg-clip-text text-transparent">
                  {greetingName ? `Hi, ${greetingName}.` : 'Hi! How should I address you?'}
                </h2>
                <p className="mt-3 text-muted-foreground text-lg">
                  {datasetId == null
                    ? 'No dataset is selected yet.'
                    : activeDatasetName
                      ? `I can help you explore ${activeDatasetName} in plain language.`
                      : t('chat.welcome')}
                </p>

                <div className="mx-auto mt-5 max-w-xl rounded-2xl border border-white/10 bg-card/85 p-4 text-left lg:mx-0">
                  <div className="flex items-start gap-3">
                    <AssistantMascot size="sm" className="mt-0.5" />
                    <div className="min-w-0 flex-1">
                      {datasetId == null ? (
                        <>
                          <p className="text-sm font-semibold text-white">Hi 👋</p>
                          <p className="mt-1 text-sm leading-6 text-white/65">
                            No dataset is selected yet. Please upload a dataset or choose one from the selector above to start analysis.
                          </p>
                        </>
                      ) : datasetContext ? (
                        <>
                          <p className="text-sm font-semibold text-white">
                            {greetingName ? `Hi, ${greetingName} 👋` : 'Hi 👋'}
                          </p>
                          <p className="mt-1 text-sm leading-6 text-white/65">
                            I am connected to the selected dataset.
                          </p>
                          <div className="mt-3 grid gap-2 text-xs text-white/70 sm:grid-cols-3">
                            <div className="rounded-xl border border-white/10 bg-white/[0.04] p-2">
                              <p className="text-white/40">Dataset</p>
                              <p className="mt-1 truncate font-medium text-white">{datasetContext.name}</p>
                            </div>
                            <div className="rounded-xl border border-white/10 bg-white/[0.04] p-2">
                              <p className="text-white/40">Records</p>
                              <p className="mt-1 font-medium text-white">{formatDatasetCount(datasetContext.row_count)}</p>
                            </div>
                            <div className="rounded-xl border border-white/10 bg-white/[0.04] p-2">
                              <p className="text-white/40">Columns</p>
                              <p className="mt-1 font-medium text-white">{formatDatasetCount(datasetColumnCount(datasetContext))}</p>
                            </div>
                          </div>
                          <p className="mt-3 text-sm leading-6 text-white/65">
                            You can ask questions about this dataset or choose one of the suggested analyses below.
                          </p>
                        </>
                      ) : (
                        <>
                          <p className="text-sm font-semibold text-white">Connecting to dataset...</p>
                          <p className="mt-1 text-sm leading-6 text-white/65">
                            I am loading the selected dataset metadata before suggesting analyses.
                          </p>
                        </>
                      )}
                    </div>
                  </div>
                </div>

                {shouldAskName && (
                  <form
                    className="mx-auto mt-5 flex max-w-md gap-2 lg:mx-0"
                    onSubmit={(e) => {
                      e.preventDefault();
                      saveChatDisplayName(nameDraft);
                    }}
                  >
                    <Input
                      value={nameDraft}
                      onChange={(e) => setNameDraft(e.target.value)}
                      placeholder="Your name"
                      className="h-11 rounded-xl border-white/10 bg-white/[0.04]"
                    />
                    <Button
                      type="submit"
                      disabled={!nameDraft.trim()}
                      className="h-11 rounded-xl bg-gradient-to-br from-purple-500 to-violet-600 px-5"
                    >
                      Save
                    </Button>
                    {auth.state.status === 'authenticated' && auth.state.user.nickname && (
                      <Button
                        type="button"
                        variant="ghost"
                        className="h-11 rounded-xl border border-white/10 bg-white/[0.04]"
                        onClick={() => saveChatDisplayName(auth.state.user.nickname)}
                      >
                        Use {auth.state.user.nickname}
                      </Button>
                    )}
                  </form>
                )}

                <motion.div
                  initial={{ opacity: 0, scale: 0.88, y: 12 }}
                  animate={{ opacity: 1, scale: 1, y: 0 }}
                  transition={{ delay: 0.15, type: 'spring', stiffness: 220, damping: 20 }}
                  className="mt-7 flex justify-center lg:justify-start"
                >
                  <AssistantMascot />
                </motion.div>
              </div>

              <div className="space-y-4">
                <div className="rounded-2xl border border-white/10 bg-card/85 p-4 shadow-[0_18px_55px_rgba(0,0,0,0.20)]">
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <div className="flex min-w-0 items-center gap-2">
                      <Database className="h-4 w-4 shrink-0 text-violet-300" />
                      <h3 className="truncate text-sm font-semibold text-white">What you can do with this dataset</h3>
                    </div>
                    {datasetContextLoading && <Loader2 className="h-4 w-4 animate-spin text-white/50" />}
                  </div>
                  {datasetId == null ? (
                    <p className="text-sm text-white/55">Choose a dataset to see suggested questions and fields.</p>
                  ) : (
                    <div className="space-y-3">
                      {datasetContext?.bigdata_sample && (
                        <div className="rounded-xl border border-violet-400/20 bg-violet-500/10 px-3 py-2">
                          <div className="flex flex-wrap items-center gap-2">
                            <Badge className="bg-violet-500/20 text-violet-100">Created from Big Data sample</Badge>
                            {datasetContext.bigdata_sample.source_dataset_name && (
                              <span className="text-xs text-white/60">
                                Source: {datasetContext.bigdata_sample.source_dataset_name}
                              </span>
                            )}
                            {datasetContext.bigdata_sample.rows_sampled != null && (
                              <span className="text-xs text-white/60">
                                Rows: {datasetContext.bigdata_sample.rows_sampled.toLocaleString()}
                              </span>
                            )}
                          </div>
                          {Array.isArray(datasetContext.bigdata_sample.selected_columns) && (
                            <p className="mt-2 truncate text-xs text-white/55">
                              Columns: {datasetContext.bigdata_sample.selected_columns.join(', ')}
                            </p>
                          )}
                          {Array.isArray(datasetContext.bigdata_sample.applied_filters) &&
                            datasetContext.bigdata_sample.applied_filters.length > 0 && (
                              <p className="mt-1 truncate text-xs text-white/55">
                                Filters: {datasetContext.bigdata_sample.applied_filters
                                  .map((f) => `${f.column} ${f.operator} ${f.value}`)
                                  .join(', ')}
                              </p>
                            )}
                        </div>
                      )}
                      <div className="grid gap-3 sm:grid-cols-2">
                        <FieldList label="Numeric fields" values={capabilityGroups.numeric} />
                        <FieldList label="Categories" values={capabilityGroups.categorical} />
                        <FieldList label="Text fields" values={capabilityGroups.text} />
                        <FieldList label="Date fields" values={capabilityGroups.date} />
                      </div>
                      {!datasetContextLoading &&
                        !capabilityGroups.numeric.length &&
                        !capabilityGroups.categorical.length &&
                        !capabilityGroups.text.length &&
                        !capabilityGroups.date.length && (
                          <p className="text-sm text-white/55">
                            I could not read field details yet, but you can still ask analytical questions.
                          </p>
                        )}
                    </div>
                  )}
                </div>

                {datasetSuggestions.length > 0 && (
                  <div className="rounded-2xl border border-white/10 bg-card/85 p-4">
                    <div className="mb-3 flex items-center gap-2">
                      <Sparkles className="h-4 w-4 text-violet-300" />
                      <h3 className="text-sm font-semibold text-white">Useful questions for this dataset</h3>
                    </div>
                    <p className="mb-3 text-xs leading-relaxed text-white/50">
                      Generated from the selected dataset columns
                      {datasetContext?.bigdata_sample
                        ? ` sample (${(datasetContext.bigdata_sample.rows_sampled ?? datasetContext.row_count ?? 0).toLocaleString()} rows).`
                        : datasetContext?.row_count != null
                          ? ` (${datasetContext.row_count.toLocaleString()} rows).`
                          : '.'}
                    </p>
                    <div className="space-y-3">
                      {introSuggestionGroups.map(([category, suggestions]) => (
                        <div key={category}>
                          <p className="mb-2 text-[11px] font-semibold uppercase tracking-normal text-white/40">
                            {category}
                          </p>
                          <div className="flex flex-wrap gap-2">
                            {suggestions.map((suggestion) => (
                              <button
                                key={`${suggestion.category}-${suggestion.query}`}
                                type="button"
                                onClick={() => runSuggestion(suggestion)}
                                disabled={sending || datasetId == null}
                                title={suggestion.explanation}
                                className="rounded-full border border-violet-300/20 bg-violet-500/10 px-3 py-1.5 text-left text-sm text-violet-50 transition hover:border-violet-300/40 hover:bg-violet-500/18 disabled:cursor-not-allowed disabled:opacity-55"
                              >
                                {suggestion.title}
                              </button>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {(!isNewSession || messages.length > 0) && (
        <div ref={scrollRootRef} className="flex-1 min-h-0">
          <ScrollArea className="h-full px-4 py-6">
            <div className="max-w-4xl mx-auto space-y-6">
              {(shouldAskName || datasetContext?.bigdata_sample) && (
                <div className="rounded-2xl border border-white/10 bg-card/80 p-4">
                  {shouldAskName && (
                    <form
                      className="mb-4 flex flex-col gap-2 sm:flex-row"
                      onSubmit={(e) => {
                        e.preventDefault();
                        saveChatDisplayName(nameDraft);
                      }}
                    >
                      <div className="min-w-0 flex-1">
                        <p className="mb-2 text-sm font-semibold text-white">Hi! How should I address you?</p>
                        <Input
                          value={nameDraft}
                          onChange={(e) => setNameDraft(e.target.value)}
                          placeholder="Your name"
                          className="h-10 rounded-xl border-white/10 bg-white/[0.04]"
                        />
                      </div>
                      <Button
                        type="submit"
                        disabled={!nameDraft.trim()}
                        className="self-end rounded-xl bg-gradient-to-br from-purple-500 to-violet-600"
                      >
                        Save
                      </Button>
                    </form>
                  )}
                  {datasetContext?.bigdata_sample && (
                    <div className="mb-3 flex flex-wrap items-center gap-2 text-xs text-white/60">
                      <Badge className="bg-violet-500/20 text-violet-100">Created from Big Data sample</Badge>
                      {datasetContext.bigdata_sample.source_dataset_name && (
                        <span>Source: {datasetContext.bigdata_sample.source_dataset_name}</span>
                      )}
                      {datasetContext.bigdata_sample.rows_sampled != null && (
                        <span>Rows: {datasetContext.bigdata_sample.rows_sampled.toLocaleString()}</span>
                      )}
                    </div>
                  )}
                </div>
              )}
              {datasetChangeNoticeId !== null && (
                <div className="ml-11 rounded-2xl border border-violet-300/20 bg-violet-500/10 p-3 text-sm text-violet-50">
                  {datasetChangeNoticeId === 'none' || datasetId == null ? (
                    <p>Dataset changed. No dataset is selected now.</p>
                  ) : datasetContext ? (
                    <p>
                      Dataset changed. I am now connected to: <span className="font-semibold">{datasetContext.name}</span>
                      {datasetContext.row_count != null ? ` · Records: ${datasetContext.row_count.toLocaleString()}` : ''}
                      {datasetColumnCount(datasetContext) != null ? ` · Columns: ${datasetColumnCount(datasetContext)}` : ''}
                    </p>
                  ) : (
                    <p>Dataset changed. Loading the selected dataset metadata...</p>
                  )}
                </div>
              )}
              <div className="rounded-2xl border border-white/10 bg-card/80 p-4">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <div className="flex min-w-0 items-center gap-2">
                    <Database className="h-4 w-4 shrink-0 text-violet-300" />
                    <h3 className="truncate text-sm font-semibold text-white">Current dataset</h3>
                  </div>
                  {datasetContextLoading && <Loader2 className="h-4 w-4 animate-spin text-white/50" />}
                </div>
                {datasetId == null ? (
                  <p className="text-sm text-white/60">
                    No dataset is selected yet. Choose one from the selector above or upload a dataset to start analysis.
                  </p>
                ) : datasetContext ? (
                  <div className="space-y-3">
                    <div className="grid gap-2 text-xs text-white/70 sm:grid-cols-4">
                      <div className="rounded-xl border border-white/10 bg-white/[0.04] p-2 sm:col-span-2">
                        <p className="text-white/40">Name</p>
                        <p className="mt-1 truncate font-medium text-white">{datasetContext.name}</p>
                      </div>
                      <div className="rounded-xl border border-white/10 bg-white/[0.04] p-2">
                        <p className="text-white/40">Rows</p>
                        <p className="mt-1 font-medium text-white">{formatDatasetCount(activeDatasetRows)}</p>
                      </div>
                      <div className="rounded-xl border border-white/10 bg-white/[0.04] p-2">
                        <p className="text-white/40">Columns</p>
                        <p className="mt-1 font-medium text-white">{formatDatasetCount(activeDatasetColumns)}</p>
                      </div>
                    </div>
                    <div className="flex flex-wrap items-center gap-2 text-xs text-white/60">
                      <Badge className="bg-violet-500/20 text-violet-100">{activeDatasetType}</Badge>
                      {datasetContext.bigdata_sample?.source_dataset_name && (
                        <span>Source: {datasetContext.bigdata_sample.source_dataset_name}</span>
                      )}
                      {datasetContext.bigdata_sample?.row_limit != null && (
                        <span>Row limit: {datasetContext.bigdata_sample.row_limit.toLocaleString()}</span>
                      )}
                    </div>
                    {Array.isArray(datasetContext.bigdata_sample?.selected_columns) && datasetContext.bigdata_sample.selected_columns.length > 0 && (
                      <p className="truncate text-xs text-white/50">
                        Selected columns: {datasetContext.bigdata_sample.selected_columns.join(', ')}
                      </p>
                    )}
                    {Array.isArray(datasetContext.bigdata_sample?.applied_filters) && datasetContext.bigdata_sample.applied_filters.length > 0 && (
                      <p className="truncate text-xs text-white/50">
                        Filters: {datasetContext.bigdata_sample.applied_filters.map((f) => `${f.column} ${f.operator} ${f.value}`).join(', ')}
                      </p>
                    )}
                  </div>
                ) : (
                  <p className="text-sm text-white/60">Loading selected dataset metadata before analysis...</p>
                )}
              </div>
              {messages.map((message) => (
                <motion.div
                  key={message.id}
                  id={`message-${message.id}`}
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.3 }}
                  className={cn('flex items-start gap-3', message.type === 'user' ? 'justify-end' : 'justify-start')}
                >
                  {message.type === 'assistant' && <AssistantMascot size="sm" className="mt-1" />}
                  <div
                    className={cn(
                      'max-w-[85%] rounded-2xl px-4 py-3 relative group/msg',
                      message.type === 'user'
                        ? 'bg-gradient-to-br from-purple-500 to-violet-600 text-white'
                        : 'bg-card border border-border text-white'
                    )}
                    style={
                      message.type === 'user'
                        ? { boxShadow: '0 8px 24px rgba(168, 85, 247, 0.3)' }
                        : {}
                    }
                  >
                    {message.type === 'assistant' && message.answerPayload && (
                      <motion.button
                        type="button"
                        className={cn(
                          'absolute top-3 right-3 z-10 flex h-7 w-7 items-center justify-center bg-transparent p-0 transition-colors',
                          savedMessageIds[message.id]
                            ? 'text-violet-400 drop-shadow-[0_0_10px_rgba(139,92,246,0.55)]'
                            : 'text-violet-300 hover:text-violet-100'
                        )}
                        title={t('chat.saveToSavedQueries')}
                        onClick={() => void saveAssistant(message)}
                        whileHover={{ scale: 1.07, y: -1 }}
                        whileTap={{ scale: 0.88 }}
                        animate={savedMessageIds[message.id] ? { scale: [1, 1.22, 1] } : { scale: 1 }}
                        transition={{ duration: 0.28, ease: 'easeOut' }}
                        aria-label={t('chat.saveToSavedQueries')}
                      >
                        <Bookmark
                          className="h-6 w-6 transition-all"
                          fill={savedMessageIds[message.id] ? 'currentColor' : 'transparent'}
                          strokeWidth={2.2}
                        />
                      </motion.button>
                    )}
                    {message.isAudio ? (
                      <div className="w-[min(72vw,430px)]">
                        <div className="rounded-[22px] border border-white/16 bg-white/[0.08] px-3 py-2.5 shadow-inner shadow-white/[0.03]">
                          <div className="flex items-center gap-3">
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              disabled={!message.audioUrl}
                              onClick={() => playMessageAudio(message)}
                              className={cn(
                                'h-11 w-11 shrink-0 rounded-full border border-white/18 bg-white/[0.12] text-white shadow-[0_8px_24px_rgba(0,0,0,0.16)] hover:bg-white/[0.22]',
                                !message.audioUrl && 'opacity-50'
                              )}
                              aria-label="Play voice message"
                              title={message.audioUrl ? 'Play voice message' : 'Playback is available only for this sent recording'}
                            >
                              <Play className="ml-0.5 h-4.5 w-4.5" />
                            </Button>
                            <div className="flex h-9 min-w-0 flex-1 items-center justify-center gap-[3px] overflow-hidden">
                              {(message.waveformData?.length ? message.waveformData : silentWaveform()).map((level, i) => (
                                <div
                                  key={i}
                                  className="w-1 rounded-full bg-white/80 shadow-[0_0_10px_rgba(255,255,255,0.12)]"
                                  style={{ height: `${Math.max(3, level * 25)}px` }}
                                />
                              ))}
                            </div>
                            <span className="w-9 text-right text-xs tabular-nums text-white/72">
                              {formatDuration(message.duration ?? 0)}
                            </span>
                          </div>
                        </div>
                        <div className="mt-2 flex items-center justify-between px-1">
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={() =>
                                setAudioTranscriptOpen((prev) => ({ ...prev, [message.id]: !prev[message.id] }))
                              }
                              className={cn(
                                'rounded-full border px-3 py-1 text-xs font-semibold transition',
                                audioTranscriptOpen[message.id]
                                  ? 'border-white/28 bg-white/20 text-white'
                                  : 'border-white/16 bg-white/[0.08] text-white/72 hover:bg-white/[0.16] hover:text-white'
                              )}
                              aria-expanded={Boolean(audioTranscriptOpen[message.id])}
                            >
                              Aa
                            </button>
                            <span className="text-xs text-white/45">transcript</span>
                          </div>
                          <span className="text-xs tabular-nums text-white/58">
                            {message.timestamp.toLocaleTimeString([], {
                              hour: '2-digit',
                              minute: '2-digit',
                            })}
                          </span>
                        </div>
                        <AnimatePresence>
                          {audioTranscriptOpen[message.id] && (
                            <motion.p
                              initial={{ opacity: 0, y: -4, height: 0 }}
                              animate={{ opacity: 1, y: 0, height: 'auto' }}
                              exit={{ opacity: 0, y: -4, height: 0 }}
                              transition={{ duration: 0.2 }}
                              className="mt-2 overflow-hidden rounded-2xl border border-white/12 bg-black/18 px-3 py-2 text-sm leading-relaxed text-white/90"
                            >
                              {message.transcriptText || message.content}
                            </motion.p>
                          )}
                        </AnimatePresence>
                      </div>
                    ) : (
                      <p className="text-sm leading-relaxed text-white/90 whitespace-pre-wrap pr-8">
                        {message.content}
                      </p>
                    )}
                    {message.type === 'assistant' && message.answerPayload && (
                      <AutoChart
                        chart={chartFromAnswerPayload(message.answerPayload as Record<string, unknown>)}
                        rows={
                          Array.isArray((message.answerPayload as Record<string, unknown>).rows)
                            ? ((message.answerPayload as Record<string, unknown>).rows as unknown[])
                            : []
                        }
                      />
                    )}
                    {message.type === 'assistant' &&
                      message.answerPayload &&
                      Array.isArray((message.answerPayload as any).rows) && (
                        <ResultTable
                          className="mt-3"
                          title={t('chat.result')}
                          rows={(message.answerPayload as any).rows as Record<string, unknown>[]}
                          filename={`chat-result-${message.id}.csv`}
                          showDownload={!hasVisibleChart(message.answerPayload as Record<string, unknown>)}
                        />
                      )}
                    {message.type === 'assistant' &&
                      message.answerPayload &&
                      typeof (message.answerPayload as any).sql === 'string' && (
                        <details className="mt-2 text-xs text-white/60 pr-2">
                          <summary className="cursor-pointer select-none">SQL</summary>
                          <pre className="mt-2 whitespace-pre-wrap break-words bg-black/20 border border-white/10 rounded-lg p-3">
                            {(message.answerPayload as any).sql}
                          </pre>
                        </details>
                      )}
                    {!message.isAudio && (
                      <p
                        className={cn(
                          'text-xs mt-1',
                          message.type === 'user' ? 'text-white/60' : 'text-white/50'
                        )}
                      >
                        {message.timestamp.toLocaleTimeString([], {
                          hour: '2-digit',
                          minute: '2-digit',
                        })}
                      </p>
                    )}
                    {message.type === 'assistant' && typeof message.responseTimeMs === 'number' && Number.isFinite(message.responseTimeMs) && (
                      <p className="mt-1 text-xs text-white/40">
                        {formatResponseTime(message.responseTimeMs)}
                      </p>
                    )}
                  </div>
                </motion.div>
              ))}
              {datasetSuggestions.length > 0 && messages.length > 0 && (
                <motion.div
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.24 }}
                  className="ml-11 rounded-2xl border border-white/10 bg-card/80 p-4"
                >
                  <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-white">
                        Useful questions for this dataset
                      </p>
                      <p className="mt-1 text-xs leading-relaxed text-white/50">
                        Generated from the selected dataset columns
                        {datasetContext?.bigdata_sample
                          ? ` sample (${(datasetContext.bigdata_sample.rows_sampled ?? datasetContext.row_count ?? 0).toLocaleString()} rows).`
                          : datasetContext?.row_count != null
                            ? ` (${datasetContext.row_count.toLocaleString()} rows).`
                            : '.'}
                      </p>
                    </div>
                    {datasetSuggestions.length > 6 && (
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        onClick={() => setSuggestionsExpanded((value) => !value)}
                        className="h-8 rounded-xl border border-white/10 bg-white/[0.04] px-3 text-xs text-white/70 hover:bg-white/[0.08] hover:text-white"
                      >
                        {suggestionsExpanded ? 'Show less' : 'Show more'}
                      </Button>
                    )}
                  </div>
                  <div className="space-y-3">
                    {nextSuggestionGroups.map(([category, suggestions]) => (
                      <div key={category}>
                        <p className="mb-2 text-[11px] font-semibold uppercase tracking-normal text-white/40">
                          {category}
                        </p>
                        <div className="flex flex-wrap gap-2">
                          {suggestions.map((suggestion) => (
                            <button
                              key={`${suggestion.category}-${suggestion.query}`}
                              type="button"
                              onClick={() => runSuggestion(suggestion)}
                              disabled={sending || datasetId == null}
                              title={suggestion.explanation}
                              className="rounded-full border border-violet-300/20 bg-violet-500/10 px-3 py-1.5 text-left text-sm text-violet-50 transition hover:border-violet-300/40 hover:bg-violet-500/18 disabled:cursor-not-allowed disabled:opacity-55"
                            >
                              {suggestion.title}
                            </button>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </motion.div>
              )}
              {sending && (
                <motion.div
                  initial={{ opacity: 0, y: 12 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -8 }}
                  transition={{ duration: 0.2 }}
                  className="flex items-start gap-3"
                >
                  <AssistantMascot size="sm" className="mt-1" />
                  <div className="rounded-2xl border border-border bg-card px-4 py-3 text-sm text-white/70">
                    Assistant is typing...
                  </div>
                </motion.div>
              )}
              <div ref={bottomRef} />
            </div>
          </ScrollArea>
        </div>
      )}

      <div className="relative border-t border-border bg-card/50 backdrop-blur-xl px-4 py-4 sm:px-6">
        {recordingState === 'idle' && (
          <div className="absolute left-1/2 top-0 z-20 -translate-x-1/2 -translate-y-1/2">
            <motion.button
              type="button"
              onClick={() => setComposerMode(composerMode === 'voice' ? 'text' : 'voice')}
              className="flex h-8 w-8 items-center justify-center rounded-full border border-white/10 bg-[#191824]/95 text-white/45 shadow-[0_8px_26px_rgba(0,0,0,0.35)] transition hover:bg-white/[0.07] hover:text-white/80 focus:outline-none focus:ring-2 focus:ring-violet-400/60"
              whileHover={{ y: composerMode === 'voice' ? -1 : 1, scale: 1.04 }}
              whileTap={{ scale: 0.94 }}
              aria-label={composerMode === 'voice' ? 'Switch to text input' : 'Switch to voice input'}
            >
              {composerMode === 'voice' ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </motion.button>
          </div>
        )}
        <div className="max-w-4xl mx-auto min-h-[132px] flex items-center justify-center">
          <input
            type="file"
            accept=".csv,.xlsx,.json,text/csv,application/json,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ref={fileInputRef}
            className="sr-only"
            onChange={(e) => {
              const f = e.target.files?.[0];
              if (f) void uploadDatasetForSession(f);
            }}
          />
          <AnimatePresence mode="wait">
            {composerMode === 'text' && recordingState === 'idle' ? (
              <motion.form
                key="text-composer"
                onSubmit={onSubmitText}
                initial={{ opacity: 0, y: 30, scale: 0.96 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 24, scale: 0.96 }}
                transition={{ type: 'spring', stiffness: 360, damping: 32, mass: 0.8 }}
                className="w-full flex flex-col items-center gap-2"
              >
                <div className="w-full max-w-3xl rounded-[28px] border border-white/10 bg-[#11111d]/90 px-2 py-2 shadow-[0_18px_55px_rgba(90,55,180,0.22)] ring-1 ring-violet-400/10">
                  <div className="flex items-end gap-2">
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="mb-0.5 h-11 w-11 shrink-0 rounded-2xl border border-white/10 bg-white/[0.04] text-white/70 hover:bg-violet-500/10 hover:text-white"
                      onClick={() => fileInputRef.current?.click()}
                      disabled={sending}
                      aria-label="Upload dataset"
                      title="Upload CSV, XLSX, or JSON dataset"
                    >
                      <Paperclip className="h-5 w-5" />
                    </Button>
                    <Textarea
                      value={textDraft}
                      onChange={(e) => setTextDraft(e.target.value)}
                      placeholder={datasetId == null ? t('chat.placeholderNoDataset') : t('chat.placeholderAsk')}
                      className="min-h-[44px] max-h-32 resize-none border-0 bg-transparent px-1 py-3 text-foreground shadow-none outline-none placeholder:text-white/35 focus-visible:ring-0"
                      disabled={sending}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' && !e.shiftKey) {
                          e.preventDefault();
                          onSubmitText();
                        }
                      }}
                    />
                    <Button
                      type="submit"
                      size="icon"
                      disabled={sending || !textDraft.trim() || datasetId == null}
                      className="mb-0.5 h-11 w-11 shrink-0 rounded-2xl bg-gradient-to-br from-purple-500 to-violet-600 shadow-[0_0_24px_rgba(168,85,247,0.32)]"
                      aria-label="Send query"
                    >
                      {sending ? <Loader2 className="h-5 w-5 animate-spin" /> : <Send className="h-5 w-5" />}
                    </Button>
                  </div>
                </div>
                <div className="h-8" aria-hidden />
              </motion.form>
            ) : recordingState === 'idle' ? (
              <motion.div
                key="voice-composer"
                initial={{ opacity: 0, y: -18, scale: 0.96 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 34, scale: 0.9 }}
                transition={{ type: 'spring', stiffness: 360, damping: 31, mass: 0.82 }}
                className="flex flex-col items-center justify-center gap-3"
              >
                <div className="relative flex w-56 items-center justify-center">
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="absolute left-0 h-12 w-12 rounded-2xl border border-white/10 bg-white/[0.04] text-white/60 hover:bg-violet-500/10 hover:text-white"
                    onClick={() => fileInputRef.current?.click()}
                    disabled={sending}
                    aria-label="Upload dataset"
                    title="Upload CSV, XLSX, or JSON dataset"
                  >
                    <Paperclip className="h-5 w-5" />
                  </Button>
                  <motion.button
                    type="button"
                    onClick={startRecording}
                    className="relative h-16 w-16 rounded-full bg-gradient-to-br from-purple-500 to-violet-600 flex items-center justify-center cursor-pointer border-0 focus:outline-none focus:ring-2 focus:ring-violet-300/80 focus:ring-offset-2 focus:ring-offset-[#11111d]"
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    style={{ boxShadow: '0 0 40px rgba(168, 85, 247, 0.5)' }}
                    aria-label="Start voice input"
                  >
                    <motion.div
                      animate={{ scale: [1, 1.2, 1], opacity: [0.5, 0.2, 0.5] }}
                      transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
                      className="absolute inset-0 rounded-full bg-purple-500"
                    />
                    <Mic className="h-7 w-7 text-white relative z-10" />
                  </motion.button>
                </div>
              </motion.div>
            ) : null}

            {recordingState === 'recording' && (
              <motion.div
                key="recording"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.9 }}
                className="flex items-center justify-center gap-4"
              >
                <div className="flex-1 max-w-2xl flex items-center gap-4 bg-accent/50 rounded-full px-6 py-3 border border-border">
                  <span className="text-sm font-medium text-purple-400">{formatDuration(recordingDuration)}</span>
                  <div className="flex-1 flex items-center justify-center gap-0.5 h-8 overflow-hidden">
                    {waveformLevels.map((level, i) => (
                      <motion.div
                        key={i}
                        animate={{ height: `${Math.max(3, level * 34)}px`, opacity: level < 0.08 ? 0.45 : 0.95 }}
                        transition={{ type: 'spring', stiffness: 520, damping: 34, mass: 0.45 }}
                        className="w-1 rounded-full bg-gradient-to-t from-purple-500 to-violet-300"
                      />
                    ))}
                  </div>
                </div>
                <motion.button
                  type="button"
                  onClick={stopRecording}
                  className="h-16 w-16 rounded-full bg-red-500 flex items-center justify-center cursor-pointer border-0"
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                >
                  <Square className="h-6 w-6 text-white fill-white" />
                </motion.button>
              </motion.div>
            )}

            {(recordingState === 'recorded' || recordingState === 'playing') && (
              <motion.div
                key="recorded"
                initial={{ opacity: 0, scale: 0.9 }}
                animate={{ opacity: 1, scale: 1 }}
                exit={{ opacity: 0, scale: 0.9 }}
                className="flex items-center justify-center gap-4"
              >
                <div className="flex-1 max-w-2xl flex items-center gap-4 bg-accent/50 rounded-full px-6 py-3 border border-border">
                  <Button
                    size="icon"
                    variant="ghost"
                    onClick={playRecording}
                    className="h-10 w-10 rounded-full bg-white/[0.04] border border-white/10"
                  >
                    {recordingState === 'playing' ? (
                      <Square className="h-4 w-4 fill-white text-white" />
                    ) : (
                      <Play className="h-4 w-4 text-white" />
                    )}
                  </Button>
                  <div className="flex-1 flex items-center justify-center gap-0.5 h-8 overflow-hidden">
                    {waveformLevels.map((level, i) => (
                      <motion.div
                        key={i}
                        animate={{
                          height: `${Math.max(3, level * 34)}px`,
                          opacity: recordingState === 'playing' ? [0.65, 1, 0.65] : level < 0.08 ? 0.45 : 0.95,
                        }}
                        transition={
                          recordingState === 'playing'
                            ? { duration: 1.1, repeat: Infinity, ease: 'easeInOut', delay: i * 0.006 }
                            : { type: 'spring', stiffness: 520, damping: 34, mass: 0.45 }
                        }
                        className="w-1 rounded-full bg-gradient-to-t from-purple-500 to-violet-300"
                      />
                    ))}
                  </div>
                  <span className="text-sm text-muted-foreground">{formatDuration(recordingDuration)}</span>
                </div>
                <Button
                  size="icon"
                  variant="ghost"
                  onClick={deleteRecording}
                  className="h-14 w-14 rounded-2xl border border-red-500/25 bg-red-500/10 text-red-300 hover:bg-red-500/20 hover:text-red-100"
                  aria-label="Delete recording"
                  title="Delete recording"
                >
                  <Trash2 className="h-5 w-5" />
                </Button>
                <motion.button
                  type="button"
                  onClick={sendRecording}
                  className="h-16 w-16 rounded-full bg-gradient-to-br from-purple-500 to-violet-600 flex items-center justify-center cursor-pointer border-0"
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                >
                  <Send className="h-6 w-6 text-white" />
                </motion.button>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </div>
  );
}
