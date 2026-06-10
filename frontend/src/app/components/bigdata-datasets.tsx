import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useNavigate } from 'react-router';
import { motion } from 'motion/react';
import {
  BarChart3,
  CalendarDays,
  Car,
  CheckCircle2,
  CreditCard,
  Database,
  FileText,
  Hash,
  Info,
  Loader2,
  Pencil,
  Play,
  RefreshCw,
  Search,
  ShoppingCart,
  Star,
  Trash2,
  TrendingUp,
  Upload,
  Users,
  type LucideIcon,
} from 'lucide-react';
import { toast } from 'sonner';
import { Badge } from './ui/badge';
import { Button } from './ui/button';
import { Card } from './ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from './ui/dialog';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip';
import {
  createBigDataChatSample,
  deleteBigDataDataset,
  getBigDataDataset,
  listBigDataDatasets,
  profileBigDataDataset,
  registerLocalBigDataFile,
  renameBigDataDataset,
  uploadBigDataCsv,
  type BigDataDataset,
  type BigDataDatasetSummary,
  type BigDataDatasetStatus,
  type BigDataChatSampleFilter,
  type BigDataProfileColumn,
} from '../../lib/bigdata-api';
import { useDataset } from '../dataset-context';
import { useI18n } from '../i18n/context';

const SUPPORTED_BIGDATA_EXTENSIONS = ['.csv', '.csv.gz', '.json', '.jsonl', '.json.gz', '.parquet'];
const SUPPORTED_BIGDATA_ACCEPT = '.csv,.csv.gz,.json,.jsonl,.json.gz,.parquet,application/json,text/csv,application/octet-stream';
const SUPPORTED_BIGDATA_TEXT = 'Big Data formats: CSV, CSV.GZ, JSON, JSONL, JSON.GZ, PARQUET.';

function statusLabel(status: BigDataDatasetStatus, t: (key: string) => string) {
  if (status === 'uploaded') return t('bigdata.status.uploaded');
  if (status === 'processing') return t('bigdata.status.processing');
  if (status === 'processed') return t('bigdata.status.processed');
  if (status === 'failed') return t('bigdata.status.failed');
  return status;
}

function statusClass(status: BigDataDatasetStatus) {
  if (status === 'processed') return 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20';
  if (status === 'processing') return 'bg-amber-500/10 text-amber-300 border-amber-500/20';
  if (status === 'failed') return 'bg-red-500/10 text-red-400 border-red-500/20';
  return 'bg-sky-500/10 text-sky-300 border-sky-500/20';
}

function visibleDatasetError(error: string | null | undefined) {
  if (!error) return null;
  return /parquet export|spark parquet/i.test(error) ? null : error;
}

function formatDate(value: string | null) {
  if (!value) return '';
  return new Date(value).toLocaleString();
}

function formatCount(value: number | null | undefined) {
  return typeof value === 'number' ? value.toLocaleString() : '-';
}

function formatDatasetFormat(value: string | null | undefined) {
  if (!value) return '-';
  return value
    .split('.')
    .map((part) => part.toUpperCase())
    .join('.');
}

function renderValue(value: unknown) {
  if (value === null || value === undefined || value === '') return 'null';
  return String(value);
}

function numericText(column: BigDataProfileColumn, t: (key: string) => string) {
  if (!column.numeric) return null;
  const { min, max, avg } = column.numeric;
  return `${t('bigdata.min')} ${renderValue(min)} / ${t('bigdata.max')} ${renderValue(max)} / ${t('bigdata.avg')} ${renderValue(avg)}`;
}

const COLUMN_LABELS: Record<string, string> = {
  asin: 'Product ID',
  dolocationid: 'Dropoff Location ID',
  fare_amount: 'Fare Amount',
  image: 'Review Images',
  overall: 'Rating',
  passenger_count: 'Passenger Count',
  payment_type: 'Payment Type',
  pulocationid: 'Pickup Location ID',
  reviewText: 'Review Text',
  reviewerID: 'Reviewer ID',
  reviewerName: 'Reviewer Name',
  reviewTime: 'Review Time',
  summary: 'Review Summary',
  unixReviewTime: 'Unix Review Time',
  vendorid: 'Vendor ID',
  verified: 'Verified Purchase',
  vote: 'Helpful Votes',
};

function readableColumnLabel(name: string) {
  const mapped = COLUMN_LABELS[name] ?? COLUMN_LABELS[name.toLowerCase()];
  if (mapped) return mapped;

  const withSpaces = name
    .replace(/_/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .replace(/\bid\b/gi, 'ID')
    .replace(/\bntz\b/gi, 'NTZ')
    .trim();

  return withSpaces.replace(/\w\S*/g, (word) =>
    word === word.toUpperCase() ? word : word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
  );
}

function readableDataType(type: string, t: (key: string) => string) {
  const lower = type.toLowerCase();
  if (lower.includes('int') || lower.includes('long')) return t('bigdata.type.whole');
  if (lower.includes('double') || lower.includes('float') || lower.includes('decimal')) return t('bigdata.type.decimal');
  if (lower.includes('timestamp') || lower.includes('date')) return t('bigdata.type.datetime');
  if (lower.includes('bool')) return t('bigdata.type.boolean');
  if (lower.includes('array')) return t('bigdata.type.list');
  if (lower.includes('string')) return t('bigdata.type.text');
  return readableColumnLabel(type);
}

function normalizedColumnSet(columns: { name: string }[]) {
  return new Set(columns.map((column) => column.name.toLowerCase().replace(/[^a-z0-9]/g, '')));
}

function hasColumn(columns: Set<string>, names: string[]) {
  return names.some((name) => columns.has(name.toLowerCase().replace(/[^a-z0-9]/g, '')));
}

function hasColumnPart(columns: Set<string>, parts: string[]) {
  return [...columns].some((name) => parts.some((part) => name.includes(part)));
}

function datasetSummary(columns: { name: string }[], t: (key: string) => string) {
  const names = normalizedColumnSet(columns);
  const hasReviewData =
    hasColumn(names, ['overall', 'verified', 'reviewText', 'summary', 'asin', 'vote']) ||
    hasColumnPart(names, ['review', 'rating']);
  const hasTaxiData =
    hasColumn(names, ['fare_amount', 'passenger_count', 'payment_type', 'trip_distance']) ||
    hasColumnPart(names, ['pickup', 'dropoff', 'fare', 'taxi', 'passenger']);

  if (hasTaxiData) {
    return t('bigdata.summary.taxi');
  }
  if (hasReviewData) {
    return t('bigdata.summary.review');
  }
  return t('bigdata.summary.generic');
}

type DatasetCapability = {
  title: string;
  description: string;
  Icon: LucideIcon;
};

function datasetCapabilities(columns: { name: string; type?: string }[], t: (key: string) => string) {
  const names = normalizedColumnSet(columns);
  const capabilities: DatasetCapability[] = [];
  const add = (condition: boolean, capability: DatasetCapability) => {
    if (condition && !capabilities.some((item) => item.title === capability.title)) {
      capabilities.push(capability);
    }
  };

  add(hasColumn(names, ['overall', 'rating']) || hasColumnPart(names, ['rating', 'score']), {
    title: t('bigdata.cap.rating'),
    description: t('bigdata.cap.ratingDesc'),
    Icon: Star,
  });
  add(hasColumn(names, ['reviewText', 'summary']) || hasColumnPart(names, ['review', 'comment', 'text']), {
    title: t('bigdata.cap.review'),
    description: t('bigdata.cap.reviewDesc'),
    Icon: FileText,
  });
  add(hasColumn(names, ['asin', 'product_id', 'product']) || hasColumnPart(names, ['product', 'item', 'asin']), {
    title: t('bigdata.cap.product'),
    description: t('bigdata.cap.productDesc'),
    Icon: ShoppingCart,
  });
  add(hasColumn(names, ['verified']) || hasColumnPart(names, ['purchase', 'verified']), {
    title: t('bigdata.cap.purchase'),
    description: t('bigdata.cap.purchaseDesc'),
    Icon: CheckCircle2,
  });
  add(hasColumn(names, ['trip_distance']) || hasColumnPart(names, ['trip', 'pickup', 'dropoff']), {
    title: t('bigdata.cap.trip'),
    description: t('bigdata.cap.tripDesc'),
    Icon: Car,
  });
  add(hasColumn(names, ['payment_type', 'fare_amount', 'tip_amount', 'total_amount']) || hasColumnPart(names, ['payment', 'fare', 'tip', 'amount']), {
    title: t('bigdata.cap.payment'),
    description: t('bigdata.cap.paymentDesc'),
    Icon: CreditCard,
  });
  add(hasColumn(names, ['passenger_count']) || hasColumnPart(names, ['passenger', 'customer', 'user']), {
    title: t('bigdata.cap.passenger'),
    description: t('bigdata.cap.passengerDesc'),
    Icon: Users,
  });
  add(hasColumnPart(names, ['date', 'time', 'timestamp', 'year', 'month']), {
    title: t('bigdata.cap.time'),
    description: t('bigdata.cap.timeDesc'),
    Icon: TrendingUp,
  });

  const hasNumericType = columns.some((column) => /(int|long|double|float|decimal|number)/i.test(column.type ?? ''));
  add(hasNumericType, {
    title: t('bigdata.cap.number'),
    description: t('bigdata.cap.numberDesc'),
    Icon: Hash,
  });
  add(columns.some((column) => /(date|timestamp)/i.test(column.type ?? '')), {
    title: t('bigdata.cap.date'),
    description: t('bigdata.cap.dateDesc'),
    Icon: CalendarDays,
  });

  if (capabilities.length === 0 && columns.length > 0) {
    capabilities.push({
      title: t('bigdata.cap.field'),
      description: t('bigdata.cap.fieldDesc'),
      Icon: BarChart3,
    });
  }

  return capabilities.slice(0, 6);
}

function InfoTip({ label, text }: { label: string; text: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          role="button"
          tabIndex={0}
          aria-label={label}
          className="inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full text-muted-foreground transition hover:text-foreground focus:outline-none focus:ring-2 focus:ring-primary/40"
        >
          <Info className="h-3.5 w-3.5" />
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" sideOffset={6} className="max-w-[220px]">
        {text}
      </TooltipContent>
    </Tooltip>
  );
}

function HelpLabel({ children, tip }: { children: ReactNode; tip: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {children}
      <InfoTip label="Help information" text={tip} />
    </span>
  );
}

export function BigDataDatasets() {
  const { t } = useI18n();
  const navigate = useNavigate();
  const HELP_TEXT = {
    columns: t('bigdata.detectedFields'),
    datasetFormat: t('bigdata.fileTypeHelp'),
    nullCounts: t('bigdata.nullHelp'),
    process: t('bigdata.processHelp'),
    rows: t('bigdata.rowsHelp'),
    schema: t('bigdata.schemaHelp'),
    sparkProfile: t('bigdata.sparkHelp'),
    topValues: t('bigdata.topValuesHelp'),
    chatSample: t('bigdata.chatSampleHelp'),
  };
  const { setDatasetId, refreshDatasets } = useDataset();
  const [items, setItems] = useState<BigDataDatasetSummary[]>([]);
  const [selected, setSelected] = useState<BigDataDataset | null>(null);
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<number | null>(null);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [uploadName, setUploadName] = useState('');
  const [localPath, setLocalPath] = useState('');
  const [uploading, setUploading] = useState(false);
  const [sampleOpen, setSampleOpen] = useState(false);
  const [sampleSource, setSampleSource] = useState<BigDataDataset | null>(null);
  const [sampleName, setSampleName] = useState('');
  const [sampleRowPreset, setSampleRowPreset] = useState<'1000' | '5000' | '10000' | 'custom'>('5000');
  const [sampleCustomRows, setSampleCustomRows] = useState('5000');
  const [sampleMode, setSampleMode] = useState<'first' | 'random'>('first');
  const [sampleColumns, setSampleColumns] = useState<string[]>([]);
  const [sampleFilters, setSampleFilters] = useState<BigDataChatSampleFilter[]>([]);
  const [creatingSample, setCreatingSample] = useState(false);
  const [createdSample, setCreatedSample] = useState<{ dataset_id: number; name: string } | null>(null);
  const [renameTarget, setRenameTarget] = useState<BigDataDatasetSummary | BigDataDataset | null>(null);
  const [renameName, setRenameName] = useState('');
  const [renaming, setRenaming] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await listBigDataDatasets();
      setItems(result.datasets);
      if (selected && !result.datasets.some((item) => item.id === selected.id)) {
        setSelected(null);
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.loadFailed'));
    } finally {
      setLoading(false);
    }
  }, [selected]);

  useEffect(() => {
    void load();
  }, [load]);

  const filtered = useMemo(() => {
    const query = filter.trim().toLowerCase();
    if (!query) return items;
    return items.filter((item) => item.name.toLowerCase().includes(query) || String(item.id).includes(query));
  }, [filter, items]);

  const openDetails = async (datasetId: number) => {
    setBusyId(datasetId);
    try {
      const dataset = await getBigDataDataset(datasetId);
      setSelected(dataset);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.openFailed'));
    } finally {
      setBusyId(null);
    }
  };

  const onUpload = async (file: File | undefined) => {
    if (!file) return;
    const lowerName = file.name.toLowerCase();
    if (!SUPPORTED_BIGDATA_EXTENSIONS.some((extension) => lowerName.endsWith(extension))) {
      toast.error(SUPPORTED_BIGDATA_TEXT);
      return;
    }

    setUploading(true);
    try {
      const dataset = await uploadBigDataCsv(file, uploadName || file.name);
      toast.success(t('bigdata.uploaded', { id: dataset.id }));
      setUploadOpen(false);
      setUploadName('');
      setSelected(dataset);
      await load();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.uploadFailed'));
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const onRegisterLocalFile = async () => {
    if (!localPath.trim()) {
      toast.error(t('bigdata.enterLocalPath'));
      return;
    }
    const lowerPath = localPath.trim().toLowerCase();
    if (!SUPPORTED_BIGDATA_EXTENSIONS.some((extension) => lowerPath.endsWith(extension))) {
      toast.error(SUPPORTED_BIGDATA_TEXT);
      return;
    }

    setUploading(true);
    try {
      const dataset = await registerLocalBigDataFile(localPath.trim(), uploadName || undefined);
      toast.success(t('bigdata.registered', { id: dataset.id }));
      setUploadOpen(false);
      setUploadName('');
      setLocalPath('');
      setSelected(dataset);
      await load();
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.registerFailed'));
    } finally {
      setUploading(false);
    }
  };

  const runProfile = async (datasetId: number) => {
    setBusyId(datasetId);
    try {
      const dataset = await profileBigDataDataset(datasetId);
      setSelected(dataset);
      await load();
      toast.success(t('bigdata.profileCreated'));
    } catch (error) {
      await load();
      toast.error(error instanceof Error ? error.message : t('bigdata.processingFailed'));
    } finally {
      setBusyId(null);
    }
  };

  const onDelete = async (datasetId: number) => {
    if (!confirm(t('bigdata.deleteConfirm', { id: datasetId }))) return;
    setBusyId(datasetId);
    try {
      await deleteBigDataDataset(datasetId);
      if (selected?.id === datasetId) setSelected(null);
      await load();
      toast.success(t('bigdata.deleted'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.deleteFailed'));
    } finally {
      setBusyId(null);
    }
  };

  const openRename = (dataset: BigDataDatasetSummary | BigDataDataset) => {
    setRenameTarget(dataset);
    setRenameName(dataset.name);
  };

  const submitRename = async () => {
    if (!renameTarget) return;
    const nextName = renameName.trim();
    if (!nextName) {
      toast.error(t('common.requiredName'));
      return;
    }
    if (nextName.length > 255) {
      toast.error(t('common.max255'));
      return;
    }

    const id = renameTarget.id;
    const previousItems = items;
    const previousSelected = selected;
    setRenaming(true);
    setItems((current) => current.map((item) => (item.id === id ? { ...item, name: nextName } : item)));
    setSelected((current) => (current?.id === id ? { ...current, name: nextName } : current));
    try {
      const updated = await renameBigDataDataset(id, nextName);
      setItems((current) => current.map((item) => (item.id === id ? { ...item, name: updated.name } : item)));
      setSelected((current) => (current?.id === id ? updated : current));
      setSampleSource((current) => (current?.id === id ? updated : current));
      setRenameTarget(null);
      toast.success(t('datasets.renamed'));
    } catch (error) {
      setItems(previousItems);
      setSelected(previousSelected);
      toast.error(error instanceof Error ? error.message : t('bigdata.renameFailed'));
    } finally {
      setRenaming(false);
    }
  };

  const openChatSampleModal = async (datasetId: number) => {
    setBusyId(datasetId);
    try {
      const dataset = await getBigDataDataset(datasetId);
      setSelected(dataset);
      if (dataset.status !== 'processed') {
        toast.error(t('bigdata.needProcessed'));
        return;
      }
      const names = (dataset.schema_json?.columns ?? []).map((column) => column.name);
      if (names.length === 0) {
        toast.error(t('bigdata.noSchema'));
        return;
      }
      setSampleSource(dataset);
      setSampleName(t('bigdata.sampleDefaultName', { name: dataset.name }));
      setSampleRowPreset('5000');
      setSampleCustomRows('5000');
      setSampleMode('first');
      setSampleColumns(names);
      setSampleFilters([]);
      setCreatedSample(null);
      setSampleOpen(true);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.sampleSettingsFailed'));
    } finally {
      setBusyId(null);
    }
  };

  const sampleAvailableColumns = useMemo(
    () => sampleSource?.schema_json?.columns ?? [],
    [sampleSource]
  );

  const sampleRowLimit = useMemo(() => {
    const raw = sampleRowPreset === 'custom' ? sampleCustomRows : sampleRowPreset;
    const parsed = parseInt(raw, 10);
    if (!Number.isFinite(parsed)) return 5000;
    return Math.max(1, Math.min(parsed, 50000));
  }, [sampleRowPreset, sampleCustomRows]);

  const toggleSampleColumn = (name: string) => {
    setSampleColumns((prev) =>
      prev.includes(name) ? prev.filter((item) => item !== name) : [...prev, name]
    );
  };

  const updateSampleFilter = (index: number, patch: Partial<BigDataChatSampleFilter>) => {
    setSampleFilters((prev) => prev.map((item, idx) => (idx === index ? { ...item, ...patch } : item)));
  };

  const addSampleFilter = () => {
    const firstColumn = sampleAvailableColumns[0]?.name;
    if (!firstColumn) return;
    setSampleFilters((prev) => [...prev, { column: firstColumn, operator: '=', value: '' }]);
  };

  const removeSampleFilter = (index: number) => {
    setSampleFilters((prev) => prev.filter((_, idx) => idx !== index));
  };

  const createConfiguredChatSample = async () => {
    if (!sampleSource) return;
    const name = sampleName.trim();
    if (!name) {
      toast.error(t('bigdata.enterSampleName'));
      return;
    }
    if (sampleColumns.length === 0) {
      toast.error(t('bigdata.selectOneColumn'));
      return;
    }
    const filters = sampleFilters
      .map((item) => ({ ...item, value: item.value.trim() }))
      .filter((item) => item.value !== '');

    setCreatingSample(true);
    try {
      const result = await createBigDataChatSample(sampleSource.id, {
        name,
        row_limit: sampleRowLimit,
        sampling_mode: sampleMode,
        columns: sampleColumns,
        filters,
      });
      await refreshDatasets();
      setDatasetId(result.dataset_id);
      setCreatedSample({ dataset_id: result.dataset_id, name: result.name });
      toast.success(t('common.done'));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t('bigdata.sampleCreateFailed'));
    } finally {
      setCreatingSample(false);
    }
  };

  const openCreatedSampleInChat = () => {
    if (createdSample) {
      setDatasetId(createdSample.dataset_id);
      navigate('/');
    }
  };

  const columns = selected?.profile_json?.columns ?? [];
  const schemaColumns = selected?.schema_json?.columns ?? [];
  const overviewSummary = datasetSummary(schemaColumns, t);
  const capabilities = datasetCapabilities(schemaColumns, t);

  return (
    <div className="h-full p-8 overflow-y-auto">
      <div className="max-w-7xl mx-auto">
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="mb-8"
        >
          <h1 className="text-3xl font-semibold mb-2 bg-gradient-to-r from-purple-400 to-cyan-300 bg-clip-text text-transparent">
            Big Data
          </h1>
          <p className="text-muted-foreground">
            {t('bigdata.subtitle')}
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="flex items-center justify-between gap-4 mb-6 flex-wrap"
        >
          <div className="relative flex-1 max-w-md min-w-[220px]">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder={t('bigdata.search')}
              value={filter}
              onChange={(event) => setFilter(event.target.value)}
              className="pl-10 bg-accent/50 border-border !text-white !caret-white placeholder:!text-white/45"
            />
          </div>
          <div className="flex items-center gap-2">
            <Button type="button" variant="outline" onClick={() => void load()}>
              <RefreshCw className="h-4 w-4 mr-2" />
              {t('common.refresh')}
            </Button>
            <Button
              type="button"
              className="bg-primary hover:bg-primary/90 shadow-[0_0_20px_rgba(168,85,247,0.3)]"
              onClick={() => setUploadOpen(true)}
            >
              <Upload className="h-4 w-4 mr-2" />
              {t('bigdata.uploadFile')}
            </Button>
          </div>
        </motion.div>

        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_420px] gap-5">
          <div>
            {loading ? (
              <p className="text-muted-foreground">{t('common.loading')}</p>
            ) : filtered.length === 0 ? (
              <Card className="p-6 border-border text-muted-foreground">
                {t('bigdata.empty')}
              </Card>
            ) : (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {filtered.map((dataset, index) => (
                  <motion.div
                    key={dataset.id}
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.35, delay: index * 0.03 }}
                  >
                    <Card className="p-4 hover:bg-accent/50 transition-all border-border h-full flex flex-col">
                      <div className="flex items-start gap-3 mb-4">
                        <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center shrink-0">
                          <Database className="h-5 w-5 text-primary" />
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className="mb-1 flex min-w-0 items-center gap-1.5">
                            <h3 className="min-w-0 truncate text-sm font-semibold">{dataset.name}</h3>
                            <Button
                              type="button"
                              size="icon"
                              variant="ghost"
                              className="h-6 w-6 shrink-0 text-muted-foreground hover:bg-primary/10 hover:text-primary"
                              title={t('bigdata.rename')}
                              onClick={() => openRename(dataset)}
                            >
                              <Pencil className="h-3.5 w-3.5" />
                            </Button>
                          </div>
                          <div className="flex flex-wrap items-center gap-2">
                            <Badge variant="outline" className={statusClass(dataset.status)}>
                              {statusLabel(dataset.status, t)}
                            </Badge>
                            <Badge variant="outline" className="!text-white/70">#{dataset.id}</Badge>
                          </div>
                        </div>
                      </div>

                      <div className="grid grid-cols-2 gap-3 text-sm mb-4">
                        <div className="rounded-lg bg-accent/35 p-3">
                          <p className="text-xs text-muted-foreground">
                            <HelpLabel tip={HELP_TEXT.rows}>{t('bigdata.rows')}</HelpLabel>
                          </p>
                          <p className="font-medium">{dataset.row_count ?? '-'}</p>
                        </div>
                        <div className="rounded-lg bg-accent/35 p-3">
                          <p className="text-xs text-muted-foreground">
                            <HelpLabel tip={HELP_TEXT.columns}>{t('bigdata.columns')}</HelpLabel>
                          </p>
                          <p className="font-medium">{dataset.column_count ?? '-'}</p>
                        </div>
                      </div>

                      <p className="text-xs text-muted-foreground mb-4 flex-1">
                        {dataset.status === 'failed'
                          ? t('bigdata.processingFailedDetails')
                          : formatDate(dataset.created_at)}
                      </p>

                      <div className="flex flex-wrap gap-2 pt-3 border-t border-border">
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          onClick={() => void openDetails(dataset.id)}
                          disabled={busyId === dataset.id}
                        >
                          {t('common.details')}
                        </Button>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              type="button"
                              size="sm"
                              variant="outline"
                              onClick={() => void openChatSampleModal(dataset.id)}
                              disabled={busyId === dataset.id}
                            >
                              {t('bigdata.chatSample')}
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent side="top" sideOffset={6} className="max-w-[230px]">
                            {HELP_TEXT.chatSample}
                          </TooltipContent>
                        </Tooltip>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              type="button"
                              size="sm"
                              onClick={() => void runProfile(dataset.id)}
                              disabled={busyId === dataset.id}
                            >
                              {busyId === dataset.id ? (
                                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                              ) : (
                                <Play className="h-4 w-4 mr-2" />
                              )}
                              {t('common.process')}
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent side="top" sideOffset={6} className="max-w-[220px]">
                            {HELP_TEXT.process}
                          </TooltipContent>
                        </Tooltip>
                        <div className="basis-full grid gap-1 text-[11px] leading-4 text-muted-foreground">
                          <span>{t('bigdata.processInlineHelp')}</span>
                          <span>{t('bigdata.chatSampleHelp')}</span>
                        </div>
                        <Button
                          type="button"
                          size="icon"
                          variant="ghost"
                          className="h-9 w-9 hover:bg-destructive/10 hover:text-destructive"
                          onClick={() => void onDelete(dataset.id)}
                          disabled={busyId === dataset.id}
                          title={t('common.delete')}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </Card>
                  </motion.div>
                ))}
              </div>
            )}
          </div>

          <Card className="p-5 border-border h-fit xl:sticky xl:top-8">
            {selected ? (
              <div className="space-y-5">
                <section className="space-y-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="text-xs font-medium uppercase tracking-[0.08em] text-primary/80">{t('bigdata.datasetOverview')}</p>
                      <div className="mt-1 flex min-w-0 items-center gap-1.5">
                        <h2 className="min-w-0 truncate text-lg font-semibold">{selected.name}</h2>
                        <Button
                          type="button"
                          size="icon"
                          variant="ghost"
                          className="h-7 w-7 shrink-0 text-muted-foreground hover:bg-primary/10 hover:text-primary"
                          title={t('bigdata.rename')}
                          onClick={() => openRename(selected)}
                        >
                          <Pencil className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </div>
                    <Badge variant="outline" className={statusClass(selected.status)}>
                      {statusLabel(selected.status, t)}
                    </Badge>
                  </div>

                  {visibleDatasetError(selected.error) ? (
                    <div className="rounded-lg border border-red-500/20 bg-red-500/10 p-3 text-sm text-red-300">
                      {visibleDatasetError(selected.error)}
                    </div>
                  ) : null}

                  <div className="grid grid-cols-3 gap-2">
                    <div className="rounded-lg border border-border bg-accent/35 p-3">
                      <p className="text-xs text-muted-foreground">
                        <HelpLabel tip={HELP_TEXT.rows}>{t('bigdata.records')}</HelpLabel>
                      </p>
                      <p className="mt-1 text-sm font-semibold">{formatCount(selected.row_count)}</p>
                    </div>
                    <div className="rounded-lg border border-border bg-accent/35 p-3">
                      <p className="text-xs text-muted-foreground">
                        <HelpLabel tip={HELP_TEXT.columns}>{t('bigdata.fields')}</HelpLabel>
                      </p>
                      <p className="mt-1 text-sm font-semibold">{formatCount(selected.column_count)}</p>
                    </div>
                    <div className="rounded-lg border border-border bg-accent/35 p-3">
                      <p className="text-xs text-muted-foreground">
                        <HelpLabel tip={HELP_TEXT.datasetFormat}>{t('bigdata.format')}</HelpLabel>
                      </p>
                      <p className="mt-1 text-sm font-semibold">{formatDatasetFormat(selected.format)}</p>
                    </div>
                  </div>
                </section>

                <section className="rounded-lg border border-border bg-accent/20 p-4">
                  <h3 className="text-sm font-semibold">{t('bigdata.whatContains')}</h3>
                  <p className="mt-2 text-sm leading-6 text-muted-foreground">{overviewSummary}</p>
                </section>

                <section>
                  <h3 className="mb-3 text-sm font-semibold">{t('bigdata.whatAnalyze')}</h3>
                  {capabilities.length === 0 ? (
                    <p className="text-sm text-muted-foreground">{t('bigdata.runProcessingOptions')}</p>
                  ) : (
                    <div className="grid gap-2 sm:grid-cols-2">
                      {capabilities.map(({ title, description, Icon }) => (
                        <div key={title} className="rounded-lg border border-border bg-accent/25 p-3">
                          <div className="flex items-start gap-2">
                            <span className="mt-0.5 inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary">
                              <Icon className="h-4 w-4" />
                            </span>
                            <div className="min-w-0">
                              <p className="text-sm font-medium">{title}</p>
                              <p className="mt-1 text-xs leading-5 text-muted-foreground">{description}</p>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </section>

                <section className="rounded-lg border border-border p-4">
                  <h3 className="text-sm font-semibold">{t('bigdata.createChatSampleTitle')}</h3>
                  <p className="mt-2 text-xs leading-5 text-muted-foreground">
                    {t('bigdata.createChatSampleText')}
                  </p>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="mt-3"
                    onClick={() => void openChatSampleModal(selected.id)}
                    disabled={busyId === selected.id}
                  >
                    {t('bigdata.createSample')}
                  </Button>
                </section>

                <details className="rounded-lg border border-border bg-accent/10">
                  <summary className="cursor-pointer select-none px-4 py-3 text-sm font-semibold">
                    {t('bigdata.advancedDetails')}
                  </summary>
                  <div className="space-y-5 border-t border-border p-4">
                    {selected.processed_path ? (
                      <div>
                        <p className="text-xs text-muted-foreground mb-1">{t('bigdata.processedPath')}</p>
                        <p className="break-all rounded-lg bg-accent/35 p-3 text-xs">{selected.processed_path}</p>
                      </div>
                    ) : null}

                    <div>
                      <h3 className="mb-2 flex items-center gap-1.5 text-sm font-semibold">
                        {t('bigdata.schema')}
                        <InfoTip label={t('bigdata.schemaHelpTitle')} text={HELP_TEXT.schema} />
                      </h3>
                      {schemaColumns.length === 0 ? (
                        <p className="text-sm text-muted-foreground">{t('bigdata.schemaEmpty')}</p>
                      ) : (
                        <div className="max-h-44 overflow-auto rounded-lg border border-border">
                          {schemaColumns.map((column) => (
                            <div key={column.name} className="flex items-center justify-between gap-3 border-b border-border px-3 py-2 last:border-b-0">
                              <div className="min-w-0">
                                <p className="truncate text-sm">{readableColumnLabel(column.name)}</p>
                                <p className="truncate text-[11px] text-muted-foreground">{column.name}</p>
                              </div>
                              <Badge variant="outline" className="!text-white/70" title={column.type}>
                                {readableDataType(column.type, t)}
                              </Badge>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>

                    <div>
                      <h3 className="mb-2 flex items-center gap-1.5 text-sm font-semibold">
                        {t('bigdata.sparkProfile')}
                        <InfoTip label={t('bigdata.sparkProfileHelp')} text={HELP_TEXT.sparkProfile} />
                      </h3>
                      {columns.length === 0 ? (
                        <p className="text-sm text-muted-foreground">{t('bigdata.profileEmpty')}</p>
                      ) : (
                        <div className="space-y-3 max-h-[420px] overflow-auto pr-1">
                          {columns.map((column) => (
                            <div key={column.name} className="rounded-lg border border-border p-3">
                              <div className="flex items-center justify-between gap-2 mb-2">
                                <div className="min-w-0">
                                  <p className="truncate text-sm font-medium">{readableColumnLabel(column.name)}</p>
                                  <p className="truncate text-[11px] text-muted-foreground">{column.name}</p>
                                </div>
                                <Badge variant="outline" className="!text-white/70" title={column.type}>
                                  {readableDataType(column.type, t)}
                                </Badge>
                              </div>
                              <p className="mb-1 flex items-center gap-1.5 text-xs text-muted-foreground">
                                <HelpLabel tip={HELP_TEXT.nullCounts}>{t('bigdata.nulls')}</HelpLabel>: {column.null_count ?? 0}
                              </p>
                              {numericText(column, t) ? (
                                <p className="text-xs text-muted-foreground mb-2">{numericText(column, t)}</p>
                              ) : null}
                              {column.top_values?.length ? (
                                <div className="space-y-1">
                                  <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
                                    <HelpLabel tip={HELP_TEXT.topValues}>{t('bigdata.topValues')}</HelpLabel>
                                  </p>
                                  {column.top_values.slice(0, 5).map((item, index) => (
                                    <div key={`${column.name}-${index}`} className="flex items-center justify-between gap-2 text-xs">
                                      <span className="truncate text-muted-foreground">{renderValue(item.value)}</span>
                                      <span>{item.count}</span>
                                    </div>
                                  ))}
                                </div>
                              ) : null}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                </details>
              </div>
            ) : (
              <div className="text-sm text-muted-foreground">
                {t('bigdata.selectInspect')}
              </div>
            )}
          </Card>
        </div>

        <Dialog open={Boolean(renameTarget)} onOpenChange={(open) => !open && setRenameTarget(null)}>
          <DialogContent
            className="border-white/10 bg-[#141420] text-white sm:max-w-md"
            onOpenAutoFocus={(event) => event.preventDefault()}
          >
            <DialogHeader>
              <DialogTitle>{t('bigdata.rename')}</DialogTitle>
              <DialogDescription className="sr-only">
                {t('bigdata.renameDescription')}
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-2">
              <Label className="text-white/70">{t('bigdata.datasetName')}</Label>
              <Input
                value={renameName}
                maxLength={255}
                onChange={(event) => setRenameName(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') void submitRename();
                }}
                className="bg-white/5 border-white/10 !text-white !caret-white placeholder:!text-white/45"
              />
              <p className="text-xs text-white/45">{renameName.length}/255</p>
            </div>
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setRenameTarget(null)}>
                {t('common.close')}
              </Button>
              <Button type="button" onClick={() => void submitRename()} disabled={renaming}>
                {t('common.save')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        <Dialog open={uploadOpen} onOpenChange={setUploadOpen}>
          <DialogContent
            className="border-white/10 bg-[#141420] text-white sm:max-w-md"
            onOpenAutoFocus={(event) => event.preventDefault()}
          >
            <DialogHeader>
              <DialogTitle>{t('bigdata.upload')}</DialogTitle>
              <DialogDescription className="sr-only">
                {t('bigdata.uploadDescription')}
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-3">
              <div>
                <Label className="text-white/70">{t('bigdata.datasetName')}</Label>
                <Input
                  value={uploadName}
                  onChange={(event) => setUploadName(event.target.value)}
                  className="mt-1 bg-white/5 border-white/10 !text-white !caret-white placeholder:!text-white/45"
                  placeholder={t('bigdata.optionalName')}
                />
              </div>
              <div>
                <Label className="text-white/70">{t('bigdata.localPath')}</Label>
                <div className="mt-1 flex gap-2">
                  <Input
                    value={localPath}
                    onChange={(event) => setLocalPath(event.target.value)}
                    className="bg-white/5 border-white/10 !text-white !caret-white placeholder:!text-white/45"
                    placeholder="C:\\Users\\...\\Video_Games.json"
                  />
                  <Button type="button" variant="outline" onClick={() => void onRegisterLocalFile()} disabled={uploading}>
                    {t('bigdata.register')}
                  </Button>
                </div>
              </div>
              <div
                className="rounded-lg border border-dashed border-white/20 p-0 text-center transition-colors hover:border-white/35"
                onDragOver={(event) => {
                  event.preventDefault();
                }}
                onDrop={(event) => {
                  event.preventDefault();
                  void onUpload(event.dataTransfer.files?.[0]);
                }}
              >
                <button
                  type="button"
                  className="w-full rounded-lg px-4 py-8 text-left"
                  onClick={() => fileInputRef.current?.click()}
                >
                  <div className="mx-auto mb-3 flex h-11 w-11 items-center justify-center rounded-lg bg-white/5">
                    <Upload className="h-5 w-5 text-white/70" />
                  </div>
                  <p className="text-center text-sm text-white/80">{t('bigdata.dropFile')}</p>
                  <p className="text-center text-xs text-white/45">{SUPPORTED_BIGDATA_TEXT}</p>
                </button>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept={SUPPORTED_BIGDATA_ACCEPT}
                  className="hidden"
                  onChange={(event) => void onUpload(event.target.files?.[0])}
                />
              </div>
            </div>
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setUploadOpen(false)}>
                Cancel
              </Button>
              <Button type="button" onClick={() => fileInputRef.current?.click()} disabled={uploading}>
                {uploading ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Upload className="h-4 w-4 mr-2" />}
                {t('bigdata.chooseFile')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        <Dialog open={sampleOpen} onOpenChange={setSampleOpen}>
          <DialogContent
            className="max-h-[88vh] overflow-y-auto border-white/10 bg-[#141420] text-white sm:max-w-3xl"
            onOpenAutoFocus={(event) => event.preventDefault()}
          >
            <DialogHeader>
              <DialogTitle>{t('bigdata.chatSample')}</DialogTitle>
              <DialogDescription className="sr-only">
                {t('bigdata.sampleDescription')}
              </DialogDescription>
            </DialogHeader>

            <div className="space-y-5">
              <p className="text-sm text-white/65">
                {t('bigdata.createSampleIntro')}
              </p>

              <div className="grid gap-3 rounded-lg border border-white/10 bg-white/[0.03] p-3 text-sm sm:grid-cols-3">
                <div>
                  <p className="text-xs text-white/45">{t('bigdata.source')}</p>
                  <p className="truncate font-medium">{sampleSource?.name ?? '-'}</p>
                </div>
                <div>
                  <p className="text-xs text-white/45">{t('bigdata.totalRows')}</p>
                  <p className="font-medium">{sampleSource?.row_count ?? '-'}</p>
                </div>
                <div>
                  <p className="text-xs text-white/45">{t('bigdata.availableColumns')}</p>
                  <p className="font-medium">{sampleAvailableColumns.length}</p>
                </div>
              </div>

              <div>
                <Label className="text-white/70">{t('bigdata.sampleName')}</Label>
                <Input
                  value={sampleName}
                  onChange={(event) => setSampleName(event.target.value)}
                  className="mt-1 bg-white/5 border-white/10 !text-white !caret-white placeholder:!text-white/45"
                  placeholder={t('bigdata.samplePlaceholder')}
                />
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div>
                  <Label className="text-white/70">{t('bigdata.rowLimit')}</Label>
                  <select
                    className="mt-1 w-full rounded-md border border-white/10 bg-[#1d1d2b] px-3 py-2 text-sm !text-white focus:outline-none focus:ring-2 focus:ring-primary [&>option]:bg-[#1d1d2b] [&>option]:text-white"
                    value={sampleRowPreset}
                    onChange={(event) => setSampleRowPreset(event.target.value as '1000' | '5000' | '10000' | 'custom')}
                  >
                    <option value="1000">{t('bigdata.rowCount', { count: '1,000' })}</option>
                    <option value="5000">{t('bigdata.rowCount', { count: '5,000' })}</option>
                    <option value="10000">{t('bigdata.rowCount', { count: '10,000' })}</option>
                    <option value="custom">{t('bigdata.custom')}</option>
                  </select>
                  {sampleRowPreset === 'custom' ? (
                    <Input
                      type="number"
                      min={1}
                      max={50000}
                      value={sampleCustomRows}
                      onChange={(event) => setSampleCustomRows(event.target.value)}
                      className="mt-2 bg-white/5 border-white/10 !text-white !caret-white placeholder:!text-white/45"
                      placeholder={t('bigdata.maxRows', { count: '50000' })}
                    />
                  ) : null}
                </div>
                <div>
                  <Label className="text-white/70">{t('bigdata.samplingMode')}</Label>
                  <select
                    className="mt-1 w-full rounded-md border border-white/10 bg-[#1d1d2b] px-3 py-2 text-sm !text-white focus:outline-none focus:ring-2 focus:ring-primary [&>option]:bg-[#1d1d2b] [&>option]:text-white"
                    value={sampleMode}
                    onChange={(event) => setSampleMode(event.target.value as 'first' | 'random')}
                  >
                    <option value="first">{t('bigdata.firstRows')}</option>
                    <option value="random">{t('bigdata.randomSample')}</option>
                  </select>
                </div>
              </div>

              <div>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <Label className="text-white/70">{t('bigdata.columns')}</Label>
                  <div className="flex gap-2">
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      onClick={() => setSampleColumns(sampleAvailableColumns.map((column) => column.name))}
                    >
                      {t('bigdata.all')}
                    </Button>
                    <Button type="button" size="sm" variant="outline" onClick={() => setSampleColumns([])}>
                      {t('bigdata.none')}
                    </Button>
                  </div>
                </div>
                <div className="grid max-h-56 gap-2 overflow-auto rounded-lg border border-white/10 bg-white/[0.03] p-3 sm:grid-cols-2">
                  {sampleAvailableColumns.map((column) => (
                    <label key={column.name} className="flex min-w-0 items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-white/5">
                      <input
                        type="checkbox"
                        checked={sampleColumns.includes(column.name)}
                        onChange={() => toggleSampleColumn(column.name)}
                        className="h-4 w-4 accent-primary"
                      />
                      <span className="truncate text-white">{column.name}</span>
                      <Badge variant="outline" className="ml-auto shrink-0 border-white/10 bg-white/5 !text-white/70">
                        {column.type}
                      </Badge>
                    </label>
                  ))}
                </div>
              </div>

              <div>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <div>
                    <Label className="text-white/70">{t('bigdata.filters')}</Label>
                    <p className="text-xs text-white/45">{t('bigdata.filtersHint')}</p>
                  </div>
                  <Button type="button" size="sm" variant="outline" onClick={addSampleFilter}>
                    {t('bigdata.addFilter')}
                  </Button>
                </div>
                {sampleFilters.length === 0 ? (
                  <p className="rounded-lg border border-white/10 bg-white/[0.03] p-3 text-sm text-white/50">
                    {t('bigdata.noFilters')}
                  </p>
                ) : (
                  <div className="space-y-2">
                    {sampleFilters.map((filterItem, index) => (
                      <div key={`${filterItem.column}-${index}`} className="grid gap-2 rounded-lg border border-white/10 bg-white/[0.03] p-2 sm:grid-cols-[minmax(0,1fr)_110px_minmax(0,1fr)_40px]">
                        <select
                          className="rounded-md border border-white/10 bg-[#1d1d2b] px-3 py-2 text-sm !text-white focus:outline-none focus:ring-2 focus:ring-primary [&>option]:bg-[#1d1d2b] [&>option]:text-white"
                          value={filterItem.column}
                          onChange={(event) => updateSampleFilter(index, { column: event.target.value })}
                        >
                          {sampleAvailableColumns.map((column) => (
                            <option key={column.name} value={column.name}>
                              {column.name}
                            </option>
                          ))}
                        </select>
                        <select
                          className="rounded-md border border-white/10 bg-[#1d1d2b] px-3 py-2 text-sm !text-white focus:outline-none focus:ring-2 focus:ring-primary [&>option]:bg-[#1d1d2b] [&>option]:text-white"
                          value={filterItem.operator}
                          onChange={(event) => updateSampleFilter(index, { operator: event.target.value as BigDataChatSampleFilter['operator'] })}
                        >
                          <option value="=">=</option>
                          <option value="!=">!=</option>
                          <option value=">">&gt;</option>
                          <option value=">=">&gt;=</option>
                          <option value="<">&lt;</option>
                          <option value="<=">&lt;=</option>
                          <option value="contains">{t('bigdata.operator.contains')}</option>
                        </select>
                        <Input
                          value={filterItem.value}
                          onChange={(event) => updateSampleFilter(index, { value: event.target.value })}
                          className="bg-white/5 border-white/10 !text-white !caret-white placeholder:!text-white/45"
                          placeholder={t('bigdata.value')}
                        />
                        <Button
                          type="button"
                          size="icon"
                          variant="ghost"
                          className="h-10 w-10 hover:bg-destructive/10 hover:text-destructive"
                          onClick={() => removeSampleFilter(index)}
                          title={t('bigdata.removeFilter')}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {createdSample ? (
                <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/10 p-3 text-sm text-emerald-200">
                  {t('bigdata.createdSample', { id: createdSample.dataset_id, name: createdSample.name })}
                </div>
              ) : null}
            </div>

            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setSampleOpen(false)}>
                {t('common.close')}
              </Button>
              {createdSample ? (
                <Button type="button" onClick={openCreatedSampleInChat}>
                  {t('bigdata.openChat')}
                </Button>
              ) : null}
              <Button type="button" onClick={() => void createConfiguredChatSample()} disabled={creatingSample}>
                {creatingSample ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                {t('bigdata.createSample')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  );
}
