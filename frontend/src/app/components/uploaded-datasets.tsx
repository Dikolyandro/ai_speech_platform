import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { motion } from 'motion/react';
import { Search, Eye, Trash2, FileSpreadsheet, Database as DatabaseIcon, Upload, FileText, Pencil } from 'lucide-react';
import { toast } from 'sonner';
import { format } from 'date-fns';
import { Button } from './ui/button';
import { Input } from './ui/input';
import { Card } from './ui/card';
import { Badge } from './ui/badge';
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from './ui/dialog';
import { Label } from './ui/label';
import {
  createDataset,
  deleteDataset,
  importCsvFile,
  listDatasets,
  renameDataset,
  uploadDocumentFile,
  type DatasetListItem,
} from '../../lib/api';
import { useDataset } from '../dataset-context';
import { useI18n } from '../i18n/context';

export function UploadedDatasets() {
  const { t } = useI18n();
  const { setDatasetId, refreshDatasets } = useDataset();
  const [items, setItems] = useState<DatasetListItem[]>([]);
  const [filter, setFilter] = useState('');
  const [loading, setLoading] = useState(true);
  const [uploadOpen, setUploadOpen] = useState(false);
  const [newName, setNewName] = useState(() => t('datasets.defaultName'));
  const [uploading, setUploading] = useState(false);
  const [docOpen, setDocOpen] = useState(false);
  const [docTargetId, setDocTargetId] = useState<number | null>(null);
  const [renameTarget, setRenameTarget] = useState<DatasetListItem | null>(null);
  const [renameName, setRenameName] = useState('');
  const [renaming, setRenaming] = useState(false);
  const csvInputRef = useRef<HTMLInputElement>(null);
  const docInputRef = useRef<HTMLInputElement>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await listDatasets();
      setItems(r.datasets);
    } catch {
      setItems([]);
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
    if (!q) return items;
    return items.filter((d) => d.name.toLowerCase().includes(q) || String(d.id).includes(q));
  }, [items, filter]);

  const onUploadCsv = async (file: File) => {
    if (!file) return;
    setUploading(true);
    try {
      const { dataset_id } = await createDataset(newName.trim() || t('datasets.importedCsvName'));
      await importCsvFile(dataset_id, file);
      toast.success(`${t('common.done')} #${dataset_id}`);
      setUploadOpen(false);
      setNewName(t('datasets.defaultName'));
      await load();
      await refreshDatasets();
      setDatasetId(dataset_id);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : t('common.error'));
    } finally {
      setUploading(false);
    }
  };

  const onUploadDoc = async (file: File) => {
    if (docTargetId == null || !file) return;
    setUploading(true);
    try {
      const r = await uploadDocumentFile(docTargetId, file);
      toast.success(`${t('common.done')}: ${r.chunks_indexed}`);
      setDocOpen(false);
      setDocTargetId(null);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : t('common.error'));
    } finally {
      setUploading(false);
    }
  };

  const onDelete = async (id: number) => {
    if (!confirm(`${t('common.delete')} #${id}?`)) return;
    try {
      await deleteDataset(id);
      toast.success(t('common.delete'));
      await load();
      await refreshDatasets();
    } catch (e) {
      toast.error(e instanceof Error ? e.message : t('common.error'));
    }
  };

  const openRename = (dataset: DatasetListItem) => {
    setRenameTarget(dataset);
    setRenameName(dataset.name);
  };

  const submitRename = async () => {
    if (!renameTarget) return;
    const nextName = renameName.trim();
    if (!nextName) {
      toast.error('Name is required');
      return;
    }
    if (nextName.length > 255) {
      toast.error('Name must be 255 characters or fewer');
      return;
    }

    const id = renameTarget.id;
    const previous = items;
    setRenaming(true);
    setItems((current) => current.map((item) => (item.id === id ? { ...item, name: nextName } : item)));
    try {
      const updated = await renameDataset(id, nextName);
      setItems((current) => current.map((item) => (item.id === id ? { ...item, ...updated } : item)));
      setRenameTarget(null);
      toast.success('Dataset renamed');
      await refreshDatasets();
    } catch (e) {
      setItems(previous);
      toast.error(e instanceof Error ? e.message : t('common.error'));
    } finally {
      setRenaming(false);
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
            {t('datasets.title')}
          </h1>
          <p className="text-muted-foreground">
            {t('datasets.subtitle')}
          </p>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="flex items-center justify-between gap-4 mb-6 flex-wrap"
        >
          <div className="relative flex-1 max-w-md min-w-[200px]">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder={t('datasets.search')}
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="pl-10 bg-accent/50 border-border"
            />
          </div>
          <Button
            type="button"
            className="bg-primary hover:bg-primary/90 shadow-[0_0_20px_rgba(168,85,247,0.3)]"
            onClick={() => setUploadOpen(true)}
          >
            <DatabaseIcon className="h-4 w-4 mr-2" />
            {t('datasets.uploadCsv')}
          </Button>
        </motion.div>

        {loading ? (
          <p className="text-muted-foreground">{t('common.loading')}</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {filtered.map((dataset, index) => (
              <motion.div
                key={dataset.id}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.5, delay: 0.05 + index * 0.03 }}
              >
                <Card
                  className="p-4 hover:bg-accent/50 transition-all border-border group h-full flex flex-col"
                  style={{ boxShadow: '0 4px 16px rgba(0, 0, 0, 0.1)' }}
                >
                  <div className="flex items-start gap-3 mb-3">
                    <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center flex-shrink-0">
                      <FileSpreadsheet className="h-5 w-5 text-primary" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="mb-1 flex min-w-0 items-center gap-1.5">
                        <h3 className="min-w-0 truncate text-sm font-semibold transition-colors group-hover:text-primary">
                          {dataset.name}
                        </h3>
                        <Button
                          type="button"
                          size="icon"
                          variant="ghost"
                          className="h-6 w-6 shrink-0 text-muted-foreground hover:bg-primary/10 hover:text-primary"
                          title="Rename dataset"
                          onClick={() => openRename(dataset)}
                        >
                          <Pencil className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                      <Badge variant="outline" className="bg-green-500/10 text-green-400 border-green-500/20">
                        #{dataset.id} · {dataset.table_name}
                      </Badge>
                    </div>
                  </div>

                  <p className="text-xs text-muted-foreground mb-4 line-clamp-2 flex-1">
                    {Array.isArray(dataset.columns)
                      ? `${dataset.columns.length} ${t('datasets.columns')}`
                      : t('datasets.noMeta')}
                    {dataset.row_count != null ? ` · ${dataset.row_count}` : ''}
                  </p>

                  <div className="flex items-center justify-between pt-3 border-t border-border gap-2 flex-wrap">
                    <span className="text-xs text-muted-foreground">
                      {dataset.created_at
                        ? format(new Date(dataset.created_at), 'd MMM yyyy')
                        : ''}
                    </span>
                    <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 hover:bg-primary/10 hover:text-primary"
                        title={t('datasets.selectActive')}
                        onClick={() => {
                          setDatasetId(dataset.id);
                          toast.success(`${t('datasets.selectActive')} #${dataset.id}`);
                        }}
                      >
                        <Eye className="h-4 w-4" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 hover:bg-primary/10 hover:text-primary"
                        title={t('datasets.docUpload')}
                        onClick={() => {
                          setDocTargetId(dataset.id);
                          setDocOpen(true);
                        }}
                      >
                        <FileText className="h-4 w-4" />
                      </Button>
                      <Button
                        type="button"
                        size="icon"
                        variant="ghost"
                        className="h-8 w-8 hover:bg-destructive/10 hover:text-destructive"
                        onClick={() => void onDelete(dataset.id)}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </Card>
              </motion.div>
            ))}
          </div>
        )}

        <Dialog open={uploadOpen} onOpenChange={setUploadOpen}>
          <DialogContent
            className="border-white/10 bg-[#141420] text-white sm:max-w-md"
            onOpenAutoFocus={(e) => e.preventDefault()}
          >
            <DialogHeader>
              <DialogTitle>{t('datasets.newDatasetCsv')}</DialogTitle>
            </DialogHeader>
            <div className="space-y-3">
              <div>
                <Label className="text-white/70">{t('datasets.datasetName')}</Label>
                <Input
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  className="mt-1 bg-white/5 border-white/10"
                />
              </div>
              <div
                className="rounded-lg border border-dashed border-white/20 p-0 text-center transition-colors hover:border-white/35"
                onDragOver={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                }}
                onDrop={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  const f = e.dataTransfer.files?.[0];
                  if (!f) return;
                  const ok =
                    f.name.toLowerCase().endsWith('.csv') ||
                    f.type === 'text/csv' ||
                    f.type === 'application/vnd.ms-excel';
                  if (ok) void onUploadCsv(f);
                  else toast.error(t('datasets.csvHint'));
                }}
              >
                <input
                  ref={csvInputRef}
                  id="uploaded-datasets-csv-file"
                  type="file"
                  accept=".csv,text/csv"
                  disabled={uploading}
                  className="sr-only"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    if (f) void onUploadCsv(f);
                    e.target.value = '';
                  }}
                />
                <label
                  htmlFor="uploaded-datasets-csv-file"
                  className="flex cursor-pointer flex-col items-center justify-center gap-2 px-6 py-8"
                >
                  <Upload className="h-8 w-8 opacity-60" aria-hidden />
                  <span className="text-sm text-white/85">{t('chat.chooseCsv')}</span>
                  <span className="text-xs text-white/50">{t('datasets.csvHint')}</span>
                </label>
              </div>
              <Button
                type="button"
                variant="secondary"
                className="w-full bg-white/10 text-white hover:bg-white/15"
                disabled={uploading}
                onClick={() => csvInputRef.current?.click()}
              >
                {t('chat.chooseCsv')}
              </Button>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setUploadOpen(false)}>
                {t('common.close')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        <Dialog open={Boolean(renameTarget)} onOpenChange={(open) => !open && setRenameTarget(null)}>
          <DialogContent
            className="border-white/10 bg-[#141420] text-white sm:max-w-md"
            onOpenAutoFocus={(e) => e.preventDefault()}
          >
            <DialogHeader>
              <DialogTitle>Rename dataset</DialogTitle>
            </DialogHeader>
            <div className="space-y-2">
              <Label className="text-white/70">Dataset name</Label>
              <Input
                value={renameName}
                maxLength={255}
                onChange={(e) => setRenameName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') void submitRename();
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
                Save
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        <Dialog open={docOpen} onOpenChange={setDocOpen}>
          <DialogContent
            className="border-white/10 bg-[#141420] text-white sm:max-w-md"
            onOpenAutoFocus={(e) => e.preventDefault()}
          >
            <DialogHeader>
              <DialogTitle>{t('datasets.docTitle')}</DialogTitle>
            </DialogHeader>
            <p className="text-xs text-white/55">#{docTargetId}</p>
            <div
              className="rounded-lg border border-dashed border-white/20"
              onDragOver={(e) => {
                e.preventDefault();
                e.stopPropagation();
              }}
              onDrop={(e) => {
                e.preventDefault();
                e.stopPropagation();
                const f = e.dataTransfer.files?.[0];
                if (f && (f.name.toLowerCase().endsWith('.txt') || f.type === 'text/plain')) void onUploadDoc(f);
              }}
            >
              <input
                ref={docInputRef}
                id="uploaded-datasets-doc-file"
                type="file"
                accept=".txt,text/plain"
                disabled={uploading}
                className="sr-only"
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) void onUploadDoc(f);
                  e.target.value = '';
                }}
              />
              <label
                htmlFor="uploaded-datasets-doc-file"
                className="flex cursor-pointer flex-col items-center justify-center gap-2 px-6 py-6 text-sm text-white/80"
              >
                {t('datasets.docTitle')}
              </label>
            </div>
            <Button
              type="button"
              variant="secondary"
              className="w-full bg-white/10 text-white hover:bg-white/15"
              disabled={uploading}
              onClick={() => docInputRef.current?.click()}
            >
              {t('datasets.chooseTxt')}
            </Button>
            <DialogFooter>
              <Button variant="outline" onClick={() => setDocOpen(false)}>
                {t('common.close')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  );
}
