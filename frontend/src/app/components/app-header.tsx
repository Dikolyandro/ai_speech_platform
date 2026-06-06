import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from './ui/select';
import { useDataset } from '../dataset-context';
import { useI18n } from '../i18n/context';

interface AppHeaderProps {
  collapsed: boolean;
}

export function AppHeader({ collapsed: _collapsed }: AppHeaderProps) {
  const { t } = useI18n();
  const { datasetId, setDatasetId, datasets, datasetsLoading } = useDataset();

  return (
    <header className="sticky top-0 z-20">
      <div className="h-20 border-b border-white/8 bg-[#171722]/78 backdrop-blur-xl flex items-center justify-between gap-6 px-6 shadow-[0_10px_35px_rgba(0,0,0,0.28)]">
        <h1 className="text-2xl font-semibold tracking-tight text-white shrink-0">
          {t('header.title')}
        </h1>
        <div className="flex items-center gap-3 min-w-0 max-w-md flex-1 justify-end">
          <span className="text-xs text-white/45 shrink-0 hidden sm:inline">{t('header.dataset')}</span>
          <Select
            value={datasetId != null ? String(datasetId) : undefined}
            onValueChange={(v) => setDatasetId(v ? parseInt(v, 10) : null)}
            disabled={datasetsLoading}
          >
            <SelectTrigger className="w-full max-w-[280px] bg-white/[0.06] border-white/10 text-white">
              <SelectValue placeholder={datasetsLoading ? t('common.loading') : t('header.selectDataset')} />
            </SelectTrigger>
            <SelectContent className="border-white/10 bg-[#141420] text-white">
              {datasets.map((d) => (
                <SelectItem key={d.id} value={String(d.id)} className="focus:bg-white/10">
                  #{d.id} · {d.name}
                  {d.row_count != null ? ` (${d.row_count})` : ''}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </header>
  );
}
