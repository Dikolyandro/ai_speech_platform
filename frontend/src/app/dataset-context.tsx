import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { listDatasets, type DatasetListItem } from '../lib/api';

const STORAGE_KEY = 'ai_analytics_active_dataset_id';

type DatasetContextValue = {
  datasetId: number | null;
  setDatasetId: (id: number | null) => void;
  refreshDatasets: () => Promise<void>;
  datasets: DatasetListItem[];
  datasetsLoading: boolean;
};

const DatasetContext = createContext<DatasetContextValue | null>(null);

export function DatasetProvider({ children }: { children: ReactNode }) {
  const [datasetId, setDatasetIdState] = useState<number | null>(() => {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const n = parseInt(raw, 10);
    return Number.isFinite(n) ? n : null;
  });
  const [datasets, setDatasets] = useState<DatasetListItem[]>([]);
  const [datasetsLoading, setDatasetsLoading] = useState(true);

  const refreshDatasets = useCallback(async () => {
    setDatasetsLoading(true);
    try {
      const r = await listDatasets();
      setDatasets(r.datasets);
    } catch {
      setDatasets([]);
    } finally {
      setDatasetsLoading(false);
    }
  }, []);

  useEffect(() => {
    void refreshDatasets();
  }, [refreshDatasets]);

  const setDatasetId = useCallback((id: number | null) => {
    setDatasetIdState(id);
    if (id == null) localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, String(id));
  }, []);

  const value = useMemo(
    () => ({
      datasetId,
      setDatasetId,
      refreshDatasets,
      datasets,
      datasetsLoading,
    }),
    [datasetId, setDatasetId, refreshDatasets, datasets, datasetsLoading]
  );

  return <DatasetContext.Provider value={value}>{children}</DatasetContext.Provider>;
}

export function useDataset() {
  const ctx = useContext(DatasetContext);
  if (!ctx) throw new Error('useDataset must be used within DatasetProvider');
  return ctx;
}
