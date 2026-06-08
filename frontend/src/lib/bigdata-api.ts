import { apiJson } from './api';

export type BigDataDatasetStatus = 'uploaded' | 'processing' | 'processed' | 'failed' | string;

export type BigDataSchemaColumn = {
  name: string;
  type: string;
  nullable?: boolean;
};

export type BigDataTopValue = {
  value: string | number | boolean | null;
  count: number;
};

export type BigDataNumericStats = {
  min: number | string | null;
  max: number | string | null;
  avg: number | string | null;
};

export type BigDataProfileColumn = {
  name: string;
  type: string;
  null_count?: number;
  numeric?: BigDataNumericStats | null;
  top_values?: BigDataTopValue[];
};

export type BigDataSchemaJson = {
  columns?: BigDataSchemaColumn[];
};

export type BigDataProfileJson = {
  row_count?: number;
  column_count?: number;
  columns?: BigDataProfileColumn[];
};

export type BigDataDatasetSummary = {
  id: number;
  name: string;
  status: BigDataDatasetStatus;
  row_count: number | null;
  column_count: number | null;
  created_at: string | null;
};

export type BigDataDataset = BigDataDatasetSummary & {
  user_id: number;
  raw_path: string;
  processed_path: string | null;
  format: string;
  schema_json: BigDataSchemaJson | null;
  profile_json: BigDataProfileJson | null;
  error: string | null;
};

export type BigDataChatSampleFilter = {
  column: string;
  operator: '=' | '!=' | '>' | '>=' | '<' | '<=' | 'contains';
  value: string;
};

export type BigDataChatSampleRequest = {
  name: string;
  row_limit: number;
  sampling_mode: 'first' | 'random';
  columns: string[];
  filters: BigDataChatSampleFilter[];
};

export type BigDataChatSampleResult = {
  dataset_id: number;
  name: string;
  source_bigdata_id: number;
  rows_sampled: number;
  row_count: number;
  selected_columns: string[];
  applied_filters: BigDataChatSampleFilter[];
  sampling_mode: 'first' | 'random';
  row_limit: number;
  table_name: string;
  note: string;
};

export async function uploadBigDataCsv(file: File, name?: string) {
  const form = new FormData();
  form.append('file', file);
  if (name?.trim()) form.append('name', name.trim());

  return apiJson<BigDataDataset>('/api/v1/bigdata/datasets/upload', {
    method: 'POST',
    body: form,
  });
}

export async function registerLocalBigDataFile(sourcePath: string, name?: string) {
  const form = new FormData();
  form.append('source_path', sourcePath);
  if (name?.trim()) form.append('name', name.trim());

  return apiJson<BigDataDataset>('/api/v1/bigdata/datasets/register-local', {
    method: 'POST',
    body: form,
  });
}

export async function listBigDataDatasets() {
  return apiJson<{ datasets: BigDataDatasetSummary[] }>('/api/v1/bigdata/datasets');
}

export async function getBigDataDataset(datasetId: number) {
  return apiJson<BigDataDataset>(`/api/v1/bigdata/datasets/${datasetId}`);
}

export async function renameBigDataDataset(datasetId: number, name: string) {
  return apiJson<BigDataDataset>(`/api/v1/bigdata/datasets/${datasetId}`, {
    method: 'PATCH',
    body: JSON.stringify({ name }),
  });
}

export async function profileBigDataDataset(datasetId: number, topN = 10) {
  return apiJson<BigDataDataset>(
    `/api/v1/bigdata/datasets/${datasetId}/profile?top_n=${encodeURIComponent(String(topN))}`,
    { method: 'POST' }
  );
}

export async function createBigDataChatSample(datasetId: number, body: BigDataChatSampleRequest) {
  return apiJson<BigDataChatSampleResult>(`/api/v1/bigdata/datasets/${datasetId}/create-chat-sample`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function deleteBigDataDataset(datasetId: number) {
  return apiJson<{ ok: boolean }>(`/api/v1/bigdata/datasets/${datasetId}`, {
    method: 'DELETE',
  });
}
