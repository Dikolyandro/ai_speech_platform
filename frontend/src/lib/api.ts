const prefix = import.meta.env.VITE_API_BASE_URL ?? '';

function url(path: string) {
  if (path.startsWith('http')) return path;
  return `${prefix}${path}`;
}

const TOKEN_KEY = 'ai_analytics_access_token';
export type PreferredLanguage = 'ru' | 'en' | 'kk';

export function getAccessToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setAccessToken(token: string | null) {
  if (!token) localStorage.removeItem(TOKEN_KEY);
  else localStorage.setItem(TOKEN_KEY, token);
}

async function parseError(res: Response): Promise<string> {
  try {
    const j = await res.json();
    if (typeof j.detail === 'string') return j.detail;
    if (Array.isArray(j.detail)) return j.detail.map((x: { msg?: string }) => x.msg).filter(Boolean).join(', ') || res.statusText;
    return res.statusText;
  } catch {
    return res.statusText;
  }
}

export async function apiJson<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getAccessToken();
  const res = await fetch(url(path), {
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init?.body && !(init.body instanceof FormData)
        ? { 'Content-Type': 'application/json' }
        : {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });
  if (!res.ok) {
    const msg = await parseError(res);
    if (res.status === 401) {
      // centralized logout on auth failures
      setAccessToken(null);
    }
    throw new Error(msg);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

export type ChatSessionDto = {
  id: number;
  workspace_id: string;
  title: string;
  created_at: string | null;
  updated_at: string | null;
};

export type ChatMessageDto = {
  id: number;
  session_id: number;
  role: 'user' | 'assistant';
  content: string;
  meta_json: Record<string, unknown> | null;
  created_at: string | null;
};

export type DatasetListItem = {
  id: number;
  name: string;
  workspace_id: string;
  is_private?: boolean;
  created_at: string | null;
  table_name: string;
  columns: unknown;
  row_count: number | null;
};

export type DatasetColumnProfile = {
  name: string;
  type?: string | null;
  profile?: Record<string, unknown> | null;
  [key: string]: unknown;
};

export type BigDataSampleInfo = {
  source_bigdata_id?: number;
  source_dataset_name?: string;
  rows_sampled?: number;
  selected_columns?: string[];
  applied_filters?: { column: string; operator: string; value: string }[];
  sampling_mode?: 'first' | 'random' | string;
  row_limit?: number;
};

export type DatasetChatContext = {
  dataset_id: number;
  name: string;
  workspace_id: string;
  is_private: boolean;
  table_name: string;
  row_count: number | null;
  columns: DatasetColumnProfile[];
  overview?: Record<string, unknown> | null;
  bigdata_sample?: BigDataSampleInfo | null;
};

export type DatasetSuggestion = {
  title: string;
  query: string;
  required_columns: string[];
  category: string;
  explanation: string;
};

export type SavedQueryDto = {
  id: number;
  title: string;
  query_text: string;
  sql_text: string | null;
  answer_text: string | null;
  result_json: unknown;
  dataset_id: number | null;
  created_at: string | null;
};

export type QueryAnswerChart = {
  chart_type: 'bar' | 'line' | 'pie' | 'table' | null;
  x?: string | null;
  y?: string | null;
  title?: string | null;
  reason?: string | null;
  enabled?: boolean;
};

export type UserMeDto = {
  id: number;
  nickname: string;
  email?: string | null;
  preferred_language: PreferredLanguage;
  is_email_verified?: boolean;
};

export type RegisterDto = UserMeDto & {
  verification_required: boolean;
};

export async function authRegister(
  nickname: string,
  email: string,
  password: string,
  preferred_language: PreferredLanguage
) {
  return apiJson<RegisterDto>('/api/v1/auth/register', {
    method: 'POST',
    body: JSON.stringify({ nickname, email, password, preferred_language }),
  });
}

export async function authVerifyEmail(email: string, code: string) {
  return apiJson<{ message: string }>('/api/v1/auth/verify-email', {
    method: 'POST',
    body: JSON.stringify({ email, code }),
  });
}

export async function authResendVerificationCode(email: string) {
  return apiJson<{ message: string }>('/api/v1/auth/resend-verification-code', {
    method: 'POST',
    body: JSON.stringify({ email }),
  });
}

export async function authLogin(nickname: string, password: string) {
  return apiJson<{ access_token: string; token_type: string }>('/api/v1/auth/login', {
    method: 'POST',
    body: JSON.stringify({ nickname, password }),
  });
}

export async function authMe() {
  return apiJson<UserMeDto>('/api/v1/auth/me');
}

export async function authUpdateLanguage(preferred_language: PreferredLanguage) {
  try {
    return await apiJson<UserMeDto>('/api/v1/auth/me/language', {
      method: 'PATCH',
      body: JSON.stringify({ preferred_language }),
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : '';
    // Fallback for older backend routes.
    if (msg.includes('404') || msg.toLowerCase().includes('not found')) {
      return apiJson<UserMeDto>('/api/v1/auth/language', {
        method: 'PATCH',
        body: JSON.stringify({ preferred_language }),
      });
    }
    throw e;
  }
}

export async function listChatSessions(workspaceId = 'default') {
  return apiJson<{ sessions: ChatSessionDto[] }>(
    `/api/v1/chat/sessions?workspace_id=${encodeURIComponent(workspaceId)}`
  );
}

export async function createChatSession(title = 'New chat', workspaceId = 'default') {
  return apiJson<ChatSessionDto>('/api/v1/chat/sessions', {
    method: 'POST',
    body: JSON.stringify({ title, workspace_id: workspaceId }),
  });
}

export async function renameChatSession(sessionId: number, title: string) {
  return apiJson<ChatSessionDto>(`/api/v1/chat/sessions/${sessionId}`, {
    method: 'PATCH',
    body: JSON.stringify({ title }),
  });
}

export async function deleteChatSession(sessionId: number) {
  return apiJson<{ ok: string }>(`/api/v1/chat/sessions/${sessionId}`, { method: 'DELETE' });
}

export async function listChatMessages(sessionId: number) {
  return apiJson<{ messages: ChatMessageDto[] }>(`/api/v1/chat/sessions/${sessionId}/messages`);
}

export async function appendChatMessage(
  sessionId: number,
  body: { role: 'user' | 'assistant'; content: string; meta_json?: Record<string, unknown> | null }
) {
  return apiJson<ChatMessageDto>(`/api/v1/chat/sessions/${sessionId}/messages`, {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

export async function listDatasets(workspaceId = 'default') {
  return apiJson<{ datasets: DatasetListItem[] }>(
    `/api/v1/datasets?workspace_id=${encodeURIComponent(workspaceId)}`
  );
}

export async function createDataset(name: string, workspaceId = 'default') {
  return apiJson<{ dataset_id: number }>('/api/v1/datasets', {
    method: 'POST',
    body: JSON.stringify({ name, workspace_id: workspaceId }),
  });
}

export async function deleteDataset(datasetId: number) {
  return apiJson<{ ok: boolean }>(`/api/v1/datasets/${datasetId}`, { method: 'DELETE' });
}

export async function getDatasetChatContext(datasetId: number) {
  return apiJson<DatasetChatContext>(`/api/v1/datasets/${datasetId}/chat-context`);
}

export async function getDatasetSuggestions(datasetId: number) {
  return apiJson<{ dataset_id: number; suggestions: DatasetSuggestion[] }>(
    `/api/v1/datasets/${datasetId}/suggestions`
  );
}

export async function importCsvFile(
  datasetId: number,
  file: File,
  opts?: { delimiter?: string; hasHeader?: boolean }
) {
  const fd = new FormData();
  fd.append('file', file);
  fd.append('delimiter', opts?.delimiter ?? ',');
  fd.append('has_header', String(opts?.hasHeader ?? true));
  fd.append('max_rows', '50000');
  fd.append('drop_existing', 'true');
  return apiJson<{
    dataset_id: number;
    table_name: string;
    columns: { name: string; type: string }[];
    rows_inserted: number;
  }>(`/api/v1/datasets/${datasetId}/import_csv_file`, { method: 'POST', body: fd });
}

export async function uploadDocumentFile(datasetId: number, file: File, chunkSize = 500) {
  const fd = new FormData();
  fd.append('file', file);
  fd.append('chunk_size', String(chunkSize));
  return apiJson<{ document_id: number; chunks_indexed: number }>(
    `/api/v1/datasets/${datasetId}/documents/upload`,
    { method: 'POST', body: fd }
  );
}

export async function transcribeAudio(file: Blob, filename = 'voice.webm') {
  const fd = new FormData();
  fd.append('file', file, filename);
  return apiJson<{ job_id: number; status: string; text?: string; error?: string }>(
    '/api/v1/asr/transcribe',
    { method: 'POST', body: fd }
  );
}

export async function postQueryAnswer(body: {
  dataset_id: number;
  input: { type: string; text?: string; job_id?: number };
  options?: { limit?: number; explain?: boolean; confidence_threshold?: number };
}) {
  return apiJson<{
    dataset_id: number;
    interpreted: Record<string, unknown> & { chart?: QueryAnswerChart | null };
    rows: Record<string, unknown>[];
    answer_text: string;
    sql?: string;
  }>('/api/v1/query/answer', {
    method: 'POST',
    body: JSON.stringify({
      dataset_id: body.dataset_id,
      input: body.input,
      options: body.options ?? {
        limit: 20,
        explain: true,
        confidence_threshold: 0.55,
      },
    }),
  });
}

export async function listSavedQueries(workspaceId = 'default') {
  return apiJson<{ queries: SavedQueryDto[] }>(
    `/api/v1/saved-queries?workspace_id=${encodeURIComponent(workspaceId)}`
  );
}

export async function createSavedQuery(body: {
  title: string;
  query_text: string;
  sql_text?: string | null;
  answer_text?: string | null;
  result_json?: unknown;
  dataset_id?: number | null;
  workspace_id?: string;
}) {
  return apiJson<SavedQueryDto>('/api/v1/saved-queries', {
    method: 'POST',
    body: JSON.stringify({ workspace_id: 'default', ...body }),
  });
}

export async function deleteSavedQuery(id: number) {
  return apiJson<{ ok: string }>(`/api/v1/saved-queries/${id}`, { method: 'DELETE' });
}
