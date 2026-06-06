import { createBrowserRouter, Navigate } from 'react-router';
import { MainLayout } from './components/main-layout';
import { ChatSession } from './components/chat-session';
import { SavedQueries } from './components/saved-queries';
import { SavedVisuals } from './components/saved-visuals';
import { UploadedDatasets } from './components/uploaded-datasets';
import { BigDataDatasets } from './components/bigdata-datasets';
import { LoginPage } from './pages/login';
import { useAuth } from './auth-context';
import { useI18n } from './i18n/context';

function RequireAuth({ children }: { children: JSX.Element }) {
  const { state } = useAuth();
  const { t } = useI18n();
  if (state.status === 'loading') return <div className="p-6 text-muted-foreground">{t('routes.loading')}</div>;
  if (state.status !== 'authenticated') return <Navigate to="/login" replace />;
  return children;
}

export const router = createBrowserRouter([
  { path: '/login', Component: LoginPage },
  {
    path: '/',
    element: (
      <RequireAuth>
        <MainLayout />
      </RequireAuth>
    ),
    children: [
      { index: true, Component: ChatSession },
      { path: 'session/:sessionId', Component: ChatSession },
      { path: 'queries', Component: SavedQueries },
      { path: 'visuals', Component: SavedVisuals },
      { path: 'datasets', Component: UploadedDatasets },
      { path: 'bigdata', Component: BigDataDatasets },
    ],
  },
]);
