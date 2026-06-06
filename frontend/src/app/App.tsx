import { RouterProvider } from 'react-router';
import { Toaster } from 'sonner';
import { router } from './routes';
import { DatasetProvider } from './dataset-context';
import { AuthProvider } from './auth-context';
import { I18nProvider } from './i18n/context';

export default function App() {
  return (
    <AuthProvider>
      <I18nProvider>
        <DatasetProvider>
          <div className="dark">
            <RouterProvider router={router} />
            <Toaster richColors position="top-center" />
          </div>
        </DatasetProvider>
      </I18nProvider>
    </AuthProvider>
  );
}
