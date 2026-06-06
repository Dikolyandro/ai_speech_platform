import { useState } from 'react';
import { Outlet } from 'react-router';
import { AppSidebar } from './app-sidebar';
import { AppHeader } from './app-header';
import { DatasetProvider } from '../dataset-context';

export function MainLayout() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  return (
    <DatasetProvider>
      <div className="h-screen w-screen flex overflow-hidden bg-background">
        <AppSidebar
          collapsed={sidebarCollapsed}
          onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
        />
        <div className="flex-1 flex flex-col min-w-0">
          <AppHeader collapsed={sidebarCollapsed} />
          <main className="flex-1 overflow-auto">
            <Outlet />
          </main>
        </div>
      </div>
    </DatasetProvider>
  );
}
