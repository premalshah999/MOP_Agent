import { useCallback, useMemo, useState } from 'react';
import { AuthProvider, useAuth } from '@/hooks/useAuth';
import { useThreadStore } from '@/hooks/useThreadStore';
import { AuthScreen } from '@/components/AuthScreen';
import { ChatArea } from '@/components/ChatArea';
import { Sidebar } from '@/components/Sidebar';
import { DATASET_GUIDES } from '@/lib/content';

function Shell() {
  const { user, loading } = useAuth();
  if (loading) {
    return (
      <div className="flex h-screen items-center justify-center bg-[var(--bg)]">
        <span className="text-[13px] text-[var(--muted)]">Loading...</span>
      </div>
    );
  }
  if (!user) return <AuthScreen />;
  return <Workspace />;
}

function Workspace() {
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const store = useThreadStore('government_finance');

  const selectedDataset = useMemo(
    () => DATASET_GUIDES.find((d) => d.id === (store.activeThread?.datasetId || store.selectedDatasetId)) ?? DATASET_GUIDES[0],
    [store.activeThread, store.selectedDatasetId],
  );

  const handleNewChat = useCallback(async () => {
    await store.createThread(store.selectedDatasetId);
    setMobileSidebarOpen(false);
  }, [store]);

  const handleSelectThread = useCallback((id: string) => {
    store.selectThread(id);
    setMobileSidebarOpen(false);
  }, [store]);

  const handleEnsureThread = useCallback(async (): Promise<string | null> => {
    if (store.activeThread?.id) return store.activeThread.id;
    const thread = await store.createThread(store.selectedDatasetId);
    return thread?.id ?? null;
  }, [store]);

  if (store.loading) {
    return (
      <div className="flex h-screen items-center justify-center bg-[var(--bg)]">
        <span className="text-[13px] text-[var(--muted)]">Loading conversations...</span>
      </div>
    );
  }

  return (
    <div className="flex h-screen overflow-hidden bg-[var(--bg)] text-[var(--ink)]">
      {/* Desktop sidebar (resizable) */}
      <Sidebar
        datasets={DATASET_GUIDES}
        threads={store.threads}
        activeThreadId={store.activeThreadId}
        onNewChat={() => void handleNewChat()}
        onSelectThread={handleSelectThread}
        onDeleteThread={store.deleteThread}
        onClearAll={store.clearAll}
        resizable
        className="hidden lg:flex lg:flex-col"
      />

      {/* Mobile sidebar overlay */}
      <div className={`fixed inset-0 z-40 lg:hidden ${mobileSidebarOpen ? '' : 'pointer-events-none'}`}>
        <div
          className={`absolute inset-0 bg-black/20 transition-opacity duration-200 ${mobileSidebarOpen ? 'opacity-100' : 'opacity-0'}`}
          onClick={() => setMobileSidebarOpen(false)}
        />
        <Sidebar
          datasets={DATASET_GUIDES}
          threads={store.threads}
          activeThreadId={store.activeThreadId}
          onNewChat={() => void handleNewChat()}
          onSelectThread={handleSelectThread}
          onDeleteThread={store.deleteThread}
          onClearAll={store.clearAll}
          onClose={() => setMobileSidebarOpen(false)}
          className={`relative h-full transition-transform duration-200 ${mobileSidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}
        />
      </div>

      <ChatArea
        datasets={DATASET_GUIDES}
        selectedDataset={selectedDataset}
        selectedDatasetId={selectedDataset.id}
        thread={store.activeThread}
        onOpenSidebar={() => setMobileSidebarOpen(true)}
        onMessagesChange={(msgs) => store.activeThread && store.updateMessages(store.activeThread.id, msgs)}
        onUpdateTitle={store.updateThreadTitle}
        onSelectDataset={(id) => void store.selectDataset(id)}
        onNewChat={() => void handleNewChat()}
        onEnsureThread={handleEnsureThread}
      />
    </div>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <Shell />
    </AuthProvider>
  );
}
