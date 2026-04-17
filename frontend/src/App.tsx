import { useCallback, useEffect, useMemo, useState } from 'react';
import { AuthProvider, useAuth } from '@/hooks/useAuth';
import { useThreadStore } from '@/hooks/useThreadStore';
import { AuthScreen } from '@/components/AuthScreen';
import { ChatArea } from '@/components/ChatArea';
import { DatasetLibraryWorkspace } from '@/components/DatasetLibraryWorkspace';
import { Sidebar } from '@/components/Sidebar';
import { DATASET_GUIDES } from '@/lib/content';
import { getDatasetCatalog } from '@/lib/api';
import type { DatasetCatalogEntry } from '@/types/chat';

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
  const [mainView, setMainView] = useState<'chat' | 'library'>('chat');
  const [datasetCatalog, setDatasetCatalog] = useState<DatasetCatalogEntry[]>([]);
  const store = useThreadStore('cross_dataset');

  useEffect(() => {
    let active = true;
    void getDatasetCatalog()
      .then((catalog) => {
        if (active) setDatasetCatalog(catalog);
      })
      .catch((err) => {
        console.error('[MOP] Failed to load dataset catalog:', err);
      });
    return () => {
      active = false;
    };
  }, []);

  const selectedDataset = useMemo(
    () =>
      DATASET_GUIDES.find((d) => d.id === (store.activeThread?.datasetId || store.selectedDatasetId))
      ?? DATASET_GUIDES.find((d) => d.id === 'cross_dataset')
      ?? DATASET_GUIDES[0],
    [store.activeThread, store.selectedDatasetId],
  );

  const handleNewChat = useCallback(async () => {
    await store.createThread('cross_dataset');
    setMainView('chat');
    setMobileSidebarOpen(false);
  }, [store]);

  const handleOpenChat = useCallback(() => {
    setMainView('chat');
    setMobileSidebarOpen(false);
  }, []);

  const handleOpenLibrary = useCallback(() => {
    setMainView('library');
    setMobileSidebarOpen(false);
  }, []);

  const handleSelectThread = useCallback((id: string) => {
    store.selectThread(id);
    setMainView('chat');
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
        onOpenChat={handleOpenChat}
        onNewChat={() => void handleNewChat()}
        onSelectThread={handleSelectThread}
        onDeleteThread={store.deleteThread}
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
          onOpenChat={handleOpenChat}
          onNewChat={() => void handleNewChat()}
          onSelectThread={handleSelectThread}
          onDeleteThread={store.deleteThread}
          onClose={() => setMobileSidebarOpen(false)}
          className={`relative h-full transition-transform duration-200 ${mobileSidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}
        />
      </div>

      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <div className="shrink-0 border-b border-[var(--line)] bg-[var(--bg)]/95 backdrop-blur">
          <div className="mx-auto flex w-full max-w-7xl items-center justify-between gap-3 px-5 py-3 lg:px-8">
            <div>
              <p className="text-[11px] font-medium uppercase tracking-[0.24em] text-[var(--muted)]">Workspace</p>
              <p className="mt-1 text-[13px] text-[var(--ink-soft)]">
                Keep chat focused. Browse downloads in the separate data library.
              </p>
            </div>

            <div className="inline-flex rounded-[10px] border border-[var(--line)] bg-[var(--surface)] p-1 shadow-sm">
              <button
                type="button"
                onClick={() => setMainView('chat')}
                className={`rounded-[8px] px-4 py-2 text-[12px] font-medium transition ${
                  mainView === 'chat'
                    ? 'bg-[var(--ink)] text-white'
                    : 'text-[var(--muted)] hover:text-[var(--ink)]'
                }`}
              >
                Assistant
              </button>
              <button
                type="button"
                onClick={() => setMainView('library')}
                className={`rounded-[8px] px-4 py-2 text-[12px] font-medium transition ${
                  mainView === 'library'
                    ? 'bg-[var(--ink)] text-white'
                    : 'text-[var(--muted)] hover:text-[var(--ink)]'
                }`}
              >
                Data Library
              </button>
            </div>
          </div>
        </div>

        {mainView === 'chat' ? (
          <ChatArea
            datasets={DATASET_GUIDES}
            selectedDataset={selectedDataset}
            selectedDatasetId={selectedDataset.id}
            thread={store.activeThread}
            onOpenSidebar={() => setMobileSidebarOpen(true)}
            onMessagesChange={(threadId, msgs) => store.updateMessages(threadId, msgs)}
            onUpdateTitle={store.updateThreadTitle}
            onSelectDataset={(id) => void store.selectDataset(id)}
            onEnsureThread={handleEnsureThread}
          />
        ) : (
          <DatasetLibraryWorkspace
            datasets={DATASET_GUIDES}
            datasetCatalog={datasetCatalog}
            selectedDatasetId={selectedDataset.id}
            onSelectDataset={(id) => void store.selectDataset(id)}
            onUseInChat={(id) => {
              void store.selectDataset(id);
              setMainView('chat');
            }}
          />
        )}
      </div>
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
