import { useCallback, useEffect, useMemo, useState } from 'react';
import { Gauge, Info, Library, MessageSquare } from 'lucide-react';
import { AuthProvider, useAuth } from '@/hooks/useAuth';
import { useThreadStore } from '@/hooks/useThreadStore';
import { AboutPage } from '@/components/AboutPage';
import { AdminPage } from '@/components/AdminPage';
import { AuthScreen } from '@/components/AuthScreen';
import { ChatArea } from '@/components/ChatArea';
import { DatasetCatalog } from '@/components/DatasetCatalog';
import { OnboardingTour } from '@/components/OnboardingTour';
import { SettingsModal } from '@/components/SettingsModal';
import { SharedThread } from '@/components/SharedThread';
import { Sidebar } from '@/components/Sidebar';
import { DATASET_GUIDES } from '@/lib/content';
import { getDatasetCatalog } from '@/lib/api';
import { TEXT_SIZE_PX, useSettings } from '@/lib/settings';
import type { DatasetCatalogEntry } from '@/types/chat';

function Shell() {
  const { user, loading } = useAuth();
  // Public share route: /share/<token> renders read-only without auth.
  const sharedMatch = typeof window !== 'undefined' ? window.location.pathname.match(/^\/share\/([A-Za-z0-9_-]+)\/?$/) : null;
  if (sharedMatch) return <SharedThread token={sharedMatch[1]} />;
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
  const { user } = useAuth();
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const [mainView, setMainView] = useState<'chat' | 'library' | 'admin' | 'about'>('chat');
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [prefillQuestion, setPrefillQuestion] = useState<string | null>(null);
  const [datasetCatalog, setDatasetCatalog] = useState<DatasetCatalogEntry[]>([]);
  const store = useThreadStore('cross_dataset');
  const settings = useSettings();

  // Answer text size preference -> CSS var read by .markdown-flow
  useEffect(() => {
    document.documentElement.style.setProperty('--prose-size', TEXT_SIZE_PX[settings.textSize]);
  }, [settings.textSize]);

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
        onRenameThread={store.updateThreadTitle}
        onDeleteThread={store.deleteThread}
        onOpenSettings={() => setSettingsOpen(true)}
        resizable
        className="hidden border-r border-[var(--sidebar-line)] lg:flex lg:flex-col"
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
          onRenameThread={store.updateThreadTitle}
          onDeleteThread={store.deleteThread}
          onOpenSettings={() => setSettingsOpen(true)}
          onClose={() => setMobileSidebarOpen(false)}
          className={`relative h-full transition-transform duration-200 ${mobileSidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}
        />
      </div>

      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <div className="shrink-0 border-b border-[var(--line)] bg-[var(--surface)]">
          <div className="mx-auto flex h-[58px] w-full max-w-7xl items-center justify-between gap-3 px-4 lg:px-7">
            <div className="flex min-w-0 items-center gap-2.5">
              <span className="grid h-7 w-7 shrink-0 place-items-center bg-[var(--ink)] text-[12px] font-bold text-white">
                M<span className="text-[var(--brand-red)]">.</span>
              </span>
              <span className="mop-wordmark hidden truncate text-[14px] text-[var(--ink)] sm:block">
                Maryland Opportunity Project
              </span>
            </div>

            <nav className="flex h-full items-stretch" aria-label="Workspace">
              <button
                type="button"
                onClick={() => setMainView('chat')}
                className="mop-nav-button"
                aria-current={mainView === 'chat' ? 'page' : undefined}
              >
                <MessageSquare size={13} />
                Assistant
              </button>
              <button
                type="button"
                onClick={() => setMainView('library')}
                className="mop-nav-button"
                aria-current={mainView === 'library' ? 'page' : undefined}
              >
                <Library size={13} />
                Data
              </button>
              {user?.is_admin && (
                <button
                  type="button"
                  onClick={() => setMainView('admin')}
                  className="mop-nav-button"
                  aria-current={mainView === 'admin' ? 'page' : undefined}
                >
                  <Gauge size={13} />
                  Admin
                </button>
              )}
              <button
                type="button"
                onClick={() => setMainView('about')}
                title="About Maryland Opportunity"
                className="mop-nav-button"
                aria-current={mainView === 'about' ? 'page' : undefined}
              >
                <Info size={13} />
                About
              </button>
            </nav>
          </div>
        </div>

        <div key={mainView} className="view-fade flex min-h-0 flex-1 flex-col overflow-hidden">
        {mainView === 'chat' && (
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
            prefillQuestion={prefillQuestion}
            onPrefillConsumed={() => setPrefillQuestion(null)}
          />
        )}
        {mainView === 'library' && (
          <DatasetCatalog
            datasets={DATASET_GUIDES}
            datasetCatalog={datasetCatalog}
            selectedDatasetId={selectedDataset.id}
            onSelectDataset={(id) => void store.selectDataset(id)}
            onUseInChat={(id, question) => {
              void (async () => {
                await store.selectDataset(id);
                setPrefillQuestion(question ?? null);
                setMainView('chat');
              })();
            }}
          />
        )}
        {mainView === 'admin' && user?.is_admin && <AdminPage />}
        {mainView === 'about' && <AboutPage />}
        </div>
      </div>
      <OnboardingTour />
      <SettingsModal
        isOpen={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        onThreadsCleared={store.clearAll}
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
