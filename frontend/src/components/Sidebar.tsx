import { LogOut, Plus, Trash2, X } from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { useResizable } from '@/hooks/useResizable';
import type { DatasetGuide } from '@/lib/content';
import type { ChatThread } from '@/types/chat';

function formatTime(value: string): string {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '';
  const now = new Date();
  if (d.toDateString() === now.toDateString()) {
    return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
  }
  return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
}

interface SidebarProps {
  datasets: DatasetGuide[];
  threads: ChatThread[];
  activeThreadId: string | null;
  onOpenChat: () => void;
  onNewChat: () => void;
  onSelectThread: (id: string) => void;
  onDeleteThread: (id: string) => void;
  className?: string;
  onClose?: () => void;
  /** If true, sidebar manages its own width via drag handle. If false (mobile), uses fixed 280px. */
  resizable?: boolean;
}

export function Sidebar({
  datasets,
  threads,
  activeThreadId,
  onOpenChat,
  onNewChat,
  onSelectThread,
  onDeleteThread,
  className = '',
  onClose,
  resizable = false,
}: SidebarProps) {
  const { user, signOut } = useAuth();
  const { width, onMouseDown } = useResizable({
    initial: 280, min: 220, max: 420, edge: 'right', storageKey: 'mop-sidebar-w',
  });

  const ordered = [...threads].sort(
    (a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime(),
  );

  return (
    <aside
      className={`relative shrink-0 border-r border-[var(--sidebar-line)] bg-[var(--sidebar-bg)] text-[var(--sidebar-ink)] ${className}`}
      style={resizable ? { width } : { width: 280 }}
    >
      {/* Drag handle (right edge) */}
      {resizable && (
        <div
          onMouseDown={onMouseDown}
          className="absolute right-0 top-0 z-30 h-full w-1 cursor-col-resize hover:bg-[var(--sidebar-line)] active:bg-[var(--sidebar-line)]"
        />
      )}

      <div className="flex h-full flex-col">
        {/* Header */}
        <div className="px-4 pt-5 pb-4">
          <div className="flex items-center justify-between">
            <span className="font-display text-[18px] font-semibold tracking-tight text-[var(--ink)]">
              MOP Agent<span className="text-[var(--accent)]">.</span>
            </span>
            {onClose && (
              <button
                type="button"
                onClick={onClose}
                aria-label="Close sidebar"
                className="p-1 text-[var(--sidebar-muted)] hover:text-[var(--sidebar-ink)] lg:hidden"
              >
                <X size={14} />
              </button>
            )}
          </div>

          <button
            type="button"
            onClick={() => {
              onOpenChat();
              onNewChat();
            }}
            className="mt-4 flex w-full items-center justify-center gap-1.5 border border-[var(--sidebar-line)] bg-[var(--surface)] py-2 text-[11px] font-medium tracking-[0.16em] text-[var(--sidebar-ink)] transition hover:bg-[var(--sidebar-hover)]"
          >
            <Plus size={12} />
            New chat
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-2 pb-2">
          <div className="px-2 pb-2 text-[10px] font-medium uppercase tracking-widest text-[var(--sidebar-muted)]">
            Recent
          </div>

          {ordered.map((thread) => {
            const ds = datasets.find((d) => d.id === thread.datasetId);
            const active = thread.id === activeThreadId;
            return (
              <div
                key={thread.id}
            className={`group flex items-center gap-1 border border-transparent px-2 py-2 transition ${
                  active ? 'bg-[var(--sidebar-active)]' : 'hover:bg-[var(--sidebar-hover)]'
                }`}
              >
                <button
                  type="button"
                  onClick={() => {
                    onOpenChat();
                    onSelectThread(thread.id);
                  }}
                  className="min-w-0 flex-1 text-left"
                >
                  <div className="truncate text-[13px] leading-5 text-[var(--sidebar-ink)]">
                    {thread.title}
                  </div>
                  <div className="mt-0.5 flex items-center gap-1.5 text-[10px] text-[var(--sidebar-muted)]">
                    <span>{ds?.shortLabel ?? ''}</span>
                    <span className="opacity-30">·</span>
                    <span>{formatTime(thread.updatedAt)}</span>
                  </div>
                </button>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); onDeleteThread(thread.id); }}
                  className="shrink-0 p-1 text-[var(--sidebar-muted)] opacity-0 transition group-hover:opacity-100 hover:text-red-500"
                  title="Delete thread"
                  aria-label="Delete thread"
                >
                  <Trash2 size={12} />
                </button>
              </div>
            );
          })}
        </div>

        {/* Footer — user profile */}
        <div className="border-t border-[var(--sidebar-line)] px-4 py-3">
          {user && (
            <div className="flex items-center justify-between">
              <div className="min-w-0">
                <div className="truncate text-[12px] font-medium text-[var(--sidebar-ink)]">{user.name}</div>
                <div className="truncate text-[10px] text-[var(--sidebar-muted)]">{user.email}</div>
              </div>
              <button
                type="button"
                onClick={signOut}
                className="p-1.5 text-[var(--sidebar-muted)] transition hover:text-[var(--sidebar-ink)]"
                title="Sign out"
                aria-label="Sign out"
              >
                <LogOut size={13} />
              </button>
            </div>
          )}
        </div>
      </div>
    </aside>
  );
}
