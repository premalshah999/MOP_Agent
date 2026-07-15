import { LogOut, Plus, Settings, Trash2, X } from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { useResizable } from '@/hooks/useResizable';
import type { DatasetGuide } from '@/lib/content';
import type { ChatThread } from '@/types/chat';

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
  onOpenSettings?: () => void;
  /** If true, sidebar manages its own width via drag handle. If false (mobile), uses fixed 280px. */
  resizable?: boolean;
}

export function Sidebar({
  datasets: _datasets,
  threads,
  activeThreadId,
  onOpenChat,
  onNewChat,
  onSelectThread,
  onDeleteThread,
  className = '',
  onClose,
  onOpenSettings,
  resizable = false,
}: SidebarProps) {
  const { user, signOut } = useAuth();
  const { width, onMouseDown } = useResizable({
    initial: 272, min: 220, max: 400, edge: 'right', storageKey: 'mop-sidebar-w',
  });

  const ordered = [...threads].sort(
    (a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime(),
  );

  return (
    <aside
      className={`relative shrink-0 bg-[var(--sidebar-bg)] text-[var(--sidebar-ink)] ${className}`}
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
        <div className="px-3 pb-2 pt-4">
          <div className="flex items-center justify-between px-2">
            <span className="font-display text-[17px] font-semibold tracking-tight text-[var(--ink)]">
              Maryland Opportunity
            </span>
            {onClose && (
              <button
                type="button"
                onClick={onClose}
                aria-label="Close sidebar"
                className="rounded-md p-1 text-[var(--sidebar-muted)] hover:text-[var(--sidebar-ink)] lg:hidden"
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
            className="mt-4 flex w-full items-center gap-2 rounded-xl px-2.5 py-2 text-[13.5px] font-medium text-[var(--accent)] transition hover:bg-[var(--sidebar-hover)]"
          >
            <span className="grid h-6 w-6 place-items-center rounded-full bg-[var(--accent)] text-white">
              <Plus size={13} strokeWidth={2.4} />
            </span>
            New chat
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-3 pb-2">
          <div className="px-2.5 pb-1.5 pt-2 text-[11px] font-medium text-[var(--sidebar-muted)]">
            Recents
          </div>

          {ordered.map((thread) => {
            const active = thread.id === activeThreadId;
            return (
              <div
                key={thread.id}
                className={`group flex items-center gap-1 rounded-lg px-2.5 py-[7px] transition ${
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
                  <div className="truncate text-[13.5px] leading-5 text-[var(--sidebar-ink)]">
                    {thread.title}
                  </div>
                </button>
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); onDeleteThread(thread.id); }}
                  className="shrink-0 rounded-md p-1 text-[var(--sidebar-muted)] opacity-0 transition group-hover:opacity-100 hover:text-[var(--danger)]"
                  title="Delete chat"
                  aria-label="Delete chat"
                >
                  <Trash2 size={12} />
                </button>
              </div>
            );
          })}
          {ordered.length === 0 && (
            <div className="px-2.5 py-2 text-[12px] text-[var(--sidebar-muted)]">
              No chats yet
            </div>
          )}
        </div>

        {/* Footer — user profile */}
        <div className="border-t border-[var(--sidebar-line)] px-3 py-2.5">
          {user && (
            <div className="flex items-center gap-2.5 rounded-lg px-1.5 py-1">
              <span className="grid h-7 w-7 shrink-0 place-items-center rounded-full bg-[var(--accent-soft)] text-[12px] font-semibold text-[var(--accent)]">
                {(user.name || user.email || '?').charAt(0).toUpperCase()}
              </span>
              <div className="min-w-0 flex-1">
                <div className="truncate text-[12.5px] font-medium text-[var(--sidebar-ink)]">{user.name}</div>
                <div className="truncate text-[10.5px] text-[var(--sidebar-muted)]">{user.email}</div>
              </div>
              {onOpenSettings && (
                <button
                  type="button"
                  onClick={onOpenSettings}
                  className="rounded-md p-1.5 text-[var(--sidebar-muted)] transition hover:text-[var(--sidebar-ink)]"
                  title="Settings"
                  aria-label="Settings"
                >
                  <Settings size={13} />
                </button>
              )}
              <button
                type="button"
                onClick={signOut}
                className="rounded-md p-1.5 text-[var(--sidebar-muted)] transition hover:text-[var(--sidebar-ink)]"
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
