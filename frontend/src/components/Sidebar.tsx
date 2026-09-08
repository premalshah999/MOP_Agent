import { useState } from 'react';
import { LogOut, Pencil, Plus, Settings, Trash2, X } from 'lucide-react';
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
  onRenameThread: (id: string, title: string) => void;
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
  onRenameThread,
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
  const [editingThreadId, setEditingThreadId] = useState<string | null>(null);
  const [draftTitle, setDraftTitle] = useState('');

  const beginRename = (thread: ChatThread) => {
    setEditingThreadId(thread.id);
    setDraftTitle(thread.title);
  };

  const commitRename = (thread: ChatThread) => {
    const title = draftTitle.trim();
    if (title && title !== thread.title) onRenameThread(thread.id, title);
    setEditingThreadId(null);
    setDraftTitle('');
  };

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
        <div className="border-b border-[var(--sidebar-line)] px-4 pb-4 pt-5">
          <div className="flex items-center justify-between">
            <div>
              <div className="mop-kicker">Data workspace</div>
              <span className="mop-wordmark mt-1 block text-[16px] text-[var(--ink)]">
                MOP Research Assistant
              </span>
            </div>
            {onClose && (
              <button
                type="button"
                onClick={onClose}
                aria-label="Close sidebar"
                className="p-1 text-[var(--sidebar-muted)] hover:text-[var(--brand-red)] lg:hidden"
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
            className="mop-primary-button mt-4 flex w-full items-center justify-center gap-2 px-3 py-2.5"
          >
            <Plus size={13} strokeWidth={2.4} />
            New chat
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-3 pb-2">
          <div className="px-2.5 pb-2 pt-4 text-[9px] font-semibold uppercase tracking-[0.18em] text-[var(--sidebar-muted)]">
            Recents
          </div>

          {ordered.map((thread) => {
            const active = thread.id === activeThreadId;
            return (
              <div
                key={thread.id}
                className={`group flex items-center gap-1 border-l-2 px-2.5 py-[8px] transition ${
                  active
                    ? 'border-[var(--brand-red)] bg-[var(--sidebar-active)]'
                    : 'border-transparent hover:bg-[var(--sidebar-hover)]'
                }`}
              >
                {editingThreadId === thread.id ? (
                  <input
                    autoFocus
                    value={draftTitle}
                    onChange={(event) => setDraftTitle(event.target.value)}
                    onBlur={() => commitRename(thread)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') {
                        event.preventDefault();
                        commitRename(thread);
                      } else if (event.key === 'Escape') {
                        event.preventDefault();
                        setEditingThreadId(null);
                        setDraftTitle('');
                      }
                    }}
                    onClick={(event) => event.stopPropagation()}
                    aria-label={`Rename ${thread.title}`}
                    className="min-w-0 flex-1 border border-[var(--sidebar-line)] bg-[var(--surface)] px-1.5 py-0.5 text-[13px] leading-5 text-[var(--sidebar-ink)] outline-none focus:border-[var(--brand-red)]"
                  />
                ) : (
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
                )}
                {editingThreadId !== thread.id && (
                  <button
                    type="button"
                    onClick={(event) => { event.stopPropagation(); beginRename(thread); }}
                    className="shrink-0 p-1 text-[var(--sidebar-muted)] opacity-0 transition group-hover:opacity-100 hover:text-[var(--brand-red)] focus:opacity-100"
                    title="Rename chat"
                    aria-label={`Rename ${thread.title}`}
                  >
                    <Pencil size={12} />
                  </button>
                )}
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); onDeleteThread(thread.id); }}
                  className="shrink-0 p-1 text-[var(--sidebar-muted)] opacity-0 transition group-hover:opacity-100 hover:text-[var(--danger)]"
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
            <div className="flex items-center gap-2.5 px-1.5 py-1">
              <span className="grid h-7 w-7 shrink-0 place-items-center bg-[var(--ink)] text-[12px] font-semibold text-white">
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
                  className="p-1.5 text-[var(--sidebar-muted)] transition hover:text-[var(--brand-red)]"
                  title="Settings"
                  aria-label="Settings"
                >
                  <Settings size={13} />
                </button>
              )}
              <button
                type="button"
                onClick={signOut}
                className="p-1.5 text-[var(--sidebar-muted)] transition hover:text-[var(--brand-red)]"
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
