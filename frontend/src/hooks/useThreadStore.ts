import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  apiClearAllThreads,
  apiCreateThread,
  apiDeleteThread,
  apiGetMessages,
  apiListThreads,
  apiUpdateThread,
  type ApiMessage,
  type ApiThread,
} from '@/lib/api';
import type { ChatMessage, ChatThread } from '@/types/chat';

function toThread(api: ApiThread, messages: ChatMessage[] = []): ChatThread {
  return {
    id: api.id,
    title: api.title,
    datasetId: api.datasetId,
    createdAt: api.createdAt,
    updatedAt: api.updatedAt,
    messages: api.messages ? api.messages.map(toMessage) : messages,
  };
}

function toMessage(api: ApiMessage): ChatMessage {
  return {
    id: api.id,
    role: api.role,
    content: api.content,
    ts: api.ts,
    sqlQuery: api.sqlQuery,
    data: api.data,
    rowCount: api.rowCount,
    chart: api.chart,
    charts: (api as ApiMessage & { charts?: ChatMessage['charts'] }).charts,
    evidence: api.evidence,
    resolution: api.resolution,
    mapIntent: api.mapIntent ?? undefined,
    resultPackage: api.resultPackage,
    contract: api.contract,
    pipelineTrace: api.pipelineTrace,
    quality: api.quality,
    error: api.error,
    keyNumbers: api.keyNumbers,
    caveats: api.caveats,
    confidence: api.confidence,
    glossary: api.glossary,
    suggestedFollowups: api.suggestedFollowups,
  };
}

export function useThreadStore(defaultDatasetId: string) {
  const [threads, setThreads] = useState<ChatThread[]>([]);
  const [activeThreadId, setActiveThreadId] = useState<string | null>(null);
  const [selectedDatasetId, setSelectedDatasetId] = useState(defaultDatasetId);
  const [loading, setLoading] = useState(true);
  const loadedThreadsRef = useRef<Set<string>>(new Set());

  // Load threads from server on mount
  useEffect(() => {
    let active = true;
    apiListThreads()
      .then((apiThreads) => {
        if (!active) return;
        const converted = apiThreads.map((t) => toThread(t));
        setThreads(converted);
        if (converted.length > 0) {
          setActiveThreadId(converted[0].id);
          setSelectedDatasetId(converted[0].datasetId);
        }
      })
      .catch((err) => {
        console.error('[MOP] Failed to load threads:', err);
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => { active = false; };
  }, []);

  // Load messages when switching to a thread that hasn't been loaded yet
  useEffect(() => {
    if (!activeThreadId || loadedThreadsRef.current.has(activeThreadId)) return;
    loadedThreadsRef.current.add(activeThreadId);

    apiGetMessages(activeThreadId)
      .then((msgs) => {
        setThreads((prev) =>
          prev.map((t) =>
            t.id === activeThreadId ? { ...t, messages: msgs.map(toMessage) } : t,
          ),
        );
      })
      .catch((err) => {
        // A transient failure must not permanently mark this thread as loaded;
        // switching away and back should retry the request.
        loadedThreadsRef.current.delete(activeThreadId);
        console.error('[MOP] Failed to load messages:', err);
      });
  }, [activeThreadId]);

  const activeThread = useMemo(
    () => threads.find((t) => t.id === activeThreadId) ?? (threads.length > 0 ? threads[0] : undefined),
    [threads, activeThreadId],
  );

  const updateMessages = useCallback(
    (threadId: string, messages: ChatMessage[]) => {
      setThreads((prev) =>
        prev.map((t) =>
          t.id === threadId
            ? { ...t, messages, updatedAt: new Date().toISOString() }
            : t,
        ),
      );
    },
    [],
  );

  const updateThreadTitle = useCallback(
    async (threadId: string, title: string) => {
      try {
        const updated = await apiUpdateThread(threadId, { title });
        setThreads((prev) =>
          prev.map((t) => (t.id === threadId ? { ...t, title: updated.title } : t)),
        );
      } catch (err) {
        console.error('[MOP] Failed to rename thread:', err);
      }
    },
    [],
  );

  const createThread = useCallback(
    async (datasetId: string) => {
      try {
        const apiThread = await apiCreateThread(datasetId);
        const thread = toThread(apiThread, []);
        setThreads((prev) => [thread, ...prev]);
        setActiveThreadId(thread.id);
        setSelectedDatasetId(datasetId);
        loadedThreadsRef.current.add(thread.id);
        return thread;
      } catch (err) {
        console.error('[MOP] Failed to create thread:', err);
        return null;
      }
    },
    [],
  );

  const selectThread = useCallback(
    (threadId: string) => {
      const t = threads.find((x) => x.id === threadId);
      if (!t) return;
      setActiveThreadId(threadId);
      setSelectedDatasetId(t.datasetId);
    },
    [threads],
  );

  const deleteThread = useCallback(
    async (threadId: string) => {
      try {
        await apiDeleteThread(threadId);
        setThreads((prev) => {
          const next = prev.filter((t) => t.id !== threadId);
          if (threadId === activeThreadId && next.length > 0) {
            setActiveThreadId(next[0].id);
            setSelectedDatasetId(next[0].datasetId);
          } else if (next.length === 0) {
            setActiveThreadId(null);
          }
          return next;
        });
        loadedThreadsRef.current.delete(threadId);
      } catch (err) {
        console.error('[MOP] Failed to delete thread:', err);
      }
    },
    [activeThreadId],
  );

  const selectDataset = useCallback(
    async (datasetId: string) => {
      if (!activeThread || activeThread.messages.length === 0) {
        // Update the current empty thread's dataset
        if (activeThread) {
          try {
            await apiUpdateThread(activeThread.id, { dataset_id: datasetId });
            setThreads((prev) =>
              prev.map((t) => (t.id === activeThread.id ? { ...t, datasetId } : t)),
            );
            setSelectedDatasetId(datasetId);
          } catch (err) {
            console.error('[MOP] Failed to change dataset:', err);
          }
        } else {
          setSelectedDatasetId(datasetId);
        }
        return;
      }
      // Create a new thread for the new dataset
      await createThread(datasetId);
    },
    [activeThread, createThread],
  );

  const clearAll = useCallback(
    async () => {
      await apiClearAllThreads();
      setThreads([]);
      setActiveThreadId(null);
      loadedThreadsRef.current.clear();
    },
    [],
  );

  return {
    threads,
    activeThread,
    activeThreadId,
    selectedDatasetId,
    loading,
    updateMessages,
    updateThreadTitle,
    createThread,
    selectThread,
    deleteThread,
    selectDataset,
    clearAll,
  };
}
