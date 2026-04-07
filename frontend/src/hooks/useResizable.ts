import { useCallback, useEffect, useRef, useState } from 'react';

interface UseResizableOptions {
  /** Initial width in pixels */
  initial: number;
  /** Minimum width in pixels */
  min: number;
  /** Maximum width in pixels */
  max: number;
  /** Which edge the handle sits on: 'left' means dragging from the left edge, 'right' from the right */
  edge: 'left' | 'right';
  /** localStorage key to persist width (optional) */
  storageKey?: string;
}

export function useResizable({ initial, min, max, edge, storageKey }: UseResizableOptions) {
  const [width, setWidth] = useState(() => {
    if (storageKey) {
      try {
        const saved = localStorage.getItem(storageKey);
        if (saved) {
          const n = Number(saved);
          if (Number.isFinite(n) && n >= min && n <= max) return n;
        }
      } catch { /* ignore */ }
    }
    return initial;
  });

  const dragging = useRef(false);
  const startX = useRef(0);
  const startW = useRef(0);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    startX.current = e.clientX;
    startW.current = width;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
  }, [width]);

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      const delta = e.clientX - startX.current;
      const next = edge === 'right'
        ? startW.current + delta
        : startW.current - delta;
      setWidth(Math.max(min, Math.min(max, next)));
    };

    const onMouseUp = () => {
      if (!dragging.current) return;
      dragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, [min, max, edge]);

  // Persist
  useEffect(() => {
    if (storageKey) {
      try { localStorage.setItem(storageKey, String(width)); } catch { /* ignore */ }
    }
  }, [width, storageKey]);

  return { width, onMouseDown };
}
