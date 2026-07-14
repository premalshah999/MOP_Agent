import { useEffect, useRef, useState } from 'react';
import embed from 'vega-embed';
import type { Result } from 'vega-embed';

interface VegaChartProps {
  spec: Record<string, unknown>;
  ariaLabel?: string;
}

/* Human number formatting for every axis/legend/text that doesn't set its
   own format. Replaces d3's raw SI notation, which rendered billions as
   "5G" and fractions as "800m" (milli) — both nonsense to a policy reader. */
function humanNumber(value: unknown): string {
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) return String(value ?? '');
  const a = Math.abs(n);
  const trim = (x: number) => {
    const s = x.toFixed(Math.abs(x) >= 100 ? 0 : Math.abs(x) >= 10 ? 1 : 2);
    return s.replace(/\.0+$/, '').replace(/(\.\d*[1-9])0+$/, '$1');
  };
  if (a >= 1e12) return `${trim(n / 1e12)}T`;
  if (a >= 1e9) return `${trim(n / 1e9)}B`;
  if (a >= 1e6) return `${trim(n / 1e6)}M`;
  if (a >= 1e4) return `${trim(n / 1e3)}k`;
  if (a >= 1) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return n.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

/* Dispatcher: Vega hands us (value, formatString). Honor the handful of
   explicit d3 patterns our server-side specs emit; everything else gets the
   human default. */
function mopFormat(value: unknown, params?: string): string {
  const fmt = params ?? '';
  const n = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(n)) return String(value ?? '');
  if (fmt.startsWith('$')) {
    const core = humanNumber(n);
    return fmt.includes('f')
      ? `${n < 0 ? '-' : ''}$${Math.abs(Math.round(n)).toLocaleString()}`
      : `${n < 0 ? '-' : ''}$${core.replace(/^-/, '')}`;
  }
  if (fmt.includes('s')) return humanNumber(n); // our '~s' axes want compact units
  if (fmt.includes('%')) return `${(n * 100).toFixed(1)}%`;
  if (fmt.includes('f') || fmt.includes(',')) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return humanNumber(n);
}

export function VegaChart({ spec, ariaLabel = 'Data visualization' }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [renderKey, setRenderKey] = useState(0);

  useEffect(() => {
    if (!containerRef.current || !spec) return;

    let disposed = false;
    let result: Result | null = null;
    let resizeObserver: ResizeObserver | null = null;
    const normalizedSpec =
      typeof spec.$schema === 'string' && spec.$schema.includes('/vega-lite/v5')
        ? { ...spec, $schema: 'https://vega.github.io/schema/vega-lite/v6.json' }
        : spec;

    const renderChart = async () => {
      if (disposed || !containerRef.current) return;
      try {
        setError(null);
        result = await embed(containerRef.current, normalizedSpec as never, {
          // PNG/SVG download menu (vega-embed actions); kept compact.
          actions: { export: { svg: true, png: true }, source: false, compiled: false, editor: false },
          renderer: 'svg',
          // All number formatting flows through mopFormat (see dispatcher above).
          expressionFunctions: { mopFormat: { fn: mopFormat } } as never,
          config: {
            font: 'Inter, ui-sans-serif, system-ui, sans-serif',
            background: 'transparent',
            padding: 6,
            customFormatTypes: true,
            numberFormatType: 'mopFormat',
            numberFormat: 'auto',
            // Default hover tooltips on every mark; specs can still override.
            mark: { tooltip: true },
            axis: {
              domain: false,
              ticks: false,
              labelPadding: 8,
              gridColor: '#eae8de',
              gridWidth: 1,
              gridDash: [2, 3],
              labelColor: '#6e6d64',
              titleColor: '#3d3c38',
              labelFontSize: 11,
              titleFontSize: 11,
              titleFontWeight: 500,
              labelFont: 'Inter',
              titleFont: 'Inter',
            },
            view: { stroke: null },
            style: { 'guide-label': { font: 'Inter' }, 'guide-title': { font: 'Inter' } },
            bar: { color: '#c6613f', cornerRadiusEnd: 5 },
            line: { color: '#c6613f', strokeWidth: 2 },
            point: { color: '#c6613f', size: 72, filled: true },
            circle: { color: '#c6613f' },
            rule: { color: '#d8d5c9' },
            area: { color: '#c6613f', opacity: 0.1 },
            rect: { cornerRadius: 2 },
            legend: {
              labelFont: 'Inter',
              titleFont: 'Inter',
              labelColor: '#6e6d64',
              titleColor: '#3d3c38',
              labelFontSize: 11,
              symbolType: 'circle',
            },
          },
        });
        if (disposed || !containerRef.current) return;
        resizeObserver = new ResizeObserver(() => {
          if (!disposed && result) void result.view.resize().runAsync();
        });
        resizeObserver.observe(containerRef.current);
      } catch (err) {
        console.warn('[VegaChart] Render failed:', err);
        if (!disposed) setError(err instanceof Error ? err.message : 'Chart could not be rendered.');
      }
    };

    void renderChart();

    return () => {
      disposed = true;
      resizeObserver?.disconnect();
      result?.finalize();
    };
  }, [spec, renderKey]);

  if (error) {
    return (
      <div className="mt-3 flex items-center justify-between rounded-[10px] border border-[var(--line-soft)] bg-[var(--surface)] px-4 py-3 text-[12px] text-[var(--muted)]">
        <span>Chart couldn't render — the data fetched fine, but the visualization spec is malformed.</span>
        <button
          type="button"
          onClick={() => setRenderKey((k) => k + 1)}
          className="ml-3 rounded-md border border-[var(--line)] px-2 py-1 text-[11px] text-[var(--ink)] hover:bg-[var(--surface-2)]"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      role="img"
      aria-label={ariaLabel}
      className="mt-3 w-full overflow-x-auto overflow-y-visible rounded-[10px] border border-[var(--line-soft)] bg-[var(--surface)] px-4 py-3"
    />
  );
}
