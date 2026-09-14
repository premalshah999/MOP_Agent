import { BarChart3, Download, Maximize2, Minimize2 } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import embed from 'vega-embed';
import type { Result } from 'vega-embed';

interface VegaChartProps {
  spec: Record<string, unknown>;
  ariaLabel?: string;
  title?: string;
  subtitle?: string;
}

type ExportFormat = 'png' | 'svg';

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
  if (fmt.includes('s')) return humanNumber(n);
  if (fmt.includes('%')) return `${(n * 100).toFixed(1)}%`;
  if (fmt.includes('f') || fmt.includes(',')) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return humanNumber(n);
}

function valuesCount(spec: Record<string, unknown>): number | null {
  const data = spec.data;
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null;
  const values = (data as Record<string, unknown>).values;
  return Array.isArray(values) ? values.length : null;
}

function markTypes(spec: Record<string, unknown>): Set<string> {
  const types = new Set<string>();
  const visit = (value: unknown) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return;
    const node = value as Record<string, unknown>;
    if (typeof node.mark === 'string') types.add(node.mark);
    if (node.mark && typeof node.mark === 'object' && !Array.isArray(node.mark)) {
      const type = (node.mark as Record<string, unknown>).type;
      if (typeof type === 'string') types.add(type);
    }
    if (Array.isArray(node.layer)) node.layer.forEach(visit);
  };
  visit(spec);
  return types;
}

function visualizationKind(title: string, spec: Record<string, unknown>): string {
  const lowerTitle = title.toLowerCase();
  const marks = markTypes(spec);
  const encoding = spec.encoding && typeof spec.encoding === 'object' ? spec.encoding as Record<string, unknown> : {};
  const x = encoding.x && typeof encoding.x === 'object' ? encoding.x as Record<string, unknown> : {};
  if (lowerTitle.includes('correlation') || marks.has('circle') || (marks.has('point') && !marks.has('rule'))) return 'Relationship';
  if (lowerTitle.includes('distribution') || Boolean(x.bin)) return 'Distribution';
  if (lowerTitle.includes('over time') || marks.has('line') || marks.has('area')) return 'Trend';
  if (marks.has('rect')) return 'Matrix';
  if (lowerTitle.includes('comparison') || lowerTitle.includes('compared')) return 'Comparison';
  return 'Ranked view';
}

function fileStem(title: string): string {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 64) || 'mop-visualization';
}

export function VegaChart({
  spec,
  ariaLabel = 'Data visualization',
  title = 'Answer visualization',
  subtitle,
}: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const resultRef = useRef<Result | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [renderKey, setRenderKey] = useState(0);
  const [expanded, setExpanded] = useState(false);
  const [exporting, setExporting] = useState<ExportFormat | null>(null);
  const count = useMemo(() => valuesCount(spec), [spec]);
  const kind = useMemo(() => visualizationKind(title, spec), [spec, title]);

  useEffect(() => {
    if (!expanded) return undefined;
    const prior = document.body.style.overflow;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setExpanded(false);
    };
    document.body.style.overflow = 'hidden';
    document.addEventListener('keydown', onKey);
    return () => {
      document.body.style.overflow = prior;
      document.removeEventListener('keydown', onKey);
    };
  }, [expanded]);

  useEffect(() => {
    window.requestAnimationFrame(() => {
      if (resultRef.current) void resultRef.current.view.resize().runAsync();
    });
  }, [expanded]);

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
          actions: false,
          renderer: 'svg',
          // Vega otherwise compiles expressions with Function(), which is
          // blocked by the app's production CSP. AST mode uses the bundled
          // interpreter and keeps charts compatible without unsafe-eval.
          ast: true,
          expressionFunctions: { mopFormat: { fn: mopFormat } } as never,
          config: {
            font: 'Inter, ui-sans-serif, system-ui, sans-serif',
            background: 'transparent',
            padding: 10,
            customFormatTypes: true,
            numberFormatType: 'mopFormat',
            numberFormat: 'auto',
            mark: { tooltip: true },
            axis: {
              domain: false,
              ticks: false,
              labelPadding: 9,
              gridColor: '#e8edf3',
              gridWidth: 1,
              gridDash: [2, 4],
              labelColor: '#5f6f82',
              titleColor: '#344256',
              labelFontSize: 11,
              titleFontSize: 11,
              titleFontWeight: 600,
              labelFont: 'Inter',
              titleFont: 'Inter',
            },
            view: { stroke: null },
            style: { 'guide-label': { font: 'Inter' }, 'guide-title': { font: 'Inter' } },
            range: {
              category: ['#e03a3e', '#24364f', '#f2b134', '#2a8f85', '#6f5bd3', '#9f4f71'],
            },
            bar: { color: '#24364f', cornerRadiusEnd: 5 },
            line: { color: '#e03a3e', strokeWidth: 2.5 },
            point: { color: '#e03a3e', size: 76, filled: true, stroke: '#ffffff', strokeWidth: 1.5 },
            circle: { color: '#e03a3e' },
            rule: { color: '#b9c4d0' },
            area: { color: '#e03a3e', opacity: 0.1 },
            rect: { cornerRadius: 3 },
            legend: {
              orient: 'bottom',
              direction: 'horizontal',
              labelFont: 'Inter',
              titleFont: 'Inter',
              labelColor: '#5f6f82',
              titleColor: '#344256',
              labelFontSize: 11,
              titleFontSize: 11,
              symbolType: 'circle',
              symbolSize: 70,
              offset: 14,
            },
          },
        });
        if (disposed || !containerRef.current) return;
        resultRef.current = result;
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
      resultRef.current = null;
      result?.finalize();
    };
  }, [spec, renderKey]);

  const exportChart = async (format: ExportFormat) => {
    const result = resultRef.current;
    if (!result) return;
    setExporting(format);
    try {
      const url = await result.view.toImageURL(format, format === 'png' ? 2 : 1);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `${fileStem(title)}.${format}`;
      anchor.click();
    } finally {
      setExporting(null);
    }
  };

  return (
    <section
      className={`visualization-card ${expanded ? 'fixed inset-3 z-[140] flex flex-col sm:inset-7' : 'relative'}`}
      aria-label={ariaLabel}
    >
      <div className="visualization-accent" />
      <header className="flex flex-wrap items-start justify-between gap-3 px-4 pb-3 pt-4 sm:px-5 sm:pt-5">
        <div className="min-w-0 flex-1">
          <div className="mb-1.5 flex items-center gap-2">
            <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-[var(--accent-soft)] text-[var(--brand-red)]">
              <BarChart3 size={13} strokeWidth={2} />
            </span>
            <span className="mop-kicker">{kind}</span>
          </div>
          <h4 className="font-display text-[18px] font-medium leading-6 text-[var(--ink)] sm:text-[20px]">
            {title}
          </h4>
          {subtitle && <p className="mt-1 text-[12px] leading-5 text-[var(--muted)]">{subtitle}</p>}
        </div>
        <div className="flex items-center gap-1 rounded-lg border border-[var(--line)] bg-white p-1 shadow-sm">
          <button
            type="button"
            onClick={() => void exportChart('png')}
            disabled={Boolean(error) || exporting !== null}
            className="visualization-action"
            title="Download high-resolution PNG"
            aria-label="Download chart as PNG"
          >
            <Download size={12} />
            <span>PNG</span>
          </button>
          <button
            type="button"
            onClick={() => void exportChart('svg')}
            disabled={Boolean(error) || exporting !== null}
            className="visualization-action hidden sm:inline-flex"
            title="Download editable SVG"
            aria-label="Download chart as SVG"
          >
            <Download size={12} />
            <span>SVG</span>
          </button>
          <span className="mx-0.5 h-5 w-px bg-[var(--line)]" />
          <button
            type="button"
            onClick={() => setExpanded((value) => !value)}
            className="visualization-icon-action"
            title={expanded ? 'Exit expanded view' : 'Expand visualization'}
            aria-label={expanded ? 'Exit expanded chart view' : 'Expand chart'}
          >
            {expanded ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
          </button>
        </div>
      </header>

      <div className={`min-h-0 px-3 pb-3 sm:px-4 ${expanded ? 'flex-1 overflow-auto' : ''}`}>
        {error ? (
          <div className="flex min-h-40 items-center justify-between gap-4 rounded-xl border border-red-100 bg-red-50/60 px-4 py-4 text-[12px] text-[var(--muted)]">
            <span>The data is available, but this visualization could not be rendered.</span>
            <button
              type="button"
              onClick={() => {
                setError(null);
                setRenderKey((key) => key + 1);
              }}
              className="rounded-lg border border-[var(--line)] bg-white px-3 py-1.5 font-medium text-[var(--ink)] hover:border-[var(--brand-red)]"
            >
              Retry
            </button>
          </div>
        ) : (
          <div className={`visualization-canvas ${expanded ? 'min-h-full' : ''}`}>
            <div ref={containerRef} role="img" aria-label={ariaLabel} className="w-full overflow-x-auto overflow-y-visible" />
          </div>
        )}
      </div>

      <footer className="flex flex-wrap items-center justify-between gap-2 border-t border-[var(--line-soft)] px-4 py-2.5 text-[10.5px] text-[var(--muted)] sm:px-5">
        <span>{count !== null ? `${count.toLocaleString()} plotted observations` : 'Evidence-backed visualization'}</span>
        <span className="text-[var(--muted-2)]">Hover for exact values · Downloads preserve the current view</span>
      </footer>
    </section>
  );
}
