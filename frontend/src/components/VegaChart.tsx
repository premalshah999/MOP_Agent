import { useEffect, useRef } from 'react';
import embed from 'vega-embed';

interface VegaChartProps {
  spec: Record<string, unknown>;
}

export function VegaChart({ spec }: VegaChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current || !spec) return;

    let disposed = false;

    const renderChart = async () => {
      if (disposed || !containerRef.current) return;
      try {
        const result = await embed(containerRef.current, spec as never, {
          actions: false,
          renderer: 'svg',
          theme: 'quartz',
          config: {
            background: 'transparent',
            axis: {
              domain: false,
              tickColor: '#e2e8f0',
              gridColor: '#eef2f7',
              labelColor: '#64748b',
              titleColor: '#0f172a',
              labelFontSize: 10,
              titleFontSize: 11,
              labelFont: 'Inter',
              titleFont: 'Inter',
            },
            view: { stroke: null },
            style: { 'guide-label': { font: 'Inter' }, 'guide-title': { font: 'Inter' } },
            bar: { color: '#1f3b82', cornerRadiusEnd: 0 },
            line: { color: '#1f3b82', strokeWidth: 2 },
            point: { color: '#1f3b82', size: 34 },
            rect: { cornerRadius: 0 },
          },
        });

        // Cleanup on unmount
        return () => {
          result.finalize();
        };
      } catch (err) {
        console.warn('[VegaChart] Render failed:', err);
      }
    };

    void renderChart();

    return () => {
      disposed = true;
    };
  }, [spec]);

  return (
    <div
      ref={containerRef}
      className="w-full overflow-hidden border border-[var(--line)] bg-[var(--surface)] p-4"
    />
  );
}
