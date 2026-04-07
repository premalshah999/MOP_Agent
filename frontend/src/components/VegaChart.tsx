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
              labelFontSize: 10,
              titleFontSize: 11,
              labelFont: 'inherit',
              titleFont: 'inherit',
            },
            bar: { color: '#1a1a1a' },
            line: { color: '#1a1a1a', strokeWidth: 2 },
            point: { color: '#1a1a1a', size: 40 },
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
      className="mt-3 w-full overflow-hidden rounded-lg border border-[var(--line)] bg-white p-3"
    />
  );
}
