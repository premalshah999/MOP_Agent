import { ArrowUpRight, Layers3, Map } from 'lucide-react';

interface MapButtonProps {
  onClick: () => void;
  label?: string;
  title?: string;
  subtitle?: string;
  metricLabel?: string;
  geographyCount?: number;
}

const MAP_RAMP = ['#f8e8e8', '#f4b9ba', '#eb7d80', '#e03a3e', '#8f1f28'];

export function MapButton({
  onClick,
  label = 'Explore map',
  title,
  subtitle,
  metricLabel,
  geographyCount,
}: MapButtonProps) {
  const detail = subtitle || (metricLabel ? `Explore ${metricLabel.toLowerCase()} by geography` : 'Inspect the geographic pattern and exact regional values');

  return (
    <button type="button" onClick={onClick} className="map-preview-card group" aria-label={label}>
      <div className="map-preview-graphic" aria-hidden="true">
        <div className="map-preview-grid" />
        <span className="map-preview-pin map-preview-pin-one" />
        <span className="map-preview-pin map-preview-pin-two" />
        <div className="map-preview-ramp">
          {MAP_RAMP.map((color) => <span key={color} style={{ background: color }} />)}
        </div>
      </div>
      <div className="relative flex min-w-0 flex-1 items-center gap-3 p-4 sm:p-5">
        <span className="map-preview-icon"><Map size={17} strokeWidth={1.8} /></span>
        <span className="min-w-0 flex-1 text-left">
          <span className="mop-kicker block">Geographic analysis</span>
          <span className="mt-1 block font-display text-[17px] font-medium leading-6 text-[var(--ink)] sm:text-[19px]">
            {title || label}
          </span>
          <span className="mt-0.5 block text-[11.5px] leading-5 text-[var(--muted)]">{detail}</span>
        </span>
        <span className="hidden shrink-0 items-center gap-1.5 text-[10px] font-bold uppercase tracking-[0.12em] text-[var(--ink-soft)] sm:flex">
          {geographyCount ? <><Layers3 size={12} /> {geographyCount} regions</> : label}
          <ArrowUpRight size={14} className="transition-transform group-hover:-translate-y-0.5 group-hover:translate-x-0.5" />
        </span>
      </div>
    </button>
  );
}
