import { Expand, Map } from 'lucide-react';


interface MapButtonProps {
  onClick: () => void;
  label?: string;
}

export function MapButton({ onClick, label = 'Open map view' }: MapButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="mop-outline-button inline-flex items-center gap-1.5 px-2.5 py-1.5"
      aria-label={label}
    >
      <Map size={11} />
      {label}
      <Expand size={10} />
    </button>
  );
}
