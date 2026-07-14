import { Expand, Map } from 'lucide-react';


interface ChatbotMapButtonProps {
  onClick: () => void;
  label?: string;
}

export function ChatbotMapButton({ onClick, label = 'Open map view' }: ChatbotMapButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="inline-flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-[11px] font-medium text-[var(--muted)] transition hover:bg-[var(--surface-2)] hover:text-[var(--ink)]"
      aria-label={label}
    >
      <Map size={11} />
      {label}
      <Expand size={10} />
    </button>
  );
}
