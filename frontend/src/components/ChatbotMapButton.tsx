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
      className="inline-flex items-center gap-1.5 rounded-[6px] border border-black/6 bg-white px-2.5 py-1.5 text-[10px] font-medium text-[var(--muted)] transition hover:border-[var(--accent)]/30 hover:text-[var(--ink)]"
      aria-label={label}
    >
      <Map size={11} />
      {label}
      <Expand size={10} />
    </button>
  );
}
