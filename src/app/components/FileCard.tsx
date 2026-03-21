import { FileText, Download } from 'lucide-react';

interface FileCardProps {
  filename: string;
  size: string;
  type?: 'upload' | 'download';
}

export function FileCard({ filename, size, type = 'upload' }: FileCardProps) {
  const extension = filename.split('.').pop()?.toUpperCase() || 'FILE';

  return (
    <div className="flex items-center gap-3 p-4 rounded-2xl bg-card border border-border max-w-sm">
      <div className="w-12 h-12 rounded-xl bg-primary/10 border border-primary/20 flex items-center justify-center">
        <FileText className="w-6 h-6 text-primary" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm text-white truncate">{filename}</p>
        <p className="text-xs text-white/60">
          {extension} • {size}
        </p>
      </div>
      {type === 'download' && (
        <button className="w-8 h-8 rounded-lg bg-primary/10 hover:bg-primary/20 flex items-center justify-center transition-all">
          <Download className="w-4 h-4 text-primary" />
        </button>
      )}
    </div>
  );
}
