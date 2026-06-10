import { Download } from 'lucide-react';
import { Button } from './ui/button';
import { useI18n } from '../i18n/context';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from './ui/table';

type ResultTableProps = {
  rows: Record<string, unknown>[];
  columns?: string[];
  title?: string;
  maxRows?: number;
  filename?: string;
  className?: string;
  compact?: boolean;
  showDownload?: boolean;
};

function csvCell(value: unknown): string {
  if (value == null) return '';
  const raw = typeof value === 'object' ? JSON.stringify(value) : String(value);
  return /[",\n\r]/.test(raw) ? `"${raw.replace(/"/g, '""')}"` : raw;
}

function downloadRowsAsCsv(rows: Record<string, unknown>[], columns: string[], filename: string) {
  const body = [columns.join(','), ...rows.map((row) => columns.map((c) => csvCell(row[c])).join(','))].join('\n');
  const blob = new Blob([body], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename.endsWith('.csv') ? filename : `${filename}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function ResultTable({
  rows,
  columns,
  title,
  maxRows,
  filename = 'analytics-result.csv',
  className = '',
  compact = false,
  showDownload = true,
}: ResultTableProps) {
  const { t } = useI18n();
  if (!rows.length) return null;
  const cols = columns?.length ? columns : Array.from(new Set(rows.flatMap((r) => Object.keys(r ?? {}))));
  if (!cols.length) return null;
  const visibleRows = maxRows ? rows.slice(0, maxRows) : rows;
  const headClassName = compact ? 'h-6 px-2 py-1 text-[10px]' : undefined;
  const cellClassName = compact ? 'px-2 py-1 text-[10px]' : undefined;

  return (
    <div className={className}>
      <div className={`border border-white/10 bg-white/[0.03] ${compact ? 'rounded-lg p-1.5' : 'rounded-xl p-3'}`}>
        {title ? <div className="mb-2 text-xs text-white/50">{title}</div> : null}
        <div className={compact ? 'overflow-auto rounded-md' : 'overflow-auto rounded-lg'}>
          <Table className={compact ? 'text-[10px]' : 'text-xs'}>
            <TableHeader>
              <TableRow>
                {cols.map((c) => (
                  <TableHead key={c} className={headClassName}>{c}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {visibleRows.map((r, idx) => (
                <TableRow key={idx}>
                  {cols.map((c) => (
                    <TableCell key={c} className={cellClassName}>{r?.[c] == null ? '' : String(r[c])}</TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </div>
      {showDownload && (
        <div className={compact ? 'mt-2 flex justify-end' : 'mt-3 flex justify-end'}>
          <Button
            type="button"
            size="icon"
            variant="ghost"
            className={`${compact ? 'h-6 w-6 rounded-md' : 'h-8 w-8 rounded-lg'} border border-violet-400/35 bg-violet-500/10 text-violet-200 shadow-[0_0_18px_rgba(139,92,246,0.18)] hover:bg-violet-500/20 hover:text-white`}
            onClick={() => downloadRowsAsCsv(rows, cols, filename)}
            title={t('table.downloadCsv')}
            aria-label={t('table.downloadCsv')}
          >
            <Download className={compact ? 'h-3 w-3' : 'h-4 w-4'} />
          </Button>
        </div>
      )}
    </div>
  );
}
