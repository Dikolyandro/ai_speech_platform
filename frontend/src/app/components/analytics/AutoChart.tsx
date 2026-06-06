import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Download } from 'lucide-react';

import { Button } from '../ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { cn } from '../ui/utils';
import type { QueryAnswerChart } from '../../../lib/api';

export type AnswerChartSuggestion = QueryAnswerChart;

export type AutoChartProps = {
  chart: AnswerChartSuggestion | null | undefined;
  rows: unknown[];
  compact?: boolean;
};

const PIE_COLORS = [
  'rgba(167, 139, 250, 0.9)',
  'rgba(139, 92, 246, 0.9)',
  'rgba(196, 181, 253, 0.9)',
  'rgba(109, 40, 217, 0.85)',
  'rgba(216, 180, 254, 0.85)',
  'rgba(124, 58, 237, 0.85)',
  'rgba(233, 213, 255, 0.8)',
  'rgba(91, 33, 182, 0.85)',
];

const tooltipStyle = {
  background: 'rgba(15,15,20,0.96)',
  border: '1px solid rgba(255,255,255,0.14)',
  borderRadius: 8,
  color: 'rgba(255,255,255,0.92)',
  fontSize: 12,
  boxShadow: '0 12px 30px rgba(0,0,0,0.35)',
};

const tooltipLabelStyle = { color: 'rgba(255,255,255,0.86)' };
const tooltipItemStyle = { color: 'rgba(255,255,255,0.92)' };

function solidColor(color: string): string {
  return color.replace(/rgba\(([^,]+),([^,]+),([^,]+),[^)]+\)/, 'rgb($1,$2,$3)');
}

function downloadDataUrl(dataUrl: string, filename: string) {
  const a = document.createElement('a');
  a.href = dataUrl;
  a.download = filename.endsWith('.png') ? filename : `${filename}.png`;
  document.body.appendChild(a);
  a.click();
  a.remove();
}

function filenameFromTitle(title: string) {
  const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  return `${slug || 'chart'}.png`;
}

function drawText(
  ctx: CanvasRenderingContext2D,
  text: string,
  x: number,
  y: number,
  maxWidth: number,
  font: string,
  color = '#f4f0ff'
) {
  ctx.font = font;
  ctx.fillStyle = color;
  const ellipsis = '...';
  let out = text;
  while (ctx.measureText(out).width > maxWidth && out.length > ellipsis.length) {
    out = `${out.slice(0, -4)}${ellipsis}`;
  }
  ctx.fillText(out, x, y);
}

function drawRoundedRect(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, r: number) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

function downloadChartAsPng(
  chartType: string,
  title: string,
  data: { x: string; y: number }[],
) {
  const scale = 2;
  const width = 900;
  const height = 560;
  const canvas = document.createElement('canvas');
  canvas.width = width * scale;
  canvas.height = height * scale;
  const ctx = canvas.getContext('2d');
  if (!ctx) return;
  ctx.scale(scale, scale);

  ctx.fillStyle = '#0b0911';
  ctx.fillRect(0, 0, width, height);
  drawRoundedRect(ctx, 28, 28, width - 56, height - 56, 28);
  ctx.fillStyle = '#17151f';
  ctx.fill();
  ctx.strokeStyle = 'rgba(255,255,255,0.13)';
  ctx.lineWidth = 1.5;
  ctx.stroke();

  drawText(ctx, title, 56, 76, width - 112, '600 24px Inter, Arial, sans-serif');

  const values = data.map((d) => Math.max(0, d.y));
  const max = Math.max(...values, 1);

  if (chartType === 'pie') {
    const total = values.reduce((sum, value) => sum + value, 0) || 1;
    const cx = 330;
    const cy = 305;
    const outer = 145;
    const inner = 70;
    let start = -Math.PI / 2;
    data.forEach((item, i) => {
      const angle = (Math.max(0, item.y) / total) * Math.PI * 2;
      ctx.beginPath();
      ctx.moveTo(cx, cy);
      ctx.arc(cx, cy, outer, start, start + angle);
      ctx.closePath();
      ctx.fillStyle = solidColor(PIE_COLORS[i % PIE_COLORS.length]);
      ctx.fill();
      ctx.strokeStyle = '#0b0911';
      ctx.lineWidth = 4;
      ctx.stroke();
      start += angle;
    });
    ctx.globalCompositeOperation = 'destination-out';
    ctx.beginPath();
    ctx.arc(cx, cy, inner, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalCompositeOperation = 'source-over';

    data.slice(0, 10).forEach((item, i) => {
      const y = 165 + i * 34;
      ctx.fillStyle = solidColor(PIE_COLORS[i % PIE_COLORS.length]);
      ctx.beginPath();
      ctx.arc(570, y - 6, 7, 0, Math.PI * 2);
      ctx.fill();
      drawText(ctx, item.x, 590, y, 170, '500 16px Inter, Arial, sans-serif', '#eee9ff');
      drawText(ctx, String(item.y), 780, y, 70, '600 16px Inter, Arial, sans-serif', '#cfc6ee');
    });
  } else if (chartType === 'bar') {
    const left = 78;
    const top = 120;
    const chartW = 760;
    const chartH = 330;
    ctx.strokeStyle = 'rgba(255,255,255,0.12)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = top + (chartH / 4) * i;
      ctx.beginPath();
      ctx.moveTo(left, y);
      ctx.lineTo(left + chartW, y);
      ctx.stroke();
    }
    const gap = 12;
    const barW = Math.max(14, (chartW - gap * (data.length - 1)) / Math.max(data.length, 1));
    data.slice(0, 20).forEach((item, i) => {
      const h = (Math.max(0, item.y) / max) * (chartH - 20);
      const x = left + i * (barW + gap);
      const y = top + chartH - h;
      ctx.fillStyle = '#a78bfa';
      drawRoundedRect(ctx, x, y, barW, h, 6);
      ctx.fill();
      drawText(ctx, item.x, x, top + chartH + 28, Math.max(barW + 18, 48), '500 11px Inter, Arial, sans-serif', '#bdb6cf');
    });
  } else {
    const left = 78;
    const top = 120;
    const chartW = 760;
    const chartH = 330;
    ctx.strokeStyle = 'rgba(255,255,255,0.12)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = top + (chartH / 4) * i;
      ctx.beginPath();
      ctx.moveTo(left, y);
      ctx.lineTo(left + chartW, y);
      ctx.stroke();
    }
    const points = data.slice(0, 20).map((item, i, arr) => ({
      x: left + (arr.length <= 1 ? chartW / 2 : (chartW / (arr.length - 1)) * i),
      y: top + chartH - (Math.max(0, item.y) / max) * (chartH - 20),
      label: item.x,
    }));
    ctx.strokeStyle = '#c4b5fd';
    ctx.lineWidth = 4;
    ctx.beginPath();
    points.forEach((point, i) => (i ? ctx.lineTo(point.x, point.y) : ctx.moveTo(point.x, point.y)));
    ctx.stroke();
    points.forEach((point) => {
      ctx.fillStyle = '#a78bfa';
      ctx.beginPath();
      ctx.arc(point.x, point.y, 5, 0, Math.PI * 2);
      ctx.fill();
      drawText(ctx, point.label, point.x - 24, top + chartH + 28, 70, '500 11px Inter, Arial, sans-serif', '#bdb6cf');
    });
  }

  downloadDataUrl(canvas.toDataURL('image/png'), filenameFromTitle(title));
}

function PieLegend({
  data,
  compact,
}: {
  data: { x: string; y: number }[];
  compact: boolean;
}) {
  return (
    <div className={cn('min-w-0 shrink-0 space-y-1.5', compact ? 'w-[86px]' : 'w-[130px]')}>
      {data.slice(0, compact ? 4 : 8).map((item, i) => (
        <div key={`${item.x}-${i}`} className="flex min-w-0 items-center gap-1.5 text-[10px] leading-tight text-white/75">
          <span
            className="h-2 w-2 shrink-0 rounded-full"
            style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }}
          />
          <span className="min-w-0 flex-1 truncate">{item.x}</span>
          <span className="shrink-0 text-white/55">{item.y}</span>
        </div>
      ))}
    </div>
  );
}

function safeLabel(v: unknown): string {
  if (v == null) return '';
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
    return String(v);
  }
  try {
    return JSON.stringify(v);
  } catch {
    return '';
  }
}

function safeNumber(v: unknown): number {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = parseFloat(v.replace(/,/g, ''));
    return Number.isFinite(n) ? n : 0;
  }
  if (typeof v === 'boolean') return v ? 1 : 0;
  return 0;
}

function buildChartPoints(
  rows: unknown[],
  xKey: string,
  yKey: string,
  maxPoints: number,
): { label: string; value: number }[] {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const out: { label: string; value: number }[] = [];
  for (let i = 0; i < rows.length && out.length < maxPoints; i++) {
    const row = rows[i];
    if (!row || typeof row !== 'object') continue;
    const rec = row as Record<string, unknown>;
    if (!(xKey in rec) || !(yKey in rec)) continue;
    const label = safeLabel(rec[xKey]).trim() || `(${i})`;
    const value = safeNumber(rec[yKey]);
    out.push({ label, value });
  }
  return out;
}

/**
 * Renders a small Recharts visualization from ``interpreted.chart`` + ``rows``.
 * Never throws; returns null when chart should not be shown.
 */
export function AutoChart({ chart, rows, compact = false }: AutoChartProps) {
  try {
    if (!chart || chart.enabled === false) return null;
    const ct = chart.chart_type;
    if (!ct || ct === 'table') return null;
    if (!chart.x || !chart.y) return null;
    if (!Array.isArray(rows) || rows.length === 0) return null;

    const xKey = String(chart.x);
    const yKey = String(chart.y);
    const points = buildChartPoints(rows, xKey, yKey, 20);
    if (points.length === 0) return null;

    const data = points.map((p) => ({ x: p.label, y: p.value }));

    const title = chart.title?.trim() || 'Chart';

    const axisTick = { fill: 'rgba(255,255,255,0.55)', fontSize: 10 };
    const gridStroke = 'rgba(255,255,255,0.08)';

    return (
      <Card
        className={cn(
          compact ? 'mt-0' : 'mt-3',
          'border border-white/10 bg-white/[0.04] text-white/90 shadow-none',
          'rounded-xl overflow-hidden',
        )}
      >
        <CardHeader className={cn('flex flex-row items-center justify-between gap-2 space-y-0 px-3 pb-0', compact ? 'py-1.5' : 'py-2')}>
          <CardTitle className="text-xs font-medium text-white/80 tracking-tight truncate">{title}</CardTitle>
          <Button
            type="button"
            size="icon"
            variant="ghost"
            className={cn(
              'shrink-0 rounded-md border border-violet-400/30 bg-violet-500/10 text-violet-200 hover:bg-violet-500/20 hover:text-white',
              compact ? 'h-6 w-6' : 'h-7 w-7'
            )}
            title="Download PNG"
            aria-label="Download PNG"
            onClick={() => downloadChartAsPng(ct, title, data)}
          >
            <Download className={compact ? 'h-3 w-3' : 'h-3.5 w-3.5'} />
          </Button>
        </CardHeader>
        <CardContent className={cn('px-2 pb-2 pt-0', compact ? 'h-[170px]' : 'h-[280px]')}>
          {ct === 'pie' ? (
            <div className="flex h-full items-center gap-2">
              <div className="h-full min-w-0 flex-1">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Tooltip
                      contentStyle={tooltipStyle}
                      labelStyle={tooltipLabelStyle}
                      itemStyle={tooltipItemStyle}
                    />
                    <Pie
                      data={data}
                      dataKey="y"
                      nameKey="x"
                      cx="50%"
                      cy="50%"
                      innerRadius={compact ? 26 : 44}
                      outerRadius={compact ? 54 : 92}
                      paddingAngle={2}
                    >
                      {data.map((_, i) => (
                        <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} stroke="rgba(0,0,0,0.2)" />
                      ))}
                    </Pie>
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <PieLegend data={data} compact={compact} />
            </div>
          ) : (
          <ResponsiveContainer width="100%" height="100%">
            {ct === 'bar' ? (
              <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
                <XAxis dataKey="x" tick={axisTick} interval={0} angle={-28} textAnchor="end" height={56} />
                <YAxis tick={axisTick} width={36} />
                <Tooltip
                  contentStyle={tooltipStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                />
                <Bar dataKey="y" fill="rgba(167, 139, 250, 0.85)" radius={[4, 4, 0, 0]} maxBarSize={48} />
              </BarChart>
            ) : ct === 'line' ? (
              <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
                <XAxis dataKey="x" tick={axisTick} interval={0} angle={-20} textAnchor="end" height={52} />
                <YAxis tick={axisTick} width={36} />
                <Tooltip
                  contentStyle={tooltipStyle}
                  labelStyle={tooltipLabelStyle}
                  itemStyle={tooltipItemStyle}
                />
                <Line
                  type="monotone"
                  dataKey="y"
                  stroke="rgba(196, 181, 253, 0.95)"
                  strokeWidth={2}
                  dot={{ r: 3, fill: 'rgba(167, 139, 250, 0.95)' }}
                />
              </LineChart>
            ) : null}
          </ResponsiveContainer>
          )}
        </CardContent>
      </Card>
    );
  } catch {
    return null;
  }
}
