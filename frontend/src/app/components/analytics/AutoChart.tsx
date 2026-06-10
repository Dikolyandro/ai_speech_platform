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
import { useI18n } from '../../i18n/context';

export type AnswerChartSuggestion = QueryAnswerChart;

export type AutoChartProps = {
  chart: AnswerChartSuggestion | null | undefined;
  rows: unknown[];
  compact?: boolean;
};

type VisualType = 'vertical-bar' | 'horizontal-bar' | 'donut' | 'line' | 'histogram';

type ChartPoint = {
  x: string;
  y: number;
  fullLabel: string;
};

type PreparedChart = {
  type: VisualType;
  title: string;
  data: ChartPoint[];
  xKey: string;
  yKey: string;
  recordCount: number;
  groupedRemainder: number;
  originalCategoryCount: number;
  longestLabelLength: number;
};

const CHART_COLORS = [
  '#8B7CFF',
  '#22C55E',
  '#60A5FA',
  '#F59E0B',
  '#EC4899',
  '#14B8A6',
  '#A78BFA',
  '#F87171',
  '#38BDF8',
  '#C084FC',
  '#34D399',
  '#FBBF24',
  '#818CF8',
  '#FB7185',
  '#2DD4BF',
  '#94A3B8',
];

const tooltipStyle = {
  background: 'rgba(255,255,255,0.98)',
  border: '1px solid rgba(226,221,247,0.95)',
  borderRadius: 12,
  color: '#111827',
  fontSize: 12,
  boxShadow: '0 18px 46px rgba(17,24,39,0.14)',
};

const tooltipLabelStyle = { color: '#111827', fontWeight: 600 };
const tooltipItemStyle = { color: '#374151' };
const axisTick = { fill: 'rgba(107,114,128,0.95)', fontSize: 11 };
const compactAxisTick = { fill: 'rgba(107,114,128,0.95)', fontSize: 9 };
const gridStroke = 'rgba(226,221,247,0.72)';

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

function titleCase(value: string) {
  return value
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .trim()
    .replace(/\w\S*/g, (word) => {
      const upper = word.toUpperCase();
      if (['ID', 'URL', 'SQL', 'CSV'].includes(upper)) return upper;
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
    });
}

function truncateLabel(value: string, max = 22) {
  if (value.length <= max) return value;
  return `${value.slice(0, Math.max(0, max - 3)).trimEnd()}...`;
}

function safeLabel(value: unknown): string {
  if (value == null) return '';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return '';
  }
}

function safeNumber(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const n = parseFloat(value.replace(/,/g, ''));
    return Number.isFinite(n) ? n : 0;
  }
  if (typeof value === 'boolean') return value ? 1 : 0;
  return 0;
}

function isBooleanLabel(value: string) {
  return /^(true|false|yes|no|0|1|verified|non-verified|non verified)$/i.test(value.trim());
}

function isDateLabel(value: string) {
  const trimmed = value.trim();
  if (/^\d{4}(-\d{1,2})?(-\d{1,2})?$/.test(trimmed)) return true;
  if (/^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(trimmed)) return true;
  const parsed = Date.parse(trimmed);
  return Number.isFinite(parsed) && /[a-zA-Z]|[-/]/.test(trimmed);
}

function isNumericLabel(value: string) {
  if (!value.trim()) return false;
  return Number.isFinite(Number(value.replace(/,/g, '')));
}

function isPercentageChart(chart: QueryAnswerChart, points: ChartPoint[]) {
  const haystack = `${chart.title ?? ''} ${chart.reason ?? ''} ${chart.y ?? ''}`.toLowerCase();
  if (/percent|percentage|share|ratio|rate/.test(haystack)) return true;
  const total = points.reduce((sum, point) => sum + Math.max(0, point.y), 0);
  return points.length > 1 && points.length <= 8 && total > 98 && total < 102;
}

function buildRawPoints(rows: unknown[], xKey: string, yKey: string): ChartPoint[] {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const points: ChartPoint[] = [];
  rows.forEach((row, index) => {
    if (!row || typeof row !== 'object') return;
    const rec = row as Record<string, unknown>;
    if (!(xKey in rec) || !(yKey in rec)) return;
    const fullLabel = safeLabel(rec[xKey]).trim() || `(${index + 1})`;
    points.push({ x: fullLabel, fullLabel, y: safeNumber(rec[yKey]) });
  });
  return points;
}

function aggregateLongTail(points: ChartPoint[], compact: boolean) {
  if (points.length <= 20) return { data: points, groupedRemainder: 0 };

  const limit = compact ? 8 : 15;
  const sorted = [...points].sort((a, b) => Math.abs(b.y) - Math.abs(a.y));
  const top = sorted.slice(0, limit);
  const rest = sorted.slice(limit);
  const otherValue = rest.reduce((sum, item) => sum + item.y, 0);
  return {
    data: [
      ...top,
      {
        x: 'Other',
        fullLabel: `Other (${rest.length} categories)`,
        y: otherValue,
      },
    ],
    groupedRemainder: rest.length,
  };
}

function buildHistogram(points: ChartPoint[], compact: boolean): ChartPoint[] {
  const values = points.map((point) => Number(point.fullLabel.replace(/,/g, ''))).filter(Number.isFinite);
  if (values.length < 6) return points;
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min === max) return [{ x: String(min), fullLabel: String(min), y: points.length }];
  const binCount = compact ? 6 : Math.min(12, Math.max(6, Math.ceil(Math.sqrt(values.length))));
  const step = (max - min) / binCount;
  const bins = Array.from({ length: binCount }, (_, i) => {
    const start = min + i * step;
    const end = i === binCount - 1 ? max : start + step;
    const label = `${formatBin(start)}-${formatBin(end)}`;
    return { x: label, fullLabel: label, y: 0 };
  });
  values.forEach((value) => {
    const idx = Math.min(binCount - 1, Math.max(0, Math.floor((value - min) / step)));
    bins[idx].y += 1;
  });
  return bins;
}

function formatBin(value: number) {
  if (Math.abs(value) >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return value.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function inferTitle(chart: QueryAnswerChart, type: VisualType, xKey: string, yKey: string, t: (key: string, vars?: Record<string, string | number>) => string) {
  const provided = chart.title?.trim();
  if (provided && !/^grouped results$/i.test(provided) && !/^chart$/i.test(provided)) return provided;

  const x = titleCase(xKey);
  const y = titleCase(yKey);
  if (type === 'line') return t('chart.by', { y, x });
  if (type === 'histogram') return t('chart.distributionOf', { column: x });
  if (type === 'donut') return t('chart.breakdown', { column: x });
  if (/count|row|records?/i.test(yKey)) return t('chart.byRecordCount', { column: x });
  return t('chart.by', { y, x });
}

function inferChartType(chart: QueryAnswerChart, points: ChartPoint[], compact: boolean): VisualType {
  const requested = chart.chart_type;
  const labels = points.map((point) => point.fullLabel);
  const categoryCount = points.length;
  const longestLabel = Math.max(...labels.map((label) => label.length), 0);
  const lowerText = `${chart.title ?? ''} ${chart.reason ?? ''} ${chart.x ?? ''} ${chart.y ?? ''}`.toLowerCase();
  const booleanLike = categoryCount > 1 && categoryCount <= 4 && labels.every(isBooleanLabel);
  const dateLike = labels.length > 1 && labels.filter(isDateLabel).length / labels.length >= 0.7;
  const numericLike = labels.length > 6 && labels.filter(isNumericLabel).length / labels.length >= 0.85;

  if (dateLike || requested === 'line') return 'line';
  if (booleanLike || requested === 'pie' || isPercentageChart(chart, points)) return 'donut';
  if (/histogram|distribution/.test(lowerText) || numericLike) return 'histogram';
  if (categoryCount > 10 || longestLabel > 18 || compact) return 'horizontal-bar';
  return 'vertical-bar';
}

function prepareChart(
  chart: QueryAnswerChart,
  rows: unknown[],
  compact: boolean,
  t: (key: string, vars?: Record<string, string | number>) => string,
): PreparedChart | null {
  if (!chart.x || !chart.y || !Array.isArray(rows) || rows.length === 0) return null;
  const xKey = String(chart.x);
  const yKey = String(chart.y);
  const raw = buildRawPoints(rows, xKey, yKey);
  if (!raw.length) return null;

  const originalCategoryCount = raw.length;
  const longestLabelLength = Math.max(...raw.map((point) => point.fullLabel.length), 0);
  let type = inferChartType(chart, raw, compact);
  let data = raw;
  let groupedRemainder = 0;

  if (type === 'histogram') {
    data = buildHistogram(raw, compact);
  } else if (type === 'line') {
    data = [...raw].sort((a, b) => {
      const ad = Date.parse(a.fullLabel);
      const bd = Date.parse(b.fullLabel);
      if (Number.isFinite(ad) && Number.isFinite(bd)) return ad - bd;
      const an = Number(a.fullLabel.replace(/,/g, ''));
      const bn = Number(b.fullLabel.replace(/,/g, ''));
      if (Number.isFinite(an) && Number.isFinite(bn)) return an - bn;
      return a.fullLabel.localeCompare(b.fullLabel);
    });
  } else if (type === 'donut') {
    const aggregated = aggregateLongTail(raw, compact);
    data = aggregated.data;
    groupedRemainder = aggregated.groupedRemainder;
  } else {
    const aggregated = aggregateLongTail(raw, compact);
    data = aggregated.data;
    groupedRemainder = aggregated.groupedRemainder;
    if (originalCategoryCount > 10 || longestLabelLength > 18) type = 'horizontal-bar';
  }

  data = data.map((point) => ({
    ...point,
    x: truncateLabel(point.fullLabel, type === 'horizontal-bar' ? 28 : compact ? 10 : 16),
  }));

  return {
    type,
    title: inferTitle(chart, type, xKey, yKey, t),
    data,
    xKey,
    yKey,
    recordCount: rows.length,
    groupedRemainder,
    originalCategoryCount,
    longestLabelLength,
  };
}

function metadataText(prepared: PreparedChart, t: (key: string, vars?: Record<string, string | number>) => string) {
  const typeLabel =
    prepared.type === 'vertical-bar'
      ? t('chart.bar')
      : prepared.type === 'horizontal-bar'
        ? t('chart.horizontalBar')
        : prepared.type === 'donut'
          ? t('chart.donut')
          : prepared.type === 'histogram'
            ? t('chart.histogram')
            : t('chart.line');
  const metric = prepared.type === 'histogram' ? t('chart.frequency') : titleCase(prepared.yKey);
  return t('chart.groupedByMeta', {
    type: typeLabel,
    x: titleCase(prepared.xKey),
    metric,
    count: prepared.recordCount.toLocaleString(),
  });
}
function drawText(
  ctx: CanvasRenderingContext2D,
  text: string,
  x: number,
  y: number,
  maxWidth: number,
  font: string,
  color = '#111827',
  align: CanvasTextAlign = 'left',
) {
  ctx.font = font;
  ctx.fillStyle = color;
  ctx.textAlign = align;
  const ellipsis = '...';
  let out = text;
  while (ctx.measureText(out).width > maxWidth && out.length > ellipsis.length) {
    out = `${out.slice(0, -4)}${ellipsis}`;
  }
  ctx.fillText(out, x, y);
  ctx.textAlign = 'left';
}

function drawRoundedRect(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, r: number) {
  const radius = Math.min(r, Math.abs(w) / 2, Math.abs(h) / 2);
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + w, y, x + w, y + h, radius);
  ctx.arcTo(x + w, y + h, x, y + h, radius);
  ctx.arcTo(x, y + h, x, y, radius);
  ctx.arcTo(x, y, x + w, y, radius);
  ctx.closePath();
}

function drawGrid(ctx: CanvasRenderingContext2D, left: number, top: number, width: number, height: number) {
  ctx.strokeStyle = '#E9E7F5';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 5; i++) {
    const y = top + (height / 5) * i;
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(left + width, y);
    ctx.stroke();
  }
}

function downloadChartAsPng(prepared: PreparedChart, t: (key: string, vars?: Record<string, string | number>) => string) {
  const scale = 2;
  const width = 1600;
  const height = 900;
  const canvas = document.createElement('canvas');
  canvas.width = width * scale;
  canvas.height = height * scale;
  const ctx = canvas.getContext('2d');
  if (!ctx) return;
  ctx.scale(scale, scale);

  ctx.fillStyle = '#FFFFFF';
  ctx.fillRect(0, 0, width, height);
  const bg = ctx.createLinearGradient(0, 0, width, height);
  bg.addColorStop(0, '#FFFFFF');
  bg.addColorStop(1, '#F7F5FF');
  ctx.fillStyle = bg;
  drawRoundedRect(ctx, 48, 44, width - 96, height - 88, 32);
  ctx.fill();
  ctx.strokeStyle = '#E2DDF7';
  ctx.lineWidth = 2;
  ctx.stroke();

  drawText(ctx, prepared.title, 84, 102, width - 168, '700 34px Inter, Arial, sans-serif');
  drawText(ctx, metadataText(prepared, t), 84, 142, width - 168, '500 17px Inter, Arial, sans-serif', '#6B7280');

  const data = prepared.data;
  const max = Math.max(...data.map((d) => Math.max(0, d.y)), 1);
  const left = prepared.type === 'horizontal-bar' ? 360 : 132;
  const top = 190;
  const chartW = width - left - 120;
  const chartH = 560;

  if (prepared.type === 'donut') {
    const total = data.reduce((sum, value) => sum + Math.max(0, value.y), 0) || 1;
    const cx = 500;
    const cy = 475;
    const outer = 220;
    const inner = 112;
    let start = -Math.PI / 2;
    data.forEach((item, i) => {
      const angle = (Math.max(0, item.y) / total) * Math.PI * 2;
      ctx.beginPath();
      ctx.arc(cx, cy, outer, start, start + angle);
      ctx.arc(cx, cy, inner, start + angle, start, true);
      ctx.closePath();
      ctx.fillStyle = CHART_COLORS[i % CHART_COLORS.length];
      ctx.fill();
      ctx.strokeStyle = '#FFFFFF';
      ctx.lineWidth = 6;
      ctx.stroke();
      start += angle;
    });
    drawText(ctx, total.toLocaleString(), cx, cy - 8, 260, '800 34px Inter, Arial, sans-serif', '#111827', 'center');
    drawText(ctx, 'total', cx, cy + 28, 260, '500 18px Inter, Arial, sans-serif', '#6B7280', 'center');
    data.slice(0, 12).forEach((item, i) => {
      const y = 265 + i * 42;
      ctx.fillStyle = CHART_COLORS[i % CHART_COLORS.length];
      ctx.beginPath();
      ctx.arc(900, y - 7, 9, 0, Math.PI * 2);
      ctx.fill();
      drawText(ctx, item.fullLabel, 925, y, 340, '600 18px Inter, Arial, sans-serif');
      drawText(ctx, item.y.toLocaleString(), 1320, y, 140, '700 18px Inter, Arial, sans-serif', '#374151', 'right');
    });
  } else if (prepared.type === 'horizontal-bar') {
    ctx.strokeStyle = '#E9E7F5';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const x = left + (chartW / 4) * i;
      ctx.beginPath();
      ctx.moveTo(x, top);
      ctx.lineTo(x, top + chartH);
      ctx.stroke();
    }
    const rowH = Math.min(42, chartH / Math.max(data.length, 1));
    data.forEach((item, i) => {
      const y = top + i * rowH + 7;
      const w = (Math.max(0, item.y) / max) * chartW;
      const grad = ctx.createLinearGradient(left, y, left + w, y);
      grad.addColorStop(0, '#B7AEFF');
      grad.addColorStop(1, '#8B7CFF');
      ctx.fillStyle = grad;
      drawRoundedRect(ctx, left, y, w, Math.max(10, rowH - 14), 8);
      ctx.fill();
      drawText(ctx, item.fullLabel, 84, y + rowH / 2 + 2, left - 112, '600 16px Inter, Arial, sans-serif', '#374151');
      drawText(ctx, item.y.toLocaleString(), left + w + 12, y + rowH / 2 + 2, 110, '600 15px Inter, Arial, sans-serif', '#6B7280');
    });
  } else {
    drawGrid(ctx, left, top, chartW, chartH);
    if (prepared.type === 'line') {
      const points = data.map((item, i, arr) => ({
        x: left + (arr.length <= 1 ? chartW / 2 : (chartW / (arr.length - 1)) * i),
        y: top + chartH - (Math.max(0, item.y) / max) * (chartH - 16),
        label: item.fullLabel,
      }));
      ctx.strokeStyle = '#8B7CFF';
      ctx.lineWidth = 5;
      ctx.beginPath();
      points.forEach((point, i) => (i ? ctx.lineTo(point.x, point.y) : ctx.moveTo(point.x, point.y)));
      ctx.stroke();
      points.forEach((point, i) => {
        ctx.fillStyle = '#FFFFFF';
        ctx.beginPath();
        ctx.arc(point.x, point.y, 8, 0, Math.PI * 2);
        ctx.fill();
        ctx.strokeStyle = CHART_COLORS[i % CHART_COLORS.length];
        ctx.lineWidth = 4;
        ctx.stroke();
      });
      points.forEach((point, i) => {
        if (i % Math.ceil(points.length / 10) === 0) {
          drawText(ctx, point.label, point.x - 45, top + chartH + 34, 90, '500 13px Inter, Arial, sans-serif', '#6B7280');
        }
      });
    } else {
      const gap = Math.max(10, Math.min(22, chartW / Math.max(data.length, 1) * 0.18));
      const barW = Math.max(20, (chartW - gap * (data.length - 1)) / Math.max(data.length, 1));
      data.forEach((item, i) => {
        const h = (Math.max(0, item.y) / max) * (chartH - 18);
        const x = left + i * (barW + gap);
        const y = top + chartH - h;
        const grad = ctx.createLinearGradient(x, y, x, y + h);
        grad.addColorStop(0, '#8B7CFF');
        grad.addColorStop(1, '#B7AEFF');
        ctx.fillStyle = grad;
        drawRoundedRect(ctx, x, y, barW, h, 10);
        ctx.fill();
        drawText(ctx, item.fullLabel, x - 8, top + chartH + 34, barW + 22, '500 13px Inter, Arial, sans-serif', '#6B7280');
      });
    }
  }

  drawText(ctx, prepared.groupedRemainder ? t('chart.topCategories', { count: prepared.groupedRemainder }) : t('chart.sourceResult'), 84, 820, width - 168, '500 16px Inter, Arial, sans-serif', '#6B7280');
  downloadDataUrl(canvas.toDataURL('image/png'), filenameFromTitle(prepared.title));
}

function CustomTooltip({ active, payload, label }: any) {
  const { t } = useI18n();
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload as ChartPoint | undefined;
  const value = payload[0]?.value;
  return (
    <div style={tooltipStyle} className="max-w-[280px] px-3 py-2">
      <div style={tooltipLabelStyle} className="break-words text-xs">
        {row?.fullLabel ?? label}
      </div>
      <div style={tooltipItemStyle} className="mt-1 text-xs">
        {t('chart.value')}: {Number(value ?? 0).toLocaleString()}
      </div>
    </div>
  );
}

function PieLegend({ data, compact }: { data: ChartPoint[]; compact: boolean }) {
  return (
    <div className={cn('min-w-0 shrink-0 space-y-1.5', compact ? 'w-[92px]' : 'w-[150px]')}>
      {data.slice(0, compact ? 5 : 10).map((item, i) => (
        <div
          key={`${item.fullLabel}-${i}`}
          className="flex min-w-0 items-center gap-1.5 text-[10px] leading-tight text-white/75"
          title={item.fullLabel}
        >
          <span
            className="h-2 w-2 shrink-0 rounded-full"
            style={{ backgroundColor: CHART_COLORS[i % CHART_COLORS.length] }}
          />
          <span className="min-w-0 flex-1 truncate">{item.x}</span>
          <span className="shrink-0 text-white/55">{item.y.toLocaleString()}</span>
        </div>
      ))}
    </div>
  );
}

/**
 * Renders an adaptive Recharts visualization from ``interpreted.chart`` + ``rows``.
 * All preparation is frontend-only, so backend analytics behavior is preserved.
 */
export function AutoChart({ chart, rows, compact = false }: AutoChartProps) {
  const { t } = useI18n();
  try {
    if (!chart || chart.enabled === false || chart.chart_type === 'table') return null;
    const prepared = prepareChart(chart, rows, compact, t);
    if (!prepared || prepared.data.length === 0) return null;
    const heightClass = compact ? 'h-[190px]' : prepared.type === 'horizontal-bar' ? 'h-[360px]' : 'h-[300px]';
    const tick = compact ? compactAxisTick : axisTick;

    return (
      <Card
        className={cn(
          compact ? 'mt-0' : 'mt-3',
          'border border-white/10 bg-white/[0.04] text-white/90 shadow-none',
          'rounded-xl overflow-hidden',
        )}
      >
        <CardHeader className={cn('flex flex-row items-start justify-between gap-2 space-y-0 px-3 pb-0', compact ? 'py-1.5' : 'py-2.5')}>
          <div className="min-w-0">
            <CardTitle className="truncate text-xs font-medium tracking-tight text-white/85" title={prepared.title}>
              {prepared.title}
            </CardTitle>
            {!compact && prepared.groupedRemainder > 0 && (
              <p className="mt-1 text-[10px] text-white/45">
                {t('chart.topCategories', { count: prepared.groupedRemainder })}
              </p>
            )}
          </div>
          <Button
            type="button"
            size="icon"
            variant="ghost"
            className={cn(
              'shrink-0 rounded-md border border-violet-400/30 bg-violet-500/10 text-violet-200 hover:bg-violet-500/20 hover:text-white',
              compact ? 'h-6 w-6' : 'h-7 w-7'
            )}
            title={t('chart.downloadPng')}
            aria-label={t('chart.downloadPng')}
            onClick={() => downloadChartAsPng(prepared, t)}
          >
            <Download className={compact ? 'h-3 w-3' : 'h-3.5 w-3.5'} />
          </Button>
        </CardHeader>
        <CardContent className={cn('px-2 pb-2 pt-1', heightClass)}>
          {prepared.type === 'donut' ? (
            <div className="flex h-full items-center gap-2">
              <div className="h-full min-w-0 flex-1">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Tooltip content={<CustomTooltip />} />
                    <Pie
                      data={prepared.data}
                      dataKey="y"
                      nameKey="x"
                      cx="50%"
                      cy="50%"
                      innerRadius={compact ? 30 : 54}
                      outerRadius={compact ? 58 : 100}
                      paddingAngle={2}
                    >
                      {prepared.data.map((_, i) => (
                        <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} stroke="rgba(12,10,18,0.45)" />
                      ))}
                    </Pie>
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <PieLegend data={prepared.data} compact={compact} />
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              {prepared.type === 'horizontal-bar' ? (
                <BarChart
                  data={prepared.data}
                  layout="vertical"
                  margin={{ top: 8, right: compact ? 8 : 22, left: compact ? 42 : 96, bottom: 4 }}
                >
                  <defs>
                    <linearGradient id="barGradientH" x1="0" x2="1" y1="0" y2="0">
                      <stop offset="0%" stopColor="#B7AEFF" stopOpacity={0.92} />
                      <stop offset="100%" stopColor="#8B7CFF" stopOpacity={0.98} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} horizontal={false} />
                  <XAxis type="number" tick={tick} />
                  <YAxis
                    type="category"
                    dataKey="x"
                    tick={tick}
                    width={compact ? 52 : 110}
                    interval={0}
                  />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="y" fill="url(#barGradientH)" radius={[0, 8, 8, 0]} maxBarSize={compact ? 18 : 26} />
                </BarChart>
              ) : prepared.type === 'line' ? (
                <LineChart data={prepared.data} margin={{ top: 8, right: 12, left: 0, bottom: compact ? 18 : 34 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
                  <XAxis
                    dataKey="x"
                    tick={tick}
                    interval="preserveStartEnd"
                    minTickGap={compact ? 18 : 28}
                    angle={prepared.longestLabelLength > 8 && !compact ? -18 : 0}
                    textAnchor={prepared.longestLabelLength > 8 && !compact ? 'end' : 'middle'}
                    height={compact ? 34 : 54}
                  />
                  <YAxis tick={tick} width={38} />
                  <Tooltip content={<CustomTooltip />} />
                  <Line
                    type="monotone"
                    dataKey="y"
                    stroke="#B7AEFF"
                    strokeWidth={2.4}
                    dot={{ r: compact ? 2 : 3, fill: '#8B7CFF', stroke: '#F7F5FF', strokeWidth: 1 }}
                    activeDot={{ r: 5 }}
                  />
                </LineChart>
              ) : (
                <BarChart data={prepared.data} margin={{ top: 8, right: 12, left: 0, bottom: compact ? 18 : 48 }}>
                  <defs>
                    <linearGradient id="barGradientV" x1="0" x2="0" y1="0" y2="1">
                      <stop offset="0%" stopColor="#8B7CFF" stopOpacity={0.98} />
                      <stop offset="100%" stopColor="#B7AEFF" stopOpacity={0.86} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
                  <XAxis
                    dataKey="x"
                    tick={tick}
                    interval={0}
                    angle={prepared.longestLabelLength > 10 || prepared.data.length > 6 ? -28 : 0}
                    textAnchor={prepared.longestLabelLength > 10 || prepared.data.length > 6 ? 'end' : 'middle'}
                    height={compact ? 40 : 68}
                  />
                  <YAxis tick={tick} width={38} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="y" fill="url(#barGradientV)" radius={[8, 8, 0, 0]} maxBarSize={compact ? 26 : 46} />
                </BarChart>
              )}
            </ResponsiveContainer>
          )}
        </CardContent>
        <div className="border-t border-white/10 px-3 py-2 text-[10px] leading-relaxed text-white/45">
          {metadataText(prepared, t)}
        </div>
      </Card>
    );
  } catch {
    return null;
  }
}
