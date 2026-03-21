interface ConfidenceBadgeProps {
  confidence: number;
}

export function ConfidenceBadge({ confidence }: ConfidenceBadgeProps) {
  const isHigh = confidence >= 0.7;
  const isMedium = confidence >= 0.5 && confidence < 0.7;

  return (
    <div
      className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs ${
        isHigh
          ? 'bg-green-500/10 text-green-400 border border-green-500/20'
          : isMedium
          ? 'bg-yellow-500/10 text-yellow-400 border border-yellow-500/20'
          : 'bg-orange-500/10 text-orange-400 border border-orange-500/20'
      }`}
    >
      <div
        className={`w-1.5 h-1.5 rounded-full ${
          isHigh ? 'bg-green-400' : isMedium ? 'bg-yellow-400' : 'bg-orange-400'
        }`}
      />
      Confidence: {confidence.toFixed(2)}
    </div>
  );
}
