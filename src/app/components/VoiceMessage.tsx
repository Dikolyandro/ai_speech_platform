import { Play, Pause } from 'lucide-react';
import { useState } from 'react';

interface VoiceMessageProps {
  duration: string;
  transcript?: string;
}

export function VoiceMessage({ duration, transcript }: VoiceMessageProps) {
  const [isPlaying, setIsPlaying] = useState(false);

  return (
    <div className="inline-flex flex-col gap-2 max-w-md">
      <div className="flex items-center gap-3 px-4 py-3 rounded-2xl bg-primary/10 border border-primary/20">
        <button
          onClick={() => setIsPlaying(!isPlaying)}
          className="w-8 h-8 rounded-full bg-primary flex items-center justify-center hover:bg-primary/90 transition-all shadow-lg shadow-primary/30"
        >
          {isPlaying ? (
            <Pause className="w-4 h-4 text-white" fill="white" />
          ) : (
            <Play className="w-4 h-4 text-white ml-0.5" fill="white" />
          )}
        </button>

        {/* Waveform */}
        <div className="flex-1 flex items-center gap-0.5 h-8">
          {[3, 8, 5, 12, 7, 15, 9, 6, 11, 4, 13, 8, 5, 10, 6, 14, 7, 4, 9, 12, 6, 8, 5, 11].map(
            (height, i) => (
              <div
                key={i}
                className="w-1 rounded-full bg-primary/60 transition-all"
                style={{
                  height: `${height * (isPlaying ? 1.2 : 1)}px`,
                  opacity: isPlaying ? 1 : 0.6,
                }}
              />
            )
          )}
        </div>

        <span className="text-xs text-white/70">{duration}</span>
      </div>
      {transcript && (
        <p className="text-xs text-white/60 px-2">{transcript}</p>
      )}
    </div>
  );
}
