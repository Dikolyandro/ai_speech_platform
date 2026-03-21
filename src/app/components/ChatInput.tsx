import { Mic, Paperclip, Send, Square } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';

export function ChatInput() {
  const [isListening, setIsListening] = useState(false);
  const [samples, setSamples] = useState<number[]>([]);
  const [seconds, setSeconds] = useState(0);

  const audioContextRef = useRef<AudioContext | null>(null);
  const analyserRef = useRef<AnalyserNode | null>(null);
  const mediaStreamRef = useRef<MediaStream | null>(null);
  const animationRef = useRef<number | null>(null);
  const timerRef = useRef<number | null>(null);
  const lastPushRef = useRef(0);
  

  const MAX_SAMPLES = 120;
  

  const stopAudio = async () => {
    if (animationRef.current !== null) {
      cancelAnimationFrame(animationRef.current);
      animationRef.current = null;
    }

    if (timerRef.current !== null) {
      window.clearInterval(timerRef.current);
      timerRef.current = null;
    }

    mediaStreamRef.current?.getTracks().forEach((track) => track.stop());
    mediaStreamRef.current = null;

    if (audioContextRef.current) {
      await audioContextRef.current.close();
      audioContextRef.current = null;
    }

    analyserRef.current = null;
    lastPushRef.current = 0;
    setSeconds(0);
  };

  const startAudio = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      mediaStreamRef.current = stream;

      const audioContext = new AudioContext();
      const analyser = audioContext.createAnalyser();
      analyser.fftSize = 256;
      analyser.smoothingTimeConstant = 0.86;

      const source = audioContext.createMediaStreamSource(stream);
      source.connect(analyser);

      const dataArray = new Uint8Array(analyser.frequencyBinCount);

      audioContextRef.current = audioContext;
      analyserRef.current = analyser;

      timerRef.current = window.setInterval(() => {
        setSeconds((prev) => prev + 1);
      }, 1000);

      const updateWave = (time: number) => {
        analyser.getByteTimeDomainData(dataArray);

        let peak = 0;
        for (let i = 0; i < dataArray.length; i++) {
          const normalized = Math.abs((dataArray[i] - 128) / 128);
          if (normalized > peak) peak = normalized;
        }

        const scaled = Math.min(1, peak * 2.2);

        if (time - lastPushRef.current > 55) {
          setSamples((prev) => {
            const next = [...prev, scaled];
            return next.slice(-MAX_SAMPLES);
          });
          lastPushRef.current = time;
        }

        animationRef.current = requestAnimationFrame(updateWave);
      };

      animationRef.current = requestAnimationFrame(updateWave);
    } catch (error) {
      console.error('Microphone access denied or unavailable:', error);
      setIsListening(false);
    }
  };

  useEffect(() => {
    if (isListening) {
      startAudio();
    } else {
      stopAudio();
    }

    return () => {
      stopAudio();
    };
  }, [isListening]);

  const formattedTime = `00:${String(seconds).padStart(2, '0')}`;

  return (
    <div className="relative">
      {isListening && (
        <div className="pointer-events-none absolute -top-12 left-1/2 z-20 -translate-x-1/2">
          <div className="flex items-center gap-2 rounded-full border border-violet-300/20 bg-[#171726]/92 px-4 py-2 text-sm text-violet-100 shadow-[0_10px_35px_rgba(139,92,246,0.3)] backdrop-blur-xl">
            <span className="relative flex h-2.5 w-2.5">
              <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-fuchsia-400/70" />
              <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-fuchsia-300" />
            </span>
            Recording {formattedTime}
          </div>
        </div>
      )}

      <div className="rounded-[30px] border border-violet-400/12 bg-gradient-to-b from-[#1b1830] to-[#100f1d] px-5 py-4 shadow-[0_18px_60px_rgba(139,92,246,0.18)] backdrop-blur-xl">
        <div className="flex items-center gap-4">
          <div className="relative flex h-[82px] w-[82px] shrink-0 items-center justify-center">
            {isListening && (
              <>
                <span className="absolute inset-0 rounded-full bg-violet-500/18 blur-xl" />
                <span className="absolute -inset-2 rounded-full border border-violet-300/20" />
              </>
            )}

            <button
              onClick={() => setIsListening((prev) => !prev)}
              className={`relative z-10 flex items-center justify-center rounded-full transition-all duration-300 ${
                isListening
                  ? 'h-[82px] w-[82px] scale-[1.04] bg-gradient-to-br from-violet-400 via-fuchsia-500 to-purple-600 shadow-[0_0_55px_rgba(168,85,247,0.7)]'
                  : 'h-[74px] w-[74px] bg-gradient-to-br from-violet-400 via-violet-500 to-indigo-500 shadow-[0_16px_40px_rgba(139,92,246,0.42)] hover:scale-[1.03]'
              }`}
            >
              {isListening ? (
                <Square className="h-7 w-7 fill-white text-white" />
              ) : (
                <Mic className="h-8 w-8 text-white" />
              )}
            </button>
          </div>

          <div className="min-w-0 flex-1">
            <div className="mb-2 flex items-center justify-between gap-3">
              <p className="truncate text-sm font-medium text-white/92">
                {isListening ? 'Recording voice query...' : 'Start with voice'}
              </p>
              <span className="shrink-0 text-xs text-white/40">
                {isListening ? formattedTime : 'tap mic'}
              </span>
            </div>

            <div className="flex h-14 items-center rounded-2xl border border-violet-400/10 bg-[#121120]/92 px-3 overflow-hidden">
            <div className="flex h-full w-full items-center justify-end gap-[2px] overflow-hidden">
              {Array.from({ length: MAX_SAMPLES }).map((_, slotIndex) => {
              const offset = MAX_SAMPLES - samples.length;
              const sampleIndex = slotIndex - offset;
              const sample = sampleIndex >= 0 ? samples[sampleIndex] : undefined;

                const isFilled = sample !== undefined;
                const progress = slotIndex / Math.max(MAX_SAMPLES - 1, 1);

                const width = 3.2;

                const height = isFilled ? 8 + sample * 36 : 6;
                const opacity = isFilled ? 0.3 + progress * 0.7 : 0.08;
                const hueBoost = progress > 0.82;

                return (
                  <span
                    key={slotIndex}
                    className={`shrink-0 rounded-full transition-all duration-75 ${
                      isFilled
                        ? hueBoost
                          ? 'bg-gradient-to-t from-violet-500 via-fuchsia-400 to-violet-200'
                          : 'bg-gradient-to-t from-violet-600 to-violet-300'
                        : 'bg-white/[0.04]'
                    }`}
                    style={{
                      width: `${width}px`,
                      height: `${height}px`,
                      opacity,
                    }}
                  />
                );
              })}
            </div>
            </div>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <button className="rounded-2xl p-3 transition hover:bg-white/5">
              <Send className="h-5 w-5 text-white/55" />
            </button>

            <button className="rounded-2xl p-3 transition hover:bg-white/5">
              <Paperclip className="h-5 w-5 text-white/55" />
            </button>
          </div>
        </div>

        <div className="mt-3 border-t border-white/6 pt-3">
          <input
            type="text"
            placeholder="Or type a follow-up..."
            className="w-full bg-transparent text-sm text-white outline-none placeholder:text-white/32"
          />
        </div>
      </div>
    </div>
  );
}