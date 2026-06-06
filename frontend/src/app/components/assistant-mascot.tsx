import mascotImage from '../../assets/assistant-bot-transparent.png';
import { cn } from './ui/utils';

type AssistantMascotProps = {
  size?: 'sm' | 'lg';
  className?: string;
};

export function AssistantMascot({ size = 'lg', className }: AssistantMascotProps) {
  const isSmall = size === 'sm';

  return (
    <div
      className={cn(
        'relative inline-flex shrink-0 items-center justify-center overflow-visible',
        isSmall ? 'h-10 w-10' : 'h-44 w-44 sm:h-60 sm:w-60',
        className
      )}
      aria-hidden="true"
    >
      <div className="relative h-full w-full animate-[mascotFloat_5s_ease-in-out_infinite]">
        <img
          src={mascotImage}
          alt=""
          draggable={false}
          className="h-full w-full select-none object-contain drop-shadow-[0_24px_60px_rgba(139,92,246,0.28)]"
        />
        <span className="mascot-eye-lid left-[25.8%] top-[48.0%] h-[17.4%] w-[20.2%]" />
        <span className="mascot-eye-lid left-[54.0%] top-[48.0%] h-[17.4%] w-[20.2%]" />
      </div>
    </div>
  );
}
