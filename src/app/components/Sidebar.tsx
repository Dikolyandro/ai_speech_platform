import { useState } from 'react';
import {
  Plus,
  Mic,
  Database,
  Bookmark,
  MessageSquare,
  ChevronDown,
  PanelLeftClose,
  PanelLeftOpen,
} from 'lucide-react';

const voiceSessions = [
  'Sales performance overview',
  'Top products by region',
  'Q4 revenue analysis',
  'Customer churn insights',
  'Marketing attribution report',
];

type SidebarProps = {
  collapsed: boolean;
  setCollapsed: (value: boolean) => void;
};

export function Sidebar({ collapsed, setCollapsed }: SidebarProps) {
  const [open, setOpen] = useState(true);

  if (collapsed) {
    return (
      <aside className="w-[84px] h-screen bg-[#0b0b14] border-r border-white/10 flex flex-col items-center py-5 transition-all duration-300">
        <div className="flex flex-col items-center gap-4 w-full">
          <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center shrink-0">
            <div className="h-4 w-4 rounded-full border-2 border-white/90" />
          </div>

          <button
            onClick={() => setCollapsed(false)}
            className="h-10 w-10 rounded-xl bg-white/5 hover:bg-white/10 flex items-center justify-center transition"
          >
            <PanelLeftOpen className="h-5 w-5 text-white/70" />
          </button>
        </div>

        <div className="mt-8 flex flex-col items-center gap-3 w-full px-3">
          <button className="h-11 w-11 rounded-2xl bg-[#171726] hover:bg-[#1d1d30] border border-white/10 text-white flex items-center justify-center transition">
            <Plus className="h-5 w-5 text-white/80" />
          </button>

          <button className="h-11 w-11 rounded-2xl text-white/65 hover:text-white hover:bg-white/5 transition flex items-center justify-center">
            <Bookmark className="h-5 w-5" />
          </button>

          <button className="h-11 w-11 rounded-2xl text-white/65 hover:text-white hover:bg-white/5 transition flex items-center justify-center">
            <Database className="h-5 w-5" />
          </button>

          <button className="h-11 w-11 rounded-2xl bg-violet-500/12 text-white hover:bg-violet-500/16 transition flex items-center justify-center">
            <Mic className="h-5 w-5" />
          </button>
        </div>

        <div className="mt-auto pb-2">
          <div className="h-11 w-11 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center text-white font-semibold">
            S
          </div>
        </div>
      </aside>
    );
  }

  return (
    <aside className="w-[290px] h-screen bg-[#0b0b14] border-r border-white/10 flex flex-col transition-all duration-300">
      <div className="px-6 pt-8 pb-5">
        <div className="flex items-center justify-between mb-7">
          <div className="h-9 w-9 rounded-xl bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center shrink-0">
            <div className="h-4 w-4 rounded-full border-2 border-white/90" />
          </div>

          <button
            onClick={() => setCollapsed(true)}
            className="h-9 w-9 rounded-xl bg-white/5 hover:bg-white/10 flex items-center justify-center transition"
          >
            <PanelLeftClose className="h-5 w-5 text-white/70" />
          </button>
        </div>

        <button className="w-full h-12 rounded-2xl bg-[#171726] hover:bg-[#1d1d30] border border-white/10 text-white flex items-center gap-3 px-4 transition">
          <Plus className="h-5 w-5 text-white/80" />
          <span className="text-[15px] font-medium">New session</span>
        </button>
      </div>

      <div className="px-4">
        <div className="space-y-1">
          <button className="w-full flex items-center gap-3 px-4 h-11 rounded-2xl text-left text-white/65 hover:text-white hover:bg-white/5 transition">
            <Bookmark className="h-5 w-5 shrink-0" />
            <span className="text-[15px] font-medium">Saved queries</span>
          </button>

          <button className="w-full flex items-center gap-3 px-4 h-11 rounded-2xl text-left text-white/65 hover:text-white hover:bg-white/5 transition">
            <Database className="h-5 w-5 shrink-0" />
            <span className="text-[15px] font-medium">Uploaded datasets</span>
          </button>
        </div>
      </div>

      <div className="px-4 pt-5 pb-3">
        <div className="h-px bg-white/8" />
      </div>

      <div className="px-4 pb-2">
        <button
          onClick={() => setOpen(!open)}
          className="w-full flex items-center justify-between px-4 h-11 rounded-2xl bg-violet-500/12 text-white hover:bg-violet-500/16 transition"
        >
          <div className="flex items-center gap-3">
            <Mic className="h-5 w-5 shrink-0" />
            <span className="text-[15px] font-medium">Voice sessions</span>
          </div>

          <ChevronDown
            className={`h-4 w-4 transition-transform duration-200 ${
              open ? 'rotate-0' : '-rotate-90'
            }`}
          />
        </button>
      </div>

      <div className="flex-1 px-4 overflow-y-auto">
        {open && (
          <div className="space-y-1">
            {voiceSessions.map((chat) => (
              <button
                key={chat}
                className="w-full flex items-center gap-3 px-4 py-3 rounded-xl text-left text-white/55 hover:text-white hover:bg-white/5 transition"
              >
                <MessageSquare className="h-4 w-4 shrink-0 text-white/22" />
                <span className="truncate text-[14px]">{chat}</span>
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="p-4 border-t border-white/10">
        <div className="flex items-center gap-3 rounded-2xl bg-[#141420] px-4 py-4">
          <div className="h-11 w-11 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 flex items-center justify-center text-white font-semibold">
            S
          </div>
          <div className="min-w-0">
            <p className="text-white text-[15px] font-medium truncate">
              Sarah Johnson
            </p>
            <p className="text-white/45 text-[13px] truncate">
              sarah@company.com
            </p>
          </div>
        </div>
      </div>
    </aside>
  );
}