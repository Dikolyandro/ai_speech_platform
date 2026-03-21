import { Sidebar } from './components/Sidebar';
import { ChatInput } from './components/ChatInput';
import { VoiceMessage } from './components/VoiceMessage';
import { FileCard } from './components/FileCard';
import { ConfidenceBadge } from './components/ConfidenceBadge';
import { DataTable } from './components/DataTable';
import { Bot } from 'lucide-react';
import { useState } from 'react';

export default function App() {
  const sampleData = [
    { Product: 'Widget A', Sales: 15420, Growth: '+12.5%', Rank: 1 },
    { Product: 'Widget B', Sales: 13890, Growth: '+8.3%', Rank: 2 },
    { Product: 'Widget C', Sales: 11250, Growth: '+15.7%', Rank: 3 },
    { Product: 'Widget D', Sales: 9840, Growth: '+5.2%', Rank: 4 },

    
    
  ];

  const [collapsed, setCollapsed] = useState(false);
  
  return (
    <div className="size-full flex bg-background dark overflow-hidden">
      <Sidebar collapsed={collapsed} setCollapsed={setCollapsed} />

      <div className="relative flex-1 min-w-0 flex flex-col h-screen bg-[#07070d] overflow-hidden">
        <div className="pointer-events-none absolute inset-0">
          <div className="absolute top-[-220px] right-[-120px] h-[520px] w-[520px] rounded-full bg-violet-600/18 blur-[170px]" />
          <div className="absolute bottom-[-260px] left-[-140px] h-[560px] w-[560px] rounded-full bg-indigo-500/14 blur-[180px]" />
          <div className="absolute inset-x-0 top-0 h-[220px] bg-gradient-to-b from-violet-500/6 to-transparent" />
        </div>

        <header className="sticky top-0 z-30">
          <div className="h-20 w-full border-b border-r border-violet-500/10 border-l-0 bg-[#171722]/78 backdrop-blur-xl shadow-[0_10px_35px_rgba(0,0,0,0.28)] flex items-center px-6">
            <h1 className="text-2xl font-semibold tracking-tight text-white">
              AI Analytics Assistant
            </h1>
          </div>
        </header>

        <div className="relative z-10 flex-1 overflow-y-auto px-8 pt-4 pb-6">
          <div className="max-w-4xl mx-auto space-y-7">
            <div className="flex justify-end">
              <div className="max-w-[520px]">
                <VoiceMessage
                  duration="0:05"
                  transcript="Show me the top products by sales"
                />
              </div>
            </div>

            <div className="flex justify-end">
              <div className="max-w-[520px]">
                <FileCard filename="sales_data_2024.csv" size="2.4 MB" />
              </div>
            </div>

            <div className="flex gap-4">
              <div className="w-9 h-9 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 shadow-[0_0_22px_rgba(139,92,246,0.42)] flex items-center justify-center flex-shrink-0">
                <Bot className="w-5 h-5 text-white" />
              </div>

              <div className="flex-1 rounded-[28px] border border-violet-500/10 bg-gradient-to-b from-[#18182b]/88 to-[#10101c]/88 backdrop-blur-xl shadow-[0_12px_42px_rgba(139,92,246,0.12)] px-5 py-4">
                <div className="space-y-4">
                  <ConfidenceBadge confidence={0.91} />
                  <p className="text-sm text-white/90 leading-relaxed">
                    I've analyzed your sales data and identified the top 4 products by total sales. Here's what the data shows:
                  </p>
                  <DataTable data={sampleData} />
                  <p className="text-sm text-white/65 leading-relaxed">
                    Widget A leads with 15,420 units sold, showing strong performance with 12.5% growth.
                  </p>
                </div>
              </div>
            </div>

            <div className="flex gap-4">
              <div className="w-9 h-9 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 shadow-[0_0_22px_rgba(139,92,246,0.42)] flex items-center justify-center flex-shrink-0">
                <Bot className="w-5 h-5 text-white" />
              </div>

              <div className="flex-1 rounded-[28px] border border-violet-500/10 bg-gradient-to-b from-[#18182b]/88 to-[#10101c]/88 backdrop-blur-xl shadow-[0_12px_42px_rgba(139,92,246,0.12)] px-5 py-4">
                <div className="space-y-4">
                  <ConfidenceBadge confidence={0.87} />
                  <p className="text-sm text-white/90 leading-relaxed">
                    I've generated a detailed analysis report with visualizations and insights.
                  </p>
                  <div className="max-w-[480px]">
                    <FileCard
                      filename="sales_analysis_report.pdf"
                      size="1.8 MB"
                      type="download"
                    />
                  </div>
                </div>
              </div>
            </div>

            <div className="flex gap-4">
              <div className="w-9 h-9 rounded-full bg-gradient-to-br from-violet-500 to-indigo-400 shadow-[0_0_22px_rgba(139,92,246,0.42)] flex items-center justify-center flex-shrink-0">
                <Bot className="w-5 h-5 text-white" />
              </div>

              <div className="flex-1 rounded-[28px] border border-orange-500/14 bg-[linear-gradient(180deg,rgba(60,24,8,0.78),rgba(34,13,6,0.76))] backdrop-blur-xl shadow-[0_10px_30px_rgba(255,140,0,0.08)] px-5 py-4">
                <div className="space-y-4">
                  <ConfidenceBadge confidence={0.42} />
                  <div className="space-y-3">
                    <p className="text-sm text-orange-300 font-medium">
                      I'm not fully confident about this request. Did you mean:
                    </p>
                    <div className="space-y-2">
                      <div className="rounded-2xl border border-orange-500/12 bg-white/[0.03] px-4 py-3 text-sm text-white/82">
                        Show revenue trends by quarter?
                      </div>
                      <div className="rounded-2xl border border-orange-500/12 bg-white/[0.03] px-4 py-3 text-sm text-white/82">
                        Compare sales across regions?
                      </div>
                      <div className="rounded-2xl border border-orange-500/12 bg-white/[0.03] px-4 py-3 text-sm text-white/82">
                        Analyze customer segmentation?
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="relative z-10 p-5 border-t border-violet-500/10 bg-[#0b0b14]/72 backdrop-blur-xl">
          <div className="max-w-4xl mx-auto">
            <ChatInput />
          </div>
        </div>
      </div>
    </div>
  );
}