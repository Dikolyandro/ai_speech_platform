interface DataTableProps {
  data: Array<Record<string, string | number>>;
}

export function DataTable({ data }: DataTableProps) {
  if (data.length === 0) return null;

  const columns = Object.keys(data[0]);

  return (
    <div className="rounded-2xl border border-border overflow-hidden bg-card">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="bg-primary/5 border-b border-border">
              {columns.map((column) => (
                <th
                  key={column}
                  className="px-4 py-3 text-left text-xs text-white/60 uppercase tracking-wider"
                >
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <tr key={i} className="border-b border-border last:border-0 hover:bg-primary/5 transition-colors">
                {columns.map((column) => (
                  <td key={column} className="px-4 py-3 text-sm text-white">
                    {row[column]}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
