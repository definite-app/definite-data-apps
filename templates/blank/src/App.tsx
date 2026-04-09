import React from "react";
import {
  useDataset,
  useSqlQuery,
  useTheme,
  AppShell,
  Card,
  KpiCard,
  DataTable,
  LoadingState,
  ErrorState,
} from "./definite-runtime";

export default function App() {
  const theme = useTheme();
  const data = useDataset("main");

  const summary = useSqlQuery(
    data,
    data.tableRef
      ? `SELECT COUNT(*)::INTEGER AS totalRows FROM ${data.tableRef}`
      : "",
    [],
  );

  if (data.loading) return <LoadingState message="Loading data..." />;
  if (data.error) return <ErrorState title="Load Error" message={data.error} />;

  return (
    <AppShell title="My App" subtitle="Edit src/App.tsx to build your dashboard">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <KpiCard
          title="Total Rows"
          value={summary.data?.[0]?.totalRows}
          format="number"
          loading={summary.loading}
        />
      </div>
    </AppShell>
  );
}
