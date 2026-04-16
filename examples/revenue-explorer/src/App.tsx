import React, { useDeferredValue, useMemo, useState } from "react";

import {
  AppShell,
  Badge,
  Card,
  DataTable,
  DateRangeFilter,
  DEFAULT_DATE_RANGE_PRESETS,
  type DateRangeValue,
  EChart,
  ErrorState,
  FilterPills,
  IssueBanner,
  KpiCard,
  LoadingState,
  MultiSelect,
  PerspectivePanel,
  ReportTable,
  ResourceCacheBadge,
  Select,
  TabGroup,
  TextInput,
  Tooltip,
  useDataset,
  useJsonResource,
  usePerspective,
  useSqlQuery,
  useTheme,
} from "@definite/runtime";

type BranchOption = {
  branchId: string;
  branchName: string;
};

type AppIssue = {
  key: string;
  title: string;
  message: string;
  severity: "warning";
};

const searchIcon = (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="11" cy="11" r="8" />
    <line x1="21" y1="21" x2="16.65" y2="16.65" />
  </svg>
);

function escapeSql(value: string): string {
  return value.replace(/'/g, "''");
}

export default function App() {
  const { theme, toggleTheme } = useTheme();
  const transactions = useDataset("transactions");
  const branches = useJsonResource<BranchOption[]>("branches");
  const [selectedBranches, setSelectedBranches] = useState<BranchOption[]>([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [activeTab, setActiveTab] = useState("Dashboard");
  const [search, setSearch] = useState("");
  const [reportFilter, setReportFilter] = useState("All");
  const [reportRange, setReportRange] = useState<DateRangeValue>(() =>
    DEFAULT_DATE_RANGE_PRESETS.find((p) => p.key === "last90")!.compute(),
  );
  const deferredSearch = useDeferredValue(search);
  const perspective = usePerspective(transactions);

  // Build SQL WHERE clause from active filters
  const whereClause = useMemo(() => {
    const clauses: string[] = [];
    if (selectedBranches.length > 0) {
      const names = selectedBranches.map((b) => `'${escapeSql(b.branchName)}'`).join(", ");
      clauses.push(`branchName IN (${names})`);
    }
    if (statusFilter) {
      clauses.push(`status = '${escapeSql(statusFilter)}'`);
    }
    return clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
  }, [selectedBranches, statusFilter]);

  // Build Perspective filter array from active filters
  const perspectiveFilters = useMemo(() => {
    const filters: Array<[string, string, unknown]> = [];
    if (selectedBranches.length === 1) {
      filters.push(["branchName", "==", selectedBranches[0].branchName]);
    } else if (selectedBranches.length > 1) {
      filters.push(["branchName", "in", selectedBranches.map((b) => b.branchName)]);
    }
    if (statusFilter) {
      filters.push(["status", "==", statusFilter]);
    }
    return filters;
  }, [selectedBranches, statusFilter]);

  // Filtered KPI metrics
  const metrics = useSqlQuery(
    transactions,
    transactions.tableRef
      ? `SELECT COUNT(*)::INTEGER AS rowCount, COALESCE(SUM(amount), 0)::DOUBLE AS totalAmount, COALESCE(AVG(amount), 0)::DOUBLE AS averageAmount FROM ${transactions.tableRef}${whereClause}`
      : "",
    [whereClause],
  );

  // Report tab: WHERE clause from date range + segment
  const reportWhere = useMemo(() => {
    const clauses: string[] = [];
    if (reportRange.from) clauses.push(`transactionDate >= '${escapeSql(reportRange.from)}'`);
    if (reportRange.to) clauses.push(`transactionDate <= '${escapeSql(reportRange.to)}'`);
    if (reportFilter && reportFilter !== "All") clauses.push(`status = '${escapeSql(reportFilter)}'`);
    return clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
  }, [reportRange.from, reportRange.to, reportFilter]);

  const reportAgg = useSqlQuery<Array<{ branchName: string; units: number; volume: number }>>(
    transactions,
    transactions.tableRef
      ? `SELECT branchName, COUNT(*)::INTEGER AS units, COALESCE(SUM(amount), 0)::DOUBLE AS volume FROM ${transactions.tableRef}${reportWhere} GROUP BY branchName ORDER BY branchName`
      : "",
    [reportWhere],
  );

  // Filtered recent rows
  const recentRows = useSqlQuery(
    transactions,
    transactions.tableRef
      ? `SELECT transactionDate, branchName, status, amount FROM ${transactions.tableRef}${whereClause} ORDER BY transactionDate DESC LIMIT 20`
      : "",
    [whereClause],
  );

  const transactionRowCount = typeof metrics.data?.[0]?.rowCount === "number"
    ? metrics.data[0].rowCount
    : transactions.cache?.rowCount ?? null;
  const appIssues: AppIssue[] = [
    branches.error
      ? { key: "branches", title: "Branch directory unavailable", message: branches.error, severity: "warning" as const }
      : null,
    metrics.error
      ? { key: "metrics", title: "KPI metrics unavailable", message: metrics.error, severity: "warning" as const }
      : null,
    recentRows.error
      ? { key: "recentRows", title: "Recent transactions unavailable", message: recentRows.error, severity: "warning" as const }
      : null,
    perspective.error
      ? { key: "perspective", title: "Charts unavailable", message: perspective.error, severity: "warning" as const }
      : null,
  ].filter((issue): issue is AppIssue => issue !== null);

  const filteredBranches = (branches.data ?? [])
    .filter((branch) => {
      if (!deferredSearch) return true;
      return branch.branchName.toLowerCase().includes(deferredSearch.toLowerCase());
    })
    .slice(0, 25);

  // Drill-down handlers: click a bar to toggle the corresponding filter
  const handleBranchDrill = (row: Record<string, unknown> | null) => {
    if (!row) return;
    const value = String(row.branchName ?? row[Object.keys(row)[0]] ?? "");
    if (!value) return;
    // Toggle: if already the only selected branch, clear it
    if (selectedBranches.length === 1 && selectedBranches[0].branchName === value) {
      setSelectedBranches([]);
    } else {
      const match = (branches.data ?? []).find((b) => b.branchName === value);
      if (match) setSelectedBranches([match]);
    }
  };

  const handleStatusDrill = (row: Record<string, unknown> | null) => {
    if (!row) return;
    const value = String(row.status ?? row[Object.keys(row)[0]] ?? "");
    if (!value) return;
    setStatusFilter((prev) => (prev === value ? "" : value));
  };

  if (transactions.loading) {
    return <LoadingState message="Loading DuckDB, dataset, and Perspective runtime..." />;
  }

  if (transactions.error) {
    return <ErrorState title="Dataset failed to load" message={transactions.error} />;
  }

  const hasFilters = selectedBranches.length > 0 || statusFilter !== "";
  const kpis: Array<{ title: string; value: unknown; format: "number" | "currency"; detail?: React.ReactNode }> = [
    { title: "Transactions", value: metrics.data?.[0]?.rowCount ?? 0, format: "number" },
    {
      title: "Total Revenue",
      value: metrics.data?.[0]?.totalAmount ?? 0,
      format: "currency",
      detail: (
        <span className="text-xs" style={{ color: "var(--text-muted)" }}>
          B: <span style={{ color: "#4ade80", fontWeight: 600 }}>+8.2%</span>
          {" | "}
          LY: <span style={{ color: "#f87171", fontWeight: 600 }}>-3.1%</span>
        </span>
      ),
    },
    { title: "Average Ticket", value: metrics.data?.[0]?.averageAmount ?? 0, format: "currency" },
  ];

  return (
    <AppShell
      title="Revenue Explorer"
      subtitle={transactions.kind === "database" ? "Attached DuckDB model" : "Live dataset"}
      theme={theme}
      onToggleTheme={toggleTheme}
      meta={<ResourceCacheBadge rows={transactionRowCount} cache={transactions.cache} onClearAndReload={transactions.refresh} />}
    >
      <TabGroup
        tabs={["Dashboard", "Report", "Explorer", "Branches"]}
        activeTab={activeTab}
        onTabChange={setActiveTab}
      />

      {appIssues.length > 0 ? (
        <div className="grid gap-3">
          {appIssues.map((issue) => (
            <IssueBanner
              key={issue.key}
              title={issue.title}
              message={issue.message}
              severity={issue.severity}
            />
          ))}
        </div>
      ) : null}

      {activeTab === "Dashboard" ? (
        <>
          <div className="flex flex-wrap items-end gap-3">
            <MultiSelect<BranchOption>
              options={branches.data ?? []}
              selected={selectedBranches}
              onChange={setSelectedBranches}
              labelKey="branchName"
              valueKey="branchId"
              label="Branch"
              placeholder="All Branches"
              searchPlaceholder="Search branches..."
            />
            <Select
              options={[
                { value: "Funded", label: "Funded" },
                { value: "Pending", label: "Pending" },
                { value: "Review", label: "Review" },
              ]}
              value={statusFilter}
              onChange={setStatusFilter}
              label={<span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>Status<Tooltip content={<span>Transaction processing status.<br /><b>Funded</b>: completed, <b>Pending</b>: in progress, <b>Review</b>: awaiting approval.</span>}><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.5, cursor: "help" }}><circle cx="12" cy="12" r="10" /><line x1="12" y1="16" x2="12" y2="12" /><line x1="12" y1="8" x2="12.01" y2="8" /></svg></Tooltip></span>}
              placeholder="All Statuses"
            />
            {hasFilters ? (
              <button
                type="button"
                className="rounded-lg px-3 py-2 text-xs font-medium"
                style={{
                  background: "none",
                  border: "1px solid var(--border)",
                  color: "var(--text-muted)",
                  cursor: "pointer",
                  transition: "color 120ms ease, border-color 120ms ease",
                }}
                onMouseEnter={(e) => { e.currentTarget.style.color = "var(--text-primary)"; e.currentTarget.style.borderColor = "var(--border-hover)"; }}
                onMouseLeave={(e) => { e.currentTarget.style.color = "var(--text-muted)"; e.currentTarget.style.borderColor = "var(--border)"; }}
                onClick={() => { setSelectedBranches([]); setStatusFilter(""); }}
              >
                Clear filters
              </button>
            ) : null}
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            {kpis.map((kpi, i) => (
              <div key={kpi.title} style={{ animation: `fade-up 0.4s ease-out ${i * 0.08}s both` }}>
                <KpiCard
                  title={kpi.title}
                  value={kpi.value}
                  format={kpi.format}
                  loading={metrics.loading}
                  detail={kpi.detail}
                />
              </div>
            ))}
          </div>

          <div className="grid gap-6 xl:grid-cols-2">
            <Card title="Revenue by Branch" headerRight={selectedBranches.length > 0 ? <Badge variant="info">{selectedBranches.length} filtered</Badge> : null} noPadding>
              <div className="h-[320px]">
                <PerspectivePanel
                  client={perspective.client}
                  table={transactions.perspectiveTable}
                  resourceKey={transactions.key}
                  loading={perspective.loading}
                  error={perspective.error}
                  theme={theme}
                  onSelect={handleBranchDrill}
                  config={{
                    plugin: "X Bar",
                    columns: ["amount"],
                    group_by: ["branchName"],
                    aggregates: { amount: "sum" },
                    sort: [["amount", "desc"]],
                    settings: false,
                    filter: statusFilter ? [["status", "==", statusFilter]] : [],
                    columns_config: {
                      amount: { number_format: { style: "currency", currency: "USD", maximumFractionDigits: 0 } },
                    },
                  }}
                />
              </div>
            </Card>
            <Card title="Transactions by Status" headerRight={statusFilter ? <Badge variant="info">{statusFilter}</Badge> : null} noPadding>
              <div className="h-[320px]">
                <PerspectivePanel
                  client={perspective.client}
                  table={transactions.perspectiveTable}
                  resourceKey={transactions.key}
                  loading={perspective.loading}
                  error={perspective.error}
                  theme={theme}
                  onSelect={handleStatusDrill}
                  config={{
                    plugin: "Y Bar",
                    columns: ["transactionId"],
                    group_by: ["status"],
                    aggregates: { transactionId: "count" },
                    sort: [["transactionId", "desc"]],
                    settings: false,
                    filter: selectedBranches.length === 1
                      ? [["branchName", "==", selectedBranches[0].branchName]]
                      : selectedBranches.length > 1
                        ? [["branchName", "in", selectedBranches.map((b) => b.branchName)]]
                        : [],
                  }}
                />
              </div>
            </Card>
          </div>

          <Card title="Recent Transactions" headerRight={hasFilters ? <Badge variant="info">Filtered</Badge> : null}>
            <DataTable
              columns={[
                { key: "transactionDate", label: "Date" },
                { key: "branchName", label: "Branch" },
                { key: "status", label: "Status" },
                { key: "amount", label: "Amount" },
              ]}
              rows={(recentRows.data as Array<Record<string, unknown>>) ?? []}
              emptyState={recentRows.loading ? "Loading transactions..." : "No transactions match the current filters."}
            />
          </Card>
        </>
      ) : null}

      {activeTab === "Report" ? (
        <>
          <div className="flex flex-wrap items-end gap-4">
            <FilterPills
              label="Segment"
              options={[
                { value: "All", label: "All" },
                { value: "Funded", label: "Funded" },
                { value: "Pending", label: "Pending" },
                { value: "Review", label: "Review" },
              ]}
              value={reportFilter}
              onChange={setReportFilter}
            />
            <DateRangeFilter value={reportRange} onChange={setReportRange} />
          </div>

          <Card>
            <ReportTable
              headerGroups={[
                {
                  label: "Category",
                  subHeaders: [{ key: "category", label: "Category", align: "left" }],
                  color: "var(--bg-elevated)",
                },
                {
                  label: reportRange.label || "Range",
                  color: "#92400e",
                  subHeaders: [
                    { key: "units", label: "Units" },
                    { key: "volume", label: "Volume" },
                  ],
                },
              ]}
              rows={(() => {
                const data = reportAgg.data ?? [];
                const fmtMoney = (v: number) => `$${Math.round(v).toLocaleString()}`;
                const branchRows = data.map((r) => ({
                  type: "data" as const,
                  indent: true,
                  cells: {
                    category: r.branchName,
                    units: String(r.units),
                    volume: fmtMoney(r.volume),
                  },
                }));
                const totalUnits = data.reduce((s, r) => s + Number(r.units || 0), 0);
                const totalVolume = data.reduce((s, r) => s + Number(r.volume || 0), 0);
                if (branchRows.length === 0) {
                  return [
                    { type: "section" as const, cells: { category: "Branches" } },
                    {
                      type: "data" as const,
                      indent: true,
                      cells: {
                        category: reportAgg.loading ? "Loading..." : "No transactions in range",
                        units: "—",
                        volume: "—",
                      },
                    },
                  ];
                }
                return [
                  { type: "section" as const, cells: { category: "Branches" } },
                  ...branchRows,
                  {
                    type: "total" as const,
                    cells: {
                      category: "GRAND TOTAL",
                      units: String(totalUnits),
                      volume: fmtMoney(totalVolume),
                    },
                  },
                ];
              })()}
            />
          </Card>

          <Card title="Monthly Volume vs Target" noPadding>
            <div className="p-4">
              <EChart
                theme={theme}
                height={340}
                option={{
                  tooltip: { trigger: "axis" },
                  legend: { data: ["Actual", "Target"], top: 0, textStyle: { fontSize: 11 } },
                  grid: { left: 60, right: 20, bottom: 30, top: 40 },
                  xAxis: {
                    type: "category",
                    data: ["Jan", "Feb", "Mar", "Apr", "May", "Jun"],
                  },
                  yAxis: {
                    type: "value",
                    axisLabel: { formatter: (v: number) => v >= 1000 ? `$${v / 1000}K` : `$${v}` },
                  },
                  series: [
                    {
                      name: "Actual",
                      type: "bar",
                      data: [
                        { value: 42000, itemStyle: { color: "#22c55e" } },
                        { value: 38000, itemStyle: { color: "#22c55e" } },
                        { value: 28000, itemStyle: { color: "#f87171" } },
                        { value: 0, itemStyle: { color: "#71717a" } },
                        { value: 0, itemStyle: { color: "#71717a" } },
                        { value: 0, itemStyle: { color: "#71717a" } },
                      ],
                      barMaxWidth: 40,
                      itemStyle: { borderRadius: [4, 4, 0, 0] },
                    },
                    {
                      name: "Target",
                      type: "line",
                      data: [35000, 35000, 40000, 40000, 45000, 45000],
                      lineStyle: { type: "dashed", width: 2, color: "#eab308" },
                      itemStyle: { color: "#eab308" },
                      symbol: "circle",
                      symbolSize: 6,
                    },
                  ],
                }}
              />
            </div>
          </Card>
        </>
      ) : null}

      {activeTab === "Explorer" ? (
        <Card title="Perspective Explorer" noPadding>
          <div className="h-[600px]">
            <PerspectivePanel
              client={perspective.client}
              table={transactions.perspectiveTable}
              resourceKey={transactions.key}
              loading={perspective.loading}
              error={perspective.error}
              theme={theme}
              config={{
                plugin: "Datagrid",
                columns: ["transactionDate", "branchName", "status", "amount"],
                sort: [["transactionDate", "desc"]],
                settings: false,
              }}
            />
          </div>
        </Card>
      ) : null}

      {activeTab === "Branches" ? (
        <Card title="Branch Directory">
          <TextInput
            value={search}
            onChange={setSearch}
            placeholder="Search branches..."
            icon={searchIcon}
            className="mb-4"
          />
          <DataTable
            columns={[
              { key: "branchId", label: "Branch ID" },
              { key: "branchName", label: "Branch Name" },
            ]}
            rows={filteredBranches}
            emptyState={branches.loading ? "Loading branches..." : "No branches matched."}
          />
        </Card>
      ) : null}
    </AppShell>
  );
}
