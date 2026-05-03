import React, { useMemo, useState } from "react";

import {
  buildPalette,
  CachePopover,
  DateRangeFilter,
  DEFAULT_DATE_RANGE_PRESETS,
  type DateRangeValue,
  DrillProvider,
  ErrorState,
  FiInspectable,
  LoadingState,
  PaletteProvider,
  SaasKpiCard,
  ShellLayout,
  Sidebar,
  type SidebarNavItem,
  useDataset,
  useDrill,
  usePalette,
  useSqlQuery,
  useTheme,
} from "@definite/runtime";

// Default to the "Last 12 months" preset. The filter is applied against the
// `createdDate` column declared in app.json — change DATE_COLUMN if you
// rename it.
const DATE_COLUMN = "createdDate";

function initialDateRange(): DateRangeValue {
  const preset =
    DEFAULT_DATE_RANGE_PRESETS.find((p) => p.key === "last12m") ??
    DEFAULT_DATE_RANGE_PRESETS[0];
  return preset.compute();
}

const escSql = (v: string) => v.replace(/'/g, "''");

function buildWhere(from: string, to: string): string {
  const clauses: string[] = [];
  if (from) clauses.push(`${DATE_COLUMN} >= '${escSql(from)}'`);
  if (to) clauses.push(`${DATE_COLUMN} <= '${escSql(to)}'`);
  return clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
}

// ── Sidebar navigation ────────────────────────────────────────────────────
// Each entry maps to a view rendered in the main pane. Icons are single
// glyphs (Unicode) so apps don't need an icon library; swap for SVGs when
// you want brand-specific marks.
const NAV_ITEMS: SidebarNavItem[] = [
  { id: "overview", label: "Overview", icon: "◧" },
  { id: "detail",   label: "Detail",   icon: "≣" },
];

// ── App root ─────────────────────────────────────────────────────────────
// Pattern: outer <App> holds dataset-loading fallbacks; <InnerApp> runs
// inside the palette + drill providers so every descendant can usePalette()
// and useDrill() without prop-drilling.

export default function App() {
  const { theme, toggleTheme } = useTheme();
  // Optionally pass a brand accent: buildPalette(theme, { accent: "#FF006E" })
  const palette = useMemo(() => buildPalette(theme), [theme]);

  const data = useDataset("main");
  if (data.loading) return <LoadingState message="Loading data…" />;
  if (data.error) return <ErrorState title="Dataset failed to load" message={data.error} />;

  return (
    <PaletteProvider value={palette}>
      <DrillProvider>
        <InnerApp
          theme={theme}
          onThemeChange={(t) => { if (t !== theme) toggleTheme(); }}
          dataset={data}
        />
      </DrillProvider>
    </PaletteProvider>
  );
}

type DatasetHandle = ReturnType<typeof useDataset>;

function InnerApp({ theme, onThemeChange, dataset }: {
  theme: "dark" | "light";
  onThemeChange: (t: "dark" | "light") => void;
  dataset: DatasetHandle;
}) {
  const P = usePalette();
  const [view, setView] = useState("overview");
  const [dateRange, setDateRange] = useState<DateRangeValue>(initialDateRange);

  const where = useMemo(
    () => buildWhere(dateRange.from, dateRange.to),
    [dateRange.from, dateRange.to],
  );

  // Example query — replace with your own.
  const summary = useSqlQuery<Array<{ rowCount: number }>>(
    dataset,
    dataset.tableRef
      ? `SELECT COUNT(*)::INTEGER AS rowCount FROM ${dataset.tableRef}${where}`
      : "",
    [where],
  );

  const rowCount = summary.data?.[0]?.rowCount ?? 0;
  const navItem = NAV_ITEMS.find((n) => n.id === view) ?? NAV_ITEMS[0];

  const sidebar = (
    <Sidebar
      logo={{ title: "My App", subtitle: "Replace this" }}
      navItems={NAV_ITEMS}
      activeView={view}
      onViewChange={setView}
      dateRangeSlot={
        <DateRangeFilter
          value={dateRange}
          onChange={setDateRange}
          label={null}
          popoverPlacement="right-start"
          triggerStyle={{ width: "100%", minWidth: 0, justifyContent: "space-between", padding: "7px 10px", fontSize: 12 }}
        />
      }
      theme={theme}
      onThemeChange={onThemeChange}
      footer={<>Live DuckDB · {rowCount.toLocaleString()} rows</>}
    />
  );

  const headerRight = (
    <CachePopover
      isLoading={summary.loading}
      rowCount={dataset.cache?.rowCount ?? rowCount}
      cache={dataset.cache}
      onRefresh={dataset.refresh}
    />
  );

  return (
    <ShellLayout
      palette={P}
      sidebar={sidebar}
      title={navItem.label}
      breadcrumb={["App", navItem.label]}
      headerRight={headerRight}
    >
      {view === "overview" && <OverviewView rowCount={rowCount} loading={summary.loading} />}
      {view === "detail" && <DetailView rowCount={rowCount} />}
    </ShellLayout>
  );
}

function OverviewView({ rowCount, loading }: { rowCount: number; loading: boolean }) {
  const drill = useDrill();
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
      {/* Wrap each tile in <FiInspectable> so the host can pick it as Fi context.
          `fiId` should be stable across renders; pass `datum` for richer prompts. */}
      <FiInspectable
        fiId="kpi-total-rows"
        datasetKey="main"
        description="Total row count KPI"
        datum={{ rowCount }}
      >
        <SaasKpiCard
          title="Total rows"
          value={rowCount.toLocaleString()}
          sub="in dataset"
          loading={loading}
          onClick={() => drill.open({
            kind: "kpi",
            id: "rows",
            title: "Total rows",
            value: rowCount.toLocaleString(),
            breadcrumb: "Overview",
            stats: [["Row count", rowCount.toLocaleString()]],
            narrative: "Dataset row count. Replace with your own computed stats.",
            sql: `SELECT COUNT(*) FROM main;`,
          })}
        />
      </FiInspectable>
    </div>
  );
}

function DetailView({ rowCount }: { rowCount: number }) {
  const P = usePalette();
  return (
    <div style={{
      background: P.card, border: `1px solid ${P.border}`, borderRadius: 10,
      padding: 24, color: P.sub, fontSize: 14, lineHeight: 1.6,
    }}>
      <div style={{ fontSize: 16, color: P.text, fontWeight: 600, marginBottom: 8 }}>Detail view</div>
      Replace this with your own detail content — tables, forms, drill-downs. The dataset has <b style={{ color: P.text }}>{rowCount.toLocaleString()}</b> rows available via <code style={{ fontFamily: P.mono, background: P.elev, padding: "1px 5px", borderRadius: 3 }}>useSqlQuery(data, "...")</code>.
    </div>
  );
}
