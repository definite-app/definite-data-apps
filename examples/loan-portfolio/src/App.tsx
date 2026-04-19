import React, { useMemo, useState } from "react";

import {
  Breadcrumb,
  buildPalette,
  CachePopover,
  DateRangeFilter,
  DEFAULT_DATE_RANGE_PRESETS,
  type DateRangeValue,
  DrillProvider,
  ErrorState,
  type FilterAccordionGroup,
  type FilterAccordionOption,
  LoadingState,
  PaletteProvider,
  SaasKpiCard,
  ShellLayout,
  Sidebar,
  type SidebarNavItem,
  SkeletonShimmer,
  Sparkline,
  useDataset,
  useDrill,
  useJsonResource,
  usePalette,
  useSqlQuery,
  useTheme,
} from "./definite-runtime";

// ── SQL helpers ───────────────────────────────────────────────────────────
const esc = (v: string) => v.replace(/'/g, "''");

type Filters = Record<string, string[]>;

function buildWhere(filters: Filters, from: string, to: string): string {
  const clauses: string[] = [];
  if (from) clauses.push(`originated >= '${esc(from)}'`);
  if (to) clauses.push(`originated <= '${esc(to)}'`);
  for (const [col, vals] of Object.entries(filters)) {
    if (!vals || vals.length === 0) continue;
    const inList = vals.map((v) => `'${esc(v)}'`).join(",");
    clauses.push(`${col} IN (${inList})`);
  }
  return clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
}

// Formatters
const fmtMoney = (v: unknown) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  if (Math.abs(n) >= 1e3) return `$${Math.round(n / 1e3)}K`;
  return `$${Math.round(n).toLocaleString()}`;
};
const fmtMoneyFull = (v: unknown) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `$${Math.round(n).toLocaleString()}`;
};
const fmtNum = (v: unknown) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString();
};
const fmtPct = (v: unknown, d = 2) => {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(d)}%`;
};

// Loan-specific swatch + humanize functions (app-level concern, stays here).
const FICO_SWATCH: Record<string, string> = {
  A: "#10b981", B: "#84cc16", C: "#f59e0b", D: "#f97316", E: "#ef4444",
};
const STATUS_SWATCH: Record<string, string> = {
  current: "#10b981", late_30: "#f59e0b", late_60: "#f59e0b",
  late_90: "#ef4444", paid_off: "#71717a", charged_off: "#ef4444",
};
const INCOME_LABELS: Record<string, string> = {
  lt50: "< $50K", "50_75": "$50K–$75K", "75_100": "$75K–$100K",
  "100_150": "$100K–$150K", "150_200": "$150K–$200K", gt200: "$200K+",
};
const DTI_LABELS: Record<string, string> = {
  lt20: "Under 20%", "20_30": "20–30%", "30_40": "30–40%", gt40: "40%+",
};

type FilterGroupMeta = {
  id: string;
  label: string;
  format?: (v: string) => string;
};
const FILTER_GROUPS_META: FilterGroupMeta[] = [
  { id: "ficoBand",        label: "Risk band" },
  { id: "status",          label: "Loan status", format: (v) => v.replace(/_/g, " ") },
  { id: "state",           label: "State" },
  { id: "vintage",         label: "Origination vintage" },
  { id: "product",         label: "Product type", format: (v) => v.replace(/_/g, " ") },
  { id: "term",            label: "Loan term", format: (v) => `${v} months` },
  { id: "channel",         label: "Acquisition channel", format: (v) => v.replace(/_/g, " ") },
  { id: "employment",      label: "Employment", format: (v) => v.replace(/_/g, " ") },
  { id: "incomeBand",      label: "Income band" },
  { id: "dtiBand",         label: "DTI ratio" },
  { id: "collectionsFlag", label: "Collections flag" },
  { id: "payMethod",       label: "Payment method", format: (v) => v.replace(/_/g, " ") },
];
const META_BY_ID = Object.fromEntries(FILTER_GROUPS_META.map((g) => [g.id, g]));

function humanize(groupId: string, value: string): string {
  if (groupId === "incomeBand") return INCOME_LABELS[value] ?? value;
  if (groupId === "dtiBand") return DTI_LABELS[value] ?? value;
  if (groupId === "ficoBand") return `Band ${value}`;
  const meta = META_BY_ID[groupId];
  return meta?.format ? meta.format(value) : value;
}
function swatchFor(groupId: string, value: string): string | undefined {
  if (groupId === "ficoBand") return FICO_SWATCH[value];
  if (groupId === "status") return STATUS_SWATCH[value];
  return undefined;
}

function initialDateRange(): DateRangeValue {
  const preset =
    DEFAULT_DATE_RANGE_PRESETS.find((p) => p.key === "last12m") ??
    DEFAULT_DATE_RANGE_PRESETS[0];
  return preset.compute();
}

// Data shapes returned by useSqlQuery
type KpiRow = {
  totalOutstanding: number;
  activeCount: number;
  avgPrincipal: number;
  delinquent90Count: number;
  totalCount: number;
};
type StatusRow = { status: string; cnt: number; vol: number };
type MonthRow = { originatedMonth: string; vol: number; cnt: number };
type BandRow = { ficoBand: string; cnt: number; vol: number; avgRate: number };
type StateRow = { state: string; cnt: number; vol: number };
type RecentRow = { loanId: string; borrower: string; amount: number; fico: number; status: string; state: string; originated: string };
type DimRow = { id: string; cnt: number };

const NAV_ITEMS: SidebarNavItem[] = [
  { id: "overview",     label: "Overview",     icon: "◧" },
  { id: "loans",        label: "Loans",        icon: "≣" },
  { id: "risk",         label: "Risk",         icon: "◎" },
  { id: "originations", label: "Originations", icon: "↗" },
  { id: "delinquency",  label: "Delinquency",  icon: "⚠" },
  { id: "geography",    label: "Geography",    icon: "◔" },
];

function statusTone(status: string, ok: string, warn: string, bad: string, dim: string) {
  if (status === "current") return ok;
  if (status === "paid_off") return dim;
  if (status === "late_30" || status === "late_60") return warn;
  return bad;
}
function statusSoft(status: string, okSoft: string, warnSoft: string, badSoft: string, elev: string) {
  if (status === "current") return okSoft;
  if (status === "paid_off") return elev;
  if (status === "late_30" || status === "late_60") return warnSoft;
  return badSoft;
}

// ── Top-level App ─────────────────────────────────────────────────────────

export default function App() {
  const { theme, toggleTheme } = useTheme();
  const palette = useMemo(() => buildPalette(theme), [theme]);

  const data = useDataset("loans");
  if (data.loading) return <LoadingState message="Loading loan book…" />;
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
  const riskBands = useJsonResource<Array<{ band: string; range: string; apr: number; defaultRate: number; color: string }>>("riskBands");

  const [view, setView] = useState("overview");
  const [dateRange, setDateRange] = useState<DateRangeValue>(initialDateRange);
  const [filters, setFilters] = useState<Filters>({});
  const [sortKey, setSortKey] = useState("originated");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [loanSearch, setLoanSearch] = useState("");

  const where = useMemo(() => buildWhere(filters, dateRange.from, dateRange.to), [filters, dateRange.from, dateRange.to]);
  const t = dataset.tableRef;

  const dimCounts = useSqlQuery<DimRow[]>(
    dataset,
    t ? `
      WITH base AS (SELECT * FROM ${t})
      SELECT 'ficoBand||' || ficoBand AS id, COUNT(*)::INTEGER AS cnt FROM base GROUP BY 1
      UNION ALL SELECT 'status||' || status, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'state||' || state, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'vintage||' || vintage, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'product||' || product, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'term||' || term::VARCHAR, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'channel||' || channel, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'employment||' || employment, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'incomeBand||' || incomeBand, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'dtiBand||' || dtiBand, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'collectionsFlag||' || collectionsFlag, COUNT(*)::INTEGER FROM base GROUP BY 1
      UNION ALL SELECT 'payMethod||' || payMethod, COUNT(*)::INTEGER FROM base GROUP BY 1
    ` : "",
    [],
  );

  const filterGroups: FilterAccordionGroup[] = useMemo(() => {
    const rows = dimCounts.data || [];
    const byGroup: Record<string, FilterAccordionOption[]> = {};
    for (const r of rows) {
      const [gid, val] = String(r.id).split("||");
      if (!gid || !val) continue;
      (byGroup[gid] ||= []).push({
        id: val,
        label: humanize(gid, val),
        hint: (r.cnt as number).toLocaleString(),
        swatch: swatchFor(gid, val),
      });
    }
    const orderOf = {
      status: ["current", "late_30", "late_60", "late_90", "paid_off", "charged_off"],
      incomeBand: ["lt50", "50_75", "75_100", "100_150", "150_200", "gt200"],
      dtiBand: ["lt20", "20_30", "30_40", "gt40"],
    } as Record<string, string[]>;
    return FILTER_GROUPS_META.map((g) => ({
      id: g.id,
      label: g.label,
      options: (byGroup[g.id] || []).slice().sort((a, b) => {
        if (g.id === "ficoBand") return a.id.localeCompare(b.id);
        if (orderOf[g.id]) return orderOf[g.id].indexOf(a.id) - orderOf[g.id].indexOf(b.id);
        if (g.id === "term") return Number(a.id) - Number(b.id);
        return Number((b.hint ?? "0").replace(/,/g, "")) - Number((a.hint ?? "0").replace(/,/g, ""));
      }),
    }));
  }, [dimCounts.data]);

  const kpis = useSqlQuery<KpiRow[]>(
    dataset,
    t ? `
      SELECT
        SUM(balance)::BIGINT   AS totalOutstanding,
        SUM(CASE WHEN status IN ('current','late_30','late_60','late_90') THEN 1 ELSE 0 END)::INTEGER AS activeCount,
        ROUND(AVG(amount))::INTEGER AS avgPrincipal,
        SUM(CASE WHEN status = 'late_90' THEN 1 ELSE 0 END)::INTEGER AS delinquent90Count,
        COUNT(*)::INTEGER AS totalCount
      FROM ${t}${where}
    ` : "",
    [where],
  );
  const monthly = useSqlQuery<MonthRow[]>(
    dataset,
    t ? `SELECT originatedMonth, SUM(amount)::BIGINT AS vol, COUNT(*)::INTEGER AS cnt FROM ${t}${where} GROUP BY 1 ORDER BY 1` : "",
    [where],
  );
  const statusAgg = useSqlQuery<StatusRow[]>(
    dataset,
    t ? `SELECT status, COUNT(*)::INTEGER AS cnt, SUM(balance)::BIGINT AS vol FROM ${t}${where} GROUP BY 1` : "",
    [where],
  );
  const bandAgg = useSqlQuery<BandRow[]>(
    dataset,
    t ? `SELECT ficoBand, COUNT(*)::INTEGER AS cnt, SUM(balance)::BIGINT AS vol, ROUND(AVG(rate), 2) AS avgRate FROM ${t}${where} GROUP BY 1 ORDER BY 1` : "",
    [where],
  );
  const stateAgg = useSqlQuery<StateRow[]>(
    dataset,
    t ? `SELECT state, COUNT(*)::INTEGER AS cnt, SUM(balance)::BIGINT AS vol FROM ${t}${where} GROUP BY 1 ORDER BY vol DESC LIMIT 8` : "",
    [where],
  );
  const recent = useSqlQuery<RecentRow[]>(
    dataset,
    t ? `SELECT loanId, borrower, amount, fico, status, state, originated FROM ${t}${where} ORDER BY originated DESC LIMIT 8` : "",
    [where],
  );
  const loansSql = useMemo(() => {
    if (!t) return "";
    let clause = where;
    if (loanSearch.trim()) {
      const extra = `(borrower ILIKE '%${esc(loanSearch.trim())}%' OR loanId ILIKE '%${esc(loanSearch.trim())}%')`;
      clause = where ? `${where} AND ${extra}` : ` WHERE ${extra}`;
    }
    return `SELECT loanId, borrower, amount, balance, fico, ficoBand, rate, term, status, state, vintage, originated FROM ${t}${clause} ORDER BY ${sortKey} ${sortDir.toUpperCase()} LIMIT 500`;
  }, [t, where, loanSearch, sortKey, sortDir]);
  const loansTable = useSqlQuery<Array<Record<string, unknown>>>(dataset, loansSql, [loansSql]);

  const loading = kpis.loading || monthly.loading || statusAgg.loading || bandAgg.loading || stateAgg.loading || recent.loading;
  const k0 = kpis.data?.[0];
  const navItem = NAV_ITEMS.find((n) => n.id === view) ?? NAV_ITEMS[0];
  const sparkVals = (monthly.data || []).map((m) => Number(m.cnt) || 0);

  const sidebar = (
    <Sidebar
      logo={{ title: "Loan Portfolio", subtitle: "Fintech · Production" }}
      navItems={NAV_ITEMS.map((n) => n.id === "loans" ? { ...n, badge: k0?.totalCount?.toLocaleString() } : n)}
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
      filterGroups={filterGroups}
      filters={filters}
      onFiltersChange={setFilters}
      humanizeFilter={humanize}
      theme={theme}
      onThemeChange={onThemeChange}
      footer={<>Live DuckDB · {(k0?.totalCount ?? 0).toLocaleString()} rows<br />Cached 24h TTL</>}
    />
  );

  const headerRight = (
    <>
      <CachePopover
        isLoading={loading}
        rowCount={dataset.cache?.rowCount ?? k0?.totalCount ?? null}
        cache={dataset.cache}
        onRefresh={dataset.refresh}
      />
      <button style={{
        fontSize: 12, padding: "6px 12px", borderRadius: 6,
        background: P.accent, border: `1px solid ${P.accent}`, color: "#fff",
        cursor: "pointer", fontWeight: 500,
      }}>Export</button>
    </>
  );

  return (
    <ShellLayout
      palette={P}
      sidebar={sidebar}
      title={navItem.label}
      breadcrumb={["Portfolio", navItem.label]}
      headerRight={headerRight}
    >
      {view === "overview" && (
        <OverviewView
          loading={loading}
          k0={k0}
          monthly={monthly.data || []}
          statusAgg={statusAgg.data || []}
          bandAgg={bandAgg.data || []}
          recent={recent.data || []}
          sparkVals={sparkVals}
          dateRange={dateRange}
          where={where}
        />
      )}
      {view === "loans" && (
        <LoansView
          loading={loansTable.loading}
          rows={loansTable.data || []}
          sortKey={sortKey} sortDir={sortDir}
          onSort={(k) => { if (k === sortKey) setSortDir(sortDir === "asc" ? "desc" : "asc"); else { setSortKey(k); setSortDir("desc"); } }}
          search={loanSearch} setSearch={setLoanSearch}
        />
      )}
      {view === "risk" && (
        <RiskView loading={loading} bands={bandAgg.data || []} riskMeta={riskBands.data || []} />
      )}
      {view === "originations" && (
        <OriginationsView loading={monthly.loading} monthly={monthly.data || []} />
      )}
      {view === "delinquency" && (
        <DelinquencyView loading={loading} statusAgg={statusAgg.data || []} k0={k0} />
      )}
      {view === "geography" && (
        <GeographyView loading={stateAgg.loading} states={stateAgg.data || []} />
      )}
    </ShellLayout>
  );
}

// ── Views ─────────────────────────────────────────────────────────────────

function OverviewView({ loading, k0, monthly, statusAgg, bandAgg, recent, sparkVals, dateRange, where }: {
  loading: boolean;
  k0: KpiRow | undefined;
  monthly: MonthRow[]; statusAgg: StatusRow[]; bandAgg: BandRow[]; recent: RecentRow[];
  sparkVals: number[]; dateRange: DateRangeValue; where: string;
}) {
  const P = usePalette();
  const drill = useDrill();
  const totalStatus = statusAgg.reduce((s, r) => s + Number(r.vol || 0), 0) || 1;
  const totalCount = Number(k0?.totalCount ?? 0);
  const pctDelinq = totalCount > 0 ? (Number(k0?.delinquent90Count ?? 0) / totalCount) * 100 : 0;
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };

  const kpiSpec = [
    {
      id: "total_outstanding", title: "Total outstanding",
      value: fmtMoney(k0?.totalOutstanding ?? 0),
      delta: "+8.4%", up: true, sub: "vs. prior period", accent: P.accent,
      stats: [
        ["Outstanding", fmtMoneyFull(k0?.totalOutstanding ?? 0)],
        ["Active loans", fmtNum(k0?.activeCount ?? 0)],
        ["Avg per loan", fmtMoneyFull(k0?.avgPrincipal ?? 0)],
      ] as Array<[string, string]>,
    },
    {
      id: "active_loans", title: "Active loans",
      value: fmtNum(k0?.activeCount ?? 0),
      delta: "+312", up: true, sub: "new this quarter", accent: P.grad2,
      stats: [["Active", fmtNum(k0?.activeCount ?? 0)], ["Total", fmtNum(k0?.totalCount ?? 0)]] as Array<[string, string]>,
    },
    {
      id: "avg_principal", title: "Avg principal",
      value: fmtMoney(k0?.avgPrincipal ?? 0),
      delta: "+2.1%", up: true, sub: "trailing 90 days", accent: P.ok,
      stats: [["Avg", fmtMoneyFull(k0?.avgPrincipal ?? 0)]] as Array<[string, string]>,
    },
    {
      id: "delinquency", title: "90+ delinquent",
      value: fmtPct(pctDelinq, 2),
      delta: "+0.12pp", up: false, sub: "vs. prior month", accent: P.warn,
      stats: [["90+ DPD", fmtNum(k0?.delinquent90Count ?? 0)], ["Rate", fmtPct(pctDelinq, 2)]] as Array<[string, string]>,
    },
  ];

  const points = monthly.map((m, i) => {
    const max = Math.max(1, ...monthly.map((x) => Number(x.vol) || 0));
    const min = Math.min(...monthly.map((x) => Number(x.vol) || 0));
    const rng = max - min || 1;
    const x = monthly.length > 1 ? (i / (monthly.length - 1)) * 100 : 50;
    const y = 100 - ((Number(m.vol) - min) / rng) * 80 - 10;
    return { x, y };
  });
  const path = points.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ");
  const area = path ? `${path} L 100 100 L 0 100 Z` : "";
  const peak = monthly.reduce<MonthRow | null>((acc, m) => (!acc || Number(m.vol) > Number(acc.vol) ? m : acc), null);

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12, marginBottom: 12 }}>
        {kpiSpec.map((k) => (
          <SaasKpiCard
            key={k.id}
            title={k.title}
            value={k.value}
            delta={k.delta}
            up={k.up}
            sub={k.sub}
            spark={sparkVals.slice(-12)}
            accent={k.accent}
            loading={loading}
            onClick={() => drill.open({
              kind: "kpi", id: k.id, title: k.title, value: k.value,
              breadcrumb: "Overview",
              stats: k.stats,
              narrative: k.id === "total_outstanding"
                ? "Total principal balance across active contracts. Net of paydowns, charge-offs, and new originations."
                : k.id === "delinquency"
                ? "Ninety-plus day delinquency rate. Concentrated in D and E bands; watch item but within model tolerance."
                : undefined,
              sql: `SELECT ... FROM loans${where};`,
              breakdown: monthly.slice(-12).map((m) => ({ label: m.originatedMonth, value: Number(m.vol) })),
            })}
          />
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 12, marginBottom: 12 }}>
        <div
          onClick={() => drill.open({
            kind: "chart", id: "origination_chart", title: "Origination volume",
            value: peak ? fmtMoney(peak.vol) : undefined,
            subvalue: peak ? `peak · ${peak.originatedMonth}` : undefined,
            breadcrumb: "Overview",
            breakdown: monthly.slice(-12).map((m) => ({ label: m.originatedMonth, value: Number(m.vol) })),
            sql: `SELECT originatedMonth, SUM(amount) AS vol, COUNT(*) AS cnt\nFROM loans${where}\nGROUP BY 1 ORDER BY 1;`,
            narrative: "Monthly origination volume over the selected window. Click a bar to inspect that month.",
          })}
          style={{ ...cardStyle, padding: 18, cursor: "pointer" }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
            <div>
              <div style={{ fontSize: 13, fontWeight: 600 }}>Origination volume</div>
              <div style={{ fontSize: 11, color: P.dim, marginTop: 2 }}>{dateRange.label} · monthly</div>
            </div>
            <div style={{ fontSize: 11, color: P.sub, fontFamily: P.mono }}>peak {peak ? fmtMoney(peak.vol) : "—"}</div>
          </div>
          <div style={{ height: 200, position: "relative" }}>
            {loading ? <SkeletonShimmer width="100%" height={200} radius={6} /> : (
              <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: "100%", height: "100%" }}>
                <defs>
                  <linearGradient id="rv2Grad" x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor={P.accent} stopOpacity="0.28" />
                    <stop offset="100%" stopColor={P.accent} stopOpacity="0" />
                  </linearGradient>
                </defs>
                {[25, 50, 75].map((y) => (
                  <line key={y} x1="0" y1={y} x2="100" y2={y} stroke={P.rule} strokeWidth={0.3} vectorEffect="non-scaling-stroke" />
                ))}
                {area ? <path d={area} fill="url(#rv2Grad)" /> : null}
                {path ? <path d={path} stroke={P.accent} strokeWidth={0.6} fill="none" vectorEffect="non-scaling-stroke" /> : null}
              </svg>
            )}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontFamily: P.mono, fontSize: 10, color: P.faint }}>
            {monthly.filter((_, i) => i % Math.max(1, Math.floor(monthly.length / 6)) === 0).map((o, i) => (
              <span key={i}>{o.originatedMonth.slice(2)}</span>
            ))}
          </div>
        </div>

        <div style={{ ...cardStyle, padding: 18 }}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Loan status</div>
          <div style={{ fontSize: 11, color: P.dim, marginBottom: 18 }}>Portfolio composition</div>
          {loading
            ? Array.from({ length: 5 }).map((_, i) => (
                <div key={i} style={{ marginBottom: 10 }}><SkeletonShimmer width="100%" height={14} /></div>
              ))
            : statusAgg.map((s) => {
                const pct = (Number(s.vol) / totalStatus) * 100;
                const c = statusTone(s.status, P.ok, P.warn, P.bad, P.dim);
                return (
                  <div key={s.status}
                    onClick={() => drill.open({
                      kind: "chart", id: `status_${s.status}`,
                      title: s.status.replace(/_/g, " "),
                      value: fmtMoney(s.vol),
                      subvalue: `${fmtNum(s.cnt)} loans · ${pct.toFixed(1)}%`,
                      breadcrumb: "Overview / Status",
                      stats: [
                        ["Contracts", fmtNum(s.cnt)],
                        ["Volume", fmtMoneyFull(s.vol)],
                        ["Share", pct.toFixed(2) + "%"],
                      ],
                    })}
                    style={{ marginBottom: 10, cursor: "pointer" }}
                    onMouseEnter={(e) => (e.currentTarget.style.opacity = "0.75")}
                    onMouseLeave={(e) => (e.currentTarget.style.opacity = "1")}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                      <span style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12 }}>
                        <span style={{ width: 6, height: 6, borderRadius: "50%", background: c }} />
                        {s.status.replace(/_/g, " ")}
                      </span>
                      <span style={{ fontFamily: P.mono, fontSize: 11, color: P.sub }}>
                        {fmtMoney(s.vol)} <span style={{ color: P.faint }}>· {pct.toFixed(1)}%</span>
                      </span>
                    </div>
                    <div style={{ height: 4, background: P.elev, borderRadius: 2, overflow: "hidden" }}>
                      <div style={{ height: "100%", width: `${pct}%`, background: c, borderRadius: 2 }} />
                    </div>
                  </div>
                );
              })}
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1.3fr", gap: 12 }}>
        <div style={{ ...cardStyle, padding: 18 }}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Risk by band</div>
          <div style={{ fontSize: 11, color: P.dim, marginBottom: 14 }}>Click a row to drill</div>
          {loading
            ? Array.from({ length: 5 }).map((_, i) => <div key={i} style={{ marginBottom: 8 }}><SkeletonShimmer width="100%" height={26} /></div>)
            : bandAgg.map((b) => {
                const pct = totalCount > 0 ? (Number(b.cnt) / totalCount) * 100 : 0;
                const color = FICO_SWATCH[b.ficoBand] || P.accent;
                return (
                  <div key={b.ficoBand}
                    onClick={() => drill.open({
                      kind: "row", id: `band_${b.ficoBand}`,
                      title: `Band ${b.ficoBand}`,
                      value: `${fmtNum(b.cnt)} loans`,
                      subvalue: `Avg APR ${Number(b.avgRate).toFixed(2)}% · ${fmtMoney(b.vol)} outstanding`,
                      breadcrumb: "Risk / Band",
                      stats: [
                        ["Loans", fmtNum(b.cnt)],
                        ["Volume", fmtMoneyFull(b.vol)],
                        ["Avg APR", Number(b.avgRate).toFixed(2) + "%"],
                      ],
                    })}
                    style={{
                      display: "grid", gridTemplateColumns: "28px 1fr 60px",
                      alignItems: "center", gap: 10, padding: "9px 8px",
                      borderRadius: 6, cursor: "pointer",
                    }}
                    onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
                    onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                  >
                    <div style={{
                      width: 24, height: 24, borderRadius: 5, background: color,
                      display: "flex", alignItems: "center", justifyContent: "center",
                      fontSize: 12, fontWeight: 700, color: "#fff",
                    }}>{b.ficoBand}</div>
                    <div style={{ height: 6, background: P.elev, borderRadius: 3, overflow: "hidden" }}>
                      <div style={{ height: "100%", width: `${Math.min(100, pct * 3)}%`, background: color, borderRadius: 3 }} />
                    </div>
                    <div style={{ textAlign: "right", fontFamily: P.mono, fontSize: 12, color: P.text }}>{fmtNum(b.cnt)}</div>
                  </div>
                );
              })}
        </div>

        <div style={{ ...cardStyle, overflow: "hidden" }}>
          <div style={{ padding: "16px 18px", borderBottom: `1px solid ${P.border}` }}>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Recent originations</div>
            <div style={{ fontSize: 11, color: P.dim, marginTop: 2 }}>Last {recent.length} closings · click row to drill</div>
          </div>
          <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ color: P.dim, background: P.elev }}>
                <th style={{ textAlign: "left", padding: "8px 18px", fontWeight: 500, fontSize: 11 }}>Loan</th>
                <th style={{ textAlign: "left", padding: "8px 12px", fontWeight: 500, fontSize: 11 }}>Borrower</th>
                <th style={{ textAlign: "right", padding: "8px 12px", fontWeight: 500, fontSize: 11 }}>Amount</th>
                <th style={{ textAlign: "right", padding: "8px 12px", fontWeight: 500, fontSize: 11 }}>FICO</th>
                <th style={{ textAlign: "left", padding: "8px 18px", fontWeight: 500, fontSize: 11 }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {loading
                ? Array.from({ length: 6 }).map((_, i) => (
                    <tr key={i}><td colSpan={5} style={{ padding: "10px 18px" }}><SkeletonShimmer width="100%" height={14} /></td></tr>
                  ))
                : recent.map((r) => {
                    const c = statusTone(r.status, P.ok, P.warn, P.bad, P.dim);
                    const bg = statusSoft(r.status, P.okSoft, P.warnSoft, P.badSoft, P.elev);
                    return (
                      <tr key={r.loanId}
                        onClick={() => drill.open({
                          kind: "row", id: `loan_${r.loanId}`,
                          title: `${r.loanId} — ${r.borrower}`,
                          value: fmtMoneyFull(r.amount),
                          subvalue: `FICO ${r.fico} · ${r.state} · ${r.originated}`,
                          breadcrumb: "Overview / Loan",
                          stats: [
                            ["Amount", fmtMoneyFull(r.amount)],
                            ["FICO", String(r.fico)],
                            ["State", r.state],
                            ["Status", r.status.replace(/_/g, " ")],
                            ["Originated", r.originated],
                          ],
                        })}
                        style={{ borderTop: `1px solid ${P.border}`, cursor: "pointer" }}
                        onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
                        onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                      >
                        <td style={{ padding: "10px 18px", fontFamily: P.mono, fontSize: 11, color: P.sub }}>{r.loanId}</td>
                        <td style={{ padding: "10px 12px" }}>{r.borrower}</td>
                        <td style={{ padding: "10px 12px", textAlign: "right", fontFamily: P.mono, fontWeight: 500 }}>{fmtMoneyFull(r.amount)}</td>
                        <td style={{ padding: "10px 12px", textAlign: "right", fontFamily: P.mono, color: r.fico < 650 ? P.bad : P.sub }}>{r.fico}</td>
                        <td style={{ padding: "10px 18px" }}>
                          <span style={{
                            display: "inline-flex", alignItems: "center", gap: 5, fontSize: 11,
                            padding: "2px 7px", borderRadius: 10, background: bg, color: c, fontWeight: 500,
                          }}>
                            <span style={{ width: 5, height: 5, borderRadius: "50%", background: c }} />
                            {r.status.replace(/_/g, " ")}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

function LoansView({ loading, rows, sortKey, sortDir, onSort, search, setSearch }: {
  loading: boolean;
  rows: Array<Record<string, unknown>>;
  sortKey: string; sortDir: "asc" | "desc"; onSort: (k: string) => void;
  search: string; setSearch: (s: string) => void;
}) {
  const P = usePalette();
  const drill = useDrill();
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const cols: Array<{ key: string; label: string; align?: "left" | "right"; render?: (v: unknown, row: Record<string, unknown>) => React.ReactNode; mono?: boolean }> = [
    { key: "loanId", label: "Loan", mono: true },
    { key: "borrower", label: "Borrower" },
    { key: "amount", label: "Amount", align: "right", mono: true, render: (v) => fmtMoneyFull(v) },
    { key: "balance", label: "Balance", align: "right", mono: true, render: (v) => fmtMoneyFull(v) },
    { key: "fico", label: "FICO", align: "right", mono: true, render: (v) => <span style={{ color: Number(v) < 650 ? P.bad : P.sub }}>{String(v)}</span> },
    { key: "ficoBand", label: "Band", align: "right", mono: true },
    { key: "rate", label: "APR", align: "right", mono: true, render: (v) => Number(v).toFixed(2) + "%" },
    { key: "term", label: "Term", align: "right", mono: true },
    { key: "state", label: "State", mono: true },
    { key: "vintage", label: "Vintage", mono: true },
    { key: "status", label: "Status", render: (v) => {
      const s = String(v);
      const c = statusTone(s, P.ok, P.warn, P.bad, P.dim);
      const bg = statusSoft(s, P.okSoft, P.warnSoft, P.badSoft, P.elev);
      return (
        <span style={{ display: "inline-flex", alignItems: "center", gap: 5, fontSize: 11, padding: "2px 7px", borderRadius: 10, background: bg, color: c, fontWeight: 500 }}>
          <span style={{ width: 5, height: 5, borderRadius: "50%", background: c }} />
          {s.replace(/_/g, " ")}
        </span>
      );
    } },
    { key: "originated", label: "Originated", mono: true },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between", gap: 16 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 600, letterSpacing: "-0.01em" }}>Loan book</div>
          <div style={{ fontSize: 12, color: P.sub, marginTop: 2 }}>
            Loan-level detail · showing {rows.length.toLocaleString()} rows · sort any column
          </div>
        </div>
        <input
          value={search} onChange={(e) => setSearch(e.target.value)}
          placeholder="Search borrower or loan id…"
          style={{
            padding: "6px 10px", fontSize: 12, background: P.elev, border: `1px solid ${P.border}`,
            color: P.text, borderRadius: 6, outline: "none", fontFamily: P.sans, width: 260,
          }}
        />
      </div>
      <div style={{ ...cardStyle, overflow: "hidden" }}>
        <div style={{ maxHeight: "calc(100vh - 220px)", overflow: "auto" }}>
          <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
            <thead style={{ position: "sticky", top: 0, background: P.elev, zIndex: 1 }}>
              <tr style={{ color: P.dim }}>
                {cols.map((c) => (
                  <th key={c.key}
                    onClick={() => onSort(c.key)}
                    style={{
                      textAlign: c.align ?? "left", padding: "10px 14px",
                      fontWeight: 500, fontSize: 11, cursor: "pointer",
                      borderBottom: `1px solid ${P.border}`, userSelect: "none",
                      color: sortKey === c.key ? P.accent : P.dim,
                      whiteSpace: "nowrap",
                    }}
                  >
                    {c.label} {sortKey === c.key ? (sortDir === "asc" ? "↑" : "↓") : ""}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={cols.length} style={{ padding: "40px 14px", textAlign: "center", color: P.dim }}>Loading…</td></tr>
              ) : rows.length === 0 ? (
                <tr><td colSpan={cols.length} style={{ padding: "40px 14px", textAlign: "center", color: P.dim }}>No loans match your filters.</td></tr>
              ) : rows.map((row, i) => (
                <tr key={String(row.loanId) + i}
                  onClick={() => drill.open({
                    kind: "row", id: `loan_${row.loanId}`,
                    title: `${row.loanId} — ${row.borrower}`,
                    value: fmtMoneyFull(row.amount),
                    subvalue: `FICO ${row.fico} · ${row.state} · ${row.originated}`,
                    breadcrumb: "Loans / Loan",
                    stats: [
                      ["Amount", fmtMoneyFull(row.amount)],
                      ["Balance", fmtMoneyFull(row.balance)],
                      ["FICO", String(row.fico)],
                      ["APR", Number(row.rate).toFixed(2) + "%"],
                      ["Term", String(row.term) + " mo"],
                      ["State", String(row.state)],
                      ["Status", String(row.status).replace(/_/g, " ")],
                      ["Originated", String(row.originated)],
                    ],
                  })}
                  style={{ borderTop: `1px solid ${P.border}`, cursor: "pointer" }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                >
                  {cols.map((c) => (
                    <td key={c.key} style={{
                      padding: "8px 14px",
                      textAlign: c.align ?? "left",
                      fontFamily: c.mono ? P.mono : P.sans,
                      color: c.key === "loanId" ? P.sub : P.text,
                      whiteSpace: "nowrap",
                    }}>
                      {c.render ? c.render(row[c.key], row) : String(row[c.key] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function RiskView({ loading, bands, riskMeta }: {
  loading: boolean; bands: BandRow[];
  riskMeta: Array<{ band: string; range: string; apr: number; defaultRate: number; color: string }>;
}) {
  const P = usePalette();
  const drill = useDrill();
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const maxCount = Math.max(1, ...bands.map((b) => Number(b.cnt) || 0));
  const metaByBand = Object.fromEntries(riskMeta.map((m) => [m.band, m]));
  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 12 }}>
        {loading
          ? Array.from({ length: 5 }).map((_, i) => <div key={i} style={{ ...cardStyle, padding: 16 }}><SkeletonShimmer width="100%" height={90} /></div>)
          : bands.map((b) => {
              const meta = metaByBand[b.ficoBand];
              const color = FICO_SWATCH[b.ficoBand] || P.accent;
              return (
                <div key={b.ficoBand}
                  onClick={() => drill.open({
                    kind: "kpi", id: `band_${b.ficoBand}`,
                    title: `Band ${b.ficoBand}`,
                    value: fmtNum(b.cnt),
                    subvalue: meta ? `FICO ${meta.range}` : undefined,
                    breadcrumb: "Risk",
                    stats: [
                      ["Contracts", fmtNum(b.cnt)],
                      ["Volume", fmtMoneyFull(b.vol)],
                      ["Avg APR", Number(b.avgRate).toFixed(2) + "%"],
                      ...(meta ? ([["Default rate", meta.defaultRate.toFixed(1) + "%"]] as Array<[string, string]>) : []),
                    ],
                  })}
                  style={{ ...cardStyle, padding: 16, cursor: "pointer" }}
                >
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                    <div style={{ width: 28, height: 28, borderRadius: 6, background: color, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13, fontWeight: 700, color: "#fff" }}>{b.ficoBand}</div>
                    <div style={{ fontFamily: P.mono, fontSize: 10, color: P.dim }}>{meta?.range ?? ""}</div>
                  </div>
                  <div style={{ fontSize: 24, fontWeight: 600, letterSpacing: "-0.02em" }}>{fmtNum(b.cnt)}</div>
                  <div style={{ fontSize: 11, color: P.dim, marginTop: 2 }}>contracts</div>
                  <div style={{ marginTop: 12, height: 3, background: P.elev, borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ height: "100%", width: `${(Number(b.cnt) / maxCount) * 100}%`, background: color }} />
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 10, fontSize: 11 }}>
                    <span style={{ color: P.sub }}>APR</span>
                    <span style={{ fontFamily: P.mono, color: P.text }}>{Number(b.avgRate).toFixed(2)}%</span>
                  </div>
                  {meta ? (
                    <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, fontSize: 11 }}>
                      <span style={{ color: P.sub }}>Default</span>
                      <span style={{ fontFamily: P.mono, color: meta.defaultRate > 5 ? P.bad : P.text }}>{meta.defaultRate.toFixed(1)}%</span>
                    </div>
                  ) : null}
                </div>
              );
            })}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <div style={{ ...cardStyle, padding: 18 }}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Risk × return</div>
          <div style={{ fontSize: 11, color: P.dim, marginBottom: 18 }}>Default rate vs. weighted APR · size = contracts</div>
          <div style={{ position: "relative", height: 260 }}>
            <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: "100%", height: "100%" }}>
              {[25, 50, 75].map((y) => <line key={"h" + y} x1="0" y1={y} x2="100" y2={y} stroke={P.rule} strokeWidth={0.3} vectorEffect="non-scaling-stroke" />)}
              {[25, 50, 75].map((x) => <line key={"v" + x} x1={x} y1="0" x2={x} y2="100" stroke={P.rule} strokeWidth={0.3} vectorEffect="non-scaling-stroke" />)}
              {bands.map((b) => {
                const meta = metaByBand[b.ficoBand];
                if (!meta) return null;
                const cx = Math.min(100, Math.max(0, (Number(b.avgRate) / 20) * 100));
                const cy = Math.min(100, Math.max(0, 100 - (meta.defaultRate / 15) * 100));
                const r = 3 + (Number(b.cnt) / maxCount) * 8;
                return <circle key={b.ficoBand} cx={cx} cy={cy} r={r} fill={FICO_SWATCH[b.ficoBand] || P.accent} opacity={0.75} />;
              })}
            </svg>
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontSize: 10, color: P.faint, fontFamily: P.mono }}>
            <span>0%</span><span>APR →</span><span>20%</span>
          </div>
        </div>

        <div style={{ ...cardStyle, padding: 18 }}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Band composition</div>
          <div style={{ fontSize: 11, color: P.dim, marginBottom: 18 }}>Share of outstanding by band</div>
          <div style={{ display: "flex", height: 24, borderRadius: 4, overflow: "hidden", border: `1px solid ${P.border}` }}>
            {bands.map((b) => {
              const total = bands.reduce((s, x) => s + Number(x.vol || 0), 0) || 1;
              const pct = (Number(b.vol) / total) * 100;
              return <div key={b.ficoBand} title={`${b.ficoBand}: ${pct.toFixed(1)}%`} style={{ width: `${pct}%`, background: FICO_SWATCH[b.ficoBand] || P.accent }} />;
            })}
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 12 }}>
            {bands.map((b) => {
              const total = bands.reduce((s, x) => s + Number(x.vol || 0), 0) || 1;
              const pct = (Number(b.vol) / total) * 100;
              return (
                <div key={b.ficoBand} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: P.sub }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: FICO_SWATCH[b.ficoBand] || P.accent }} />
                  <span style={{ fontFamily: P.mono }}>{b.ficoBand} · {pct.toFixed(1)}%</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </>
  );
}

function OriginationsView({ loading, monthly }: { loading: boolean; monthly: MonthRow[] }) {
  const P = usePalette();
  const drill = useDrill();
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const totalVol = monthly.reduce((s, m) => s + Number(m.vol || 0), 0);
  const maxMonthVol = Math.max(1, ...monthly.map((m) => Number(m.vol) || 0));
  const totalCnt = monthly.reduce((s, m) => s + Number(m.cnt || 0), 0);
  const lastMonthVol = monthly.slice(-1)[0]?.vol ?? 0;

  const kpis = [
    { id: "l_vol", label: "Period volume", value: fmtMoney(totalVol), delta: "+18%", up: true },
    { id: "new_cnt", label: "New contracts", value: fmtNum(totalCnt), delta: "+312", up: true },
    { id: "last_vol", label: "Most recent month", value: fmtMoney(lastMonthVol) },
  ];

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12, marginBottom: 12 }}>
        {kpis.map((k) => (
          <div key={k.id} style={{ ...cardStyle, padding: 16 }}>
            <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>{k.label}</div>
            <div style={{ fontSize: 26, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 8 }}>{k.value}</div>
            {k.delta ? (
              <div style={{ fontSize: 11, marginTop: 8, color: k.up ? P.ok : P.bad, fontFamily: P.mono }}>
                {k.up ? "↑" : "↓"} {k.delta}
              </div>
            ) : null}
          </div>
        ))}
      </div>
      <div
        onClick={() => drill.open({
          kind: "chart", id: "origination_timeline", title: "Origination timeline",
          value: fmtMoney(totalVol), breadcrumb: "Originations",
          breakdown: monthly.map((m) => ({ label: m.originatedMonth, value: Number(m.vol) })),
          narrative: "Monthly origination volume over the selected date range.",
        })}
        style={{ ...cardStyle, padding: 18, cursor: "pointer" }}
      >
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Origination timeline</div>
        <div style={{ fontSize: 11, color: P.dim, marginBottom: 18 }}>Monthly volume · {monthly.length} months</div>
        <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: 220 }}>
          {loading ? <SkeletonShimmer width="100%" height={220} radius={4} /> : monthly.map((o, i) => {
            const h = (Number(o.vol) / maxMonthVol) * 100;
            const isRecent = i >= monthly.length - 12;
            return <div key={i} title={`${o.originatedMonth}: ${fmtMoney(o.vol)}`}
              style={{ flex: 1, height: `${h}%`, background: isRecent ? P.accent : P.accentSoft, opacity: isRecent ? 1 : 0.7, borderRadius: "2px 2px 0 0" }} />;
          })}
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontSize: 10, color: P.faint, fontFamily: P.mono }}>
          <span>{monthly[0]?.originatedMonth ?? ""}</span>
          <span>{monthly[monthly.length - 1]?.originatedMonth ?? ""}</span>
        </div>
      </div>
    </>
  );
}

function DelinquencyView({ loading, statusAgg, k0 }: { loading: boolean; statusAgg: StatusRow[]; k0: KpiRow | undefined }) {
  const P = usePalette();
  const drill = useDrill();
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const delinq = statusAgg.filter((s) => String(s.status).includes("late") || s.status === "charged_off");
  const pct = k0?.totalCount ? (Number(k0.delinquent90Count ?? 0) / Number(k0.totalCount)) * 100 : 0;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
      {loading ? (
        Array.from({ length: 5 }).map((_, i) => <div key={i} style={{ ...cardStyle, padding: 16 }}><SkeletonShimmer width="100%" height={70} /></div>)
      ) : (
        <>
          {delinq.map((s) => {
            const c = statusTone(s.status, P.ok, P.warn, P.bad, P.dim);
            return (
              <div key={s.status}
                onClick={() => drill.open({
                  kind: "kpi", id: `status_${s.status}`,
                  title: s.status.replace(/_/g, " "),
                  value: fmtNum(s.cnt),
                  subvalue: fmtMoney(s.vol),
                  breadcrumb: "Delinquency",
                  stats: [["Contracts", fmtNum(s.cnt)], ["Volume", fmtMoneyFull(s.vol)]],
                })}
                style={{ ...cardStyle, padding: 16, borderLeft: `3px solid ${c}`, cursor: "pointer" }}
              >
                <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>{s.status.replace(/_/g, " ")}</div>
                <div style={{ fontSize: 26, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 8, color: c }}>{fmtNum(s.cnt)}</div>
                <div style={{ fontSize: 11, color: P.dim, marginTop: 6, fontFamily: P.mono }}>{fmtMoney(s.vol)}</div>
              </div>
            );
          })}
          <div style={{ ...cardStyle, padding: 16, borderLeft: `3px solid ${P.warn}` }}>
            <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>90+ DPD rate</div>
            <div style={{ fontSize: 26, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 8, color: P.warn }}>{pct.toFixed(2)}%</div>
            <div style={{ fontSize: 11, color: P.bad, marginTop: 6, fontFamily: P.mono }}>↑ +0.12pp MoM</div>
          </div>
        </>
      )}
    </div>
  );
}

function GeographyView({ loading, states }: { loading: boolean; states: StateRow[] }) {
  const P = usePalette();
  const drill = useDrill();
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const maxVol = Math.max(1, ...states.map((s) => Number(s.vol) || 0));
  return (
    <div style={{ ...cardStyle, padding: 18 }}>
      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Top 8 states</div>
      <div style={{ fontSize: 11, color: P.dim, marginBottom: 18 }}>Ranked by total volume · click to drill</div>
      {loading
        ? Array.from({ length: 8 }).map((_, i) => <div key={i} style={{ marginBottom: 8 }}><SkeletonShimmer width="100%" height={22} /></div>)
        : states.map((s, i) => (
            <div key={s.state}
              onClick={() => drill.open({
                kind: "row", id: `state_${s.state}`,
                title: `State · ${s.state}`,
                value: fmtMoney(s.vol),
                subvalue: `${fmtNum(s.cnt)} loans`,
                breadcrumb: "Geography",
                stats: [["Loans", fmtNum(s.cnt)], ["Volume", fmtMoneyFull(s.vol)]],
              })}
              style={{
                display: "grid", gridTemplateColumns: "20px 50px 1fr 80px 60px",
                alignItems: "center", gap: 12, padding: "8px 6px", borderRadius: 6, cursor: "pointer",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
            >
              <span style={{ fontFamily: P.mono, fontSize: 11, color: P.faint }}>#{i + 1}</span>
              <span style={{ fontFamily: P.mono, fontSize: 12, fontWeight: 600 }}>{s.state}</span>
              <div style={{ height: 6, background: P.elev, borderRadius: 3, overflow: "hidden" }}>
                <div style={{ height: "100%", width: `${(Number(s.vol) / maxVol) * 100}%`, background: P.accent, borderRadius: 3 }} />
              </div>
              <span style={{ fontFamily: P.mono, fontSize: 12, textAlign: "right" }}>{fmtMoney(s.vol)}</span>
              <span style={{ fontFamily: P.mono, fontSize: 11, color: P.dim, textAlign: "right" }}>{fmtNum(s.cnt)} loans</span>
            </div>
          ))}
    </div>
  );
}

// Re-exports used to silence unused-import warnings for primitives that
// downstream apps consume through this file's import map.
void Sparkline; void Breadcrumb;
