import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  DateRangeFilter,
  DEFAULT_DATE_RANGE_PRESETS,
  type DateRangeValue,
  ErrorState,
  LoadingState,
  useDataset,
  useJsonResource,
  useSqlQuery,
  useTheme,
} from "./definite-runtime";

// ── Palette (dark/light) — ported from the Refined SaaS v2 design ─────────
type Palette = ReturnType<typeof buildPalette>;
function buildPalette(theme: "dark" | "light") {
  return theme === "dark"
    ? {
        bg: "#09090b", sidebar: "#0a0a0c", card: "#111113", elev: "#16161a",
        text: "#f4f4f5", sub: "#a1a1aa", dim: "#71717a", faint: "#52525b",
        border: "#1f1f23", rule: "#27272a",
        accent: "#0A99FF", accentSoft: "rgba(10,153,255,0.14)",
        ok: "#10b981", okSoft: "rgba(16,185,129,0.12)",
        warn: "#f59e0b", warnSoft: "rgba(245,158,11,0.12)",
        bad: "#ef4444", badSoft: "rgba(239,68,68,0.12)",
        grad2: "#8B5CF6",
      }
    : {
        bg: "#fafafa", sidebar: "#ffffff", card: "#ffffff", elev: "#f4f4f5",
        text: "#09090b", sub: "#52525b", dim: "#71717a", faint: "#a1a1aa",
        border: "#e4e4e7", rule: "#e4e4e7",
        accent: "#0A84D0", accentSoft: "rgba(10,132,208,0.10)",
        ok: "#059669", okSoft: "rgba(5,150,105,0.10)",
        warn: "#d97706", warnSoft: "rgba(217,119,6,0.10)",
        bad: "#dc2626", badSoft: "rgba(220,38,38,0.10)",
        grad2: "#7C3AED",
      };
}

const SANS = '"Inter", system-ui, sans-serif';
const MONO = '"JetBrains Mono", ui-monospace, monospace';

// ── SQL helpers ───────────────────────────────────────────────────────────
const esc = (v: string) => v.replace(/'/g, "''");

type Filters = Record<string, string[]>;

function buildWhere(filters: Filters, from: string, to: string): string {
  const clauses: string[] = [];
  if (from) clauses.push(`originated >= '${esc(from)}'`);
  if (to)   clauses.push(`originated <= '${esc(to)}'`);
  for (const [col, vals] of Object.entries(filters)) {
    if (!vals || vals.length === 0) continue;
    const inList = vals.map((v) => `'${esc(v)}'`).join(",");
    clauses.push(`${col} IN (${inList})`);
  }
  return clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
}

// Money / number formatters
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

// Status color lookup
const statusTone = (status: string, P: Palette) => {
  if (status === "current") return P.ok;
  if (status === "paid_off") return P.dim;
  if (status === "late_30" || status === "late_60") return P.warn;
  return P.bad;
};
const statusSoft = (status: string, P: Palette) => {
  if (status === "current") return P.okSoft;
  if (status === "paid_off") return P.elev;
  if (status === "late_30" || status === "late_60") return P.warnSoft;
  return P.badSoft;
};

// ── Filter definitions (column -> label + sort order) ─────────────────────
type FilterGroup = {
  id: string;        // column name in duckdb
  label: string;
  searchable?: boolean;
  format?: (v: string) => string;
};
const FILTER_GROUPS: FilterGroup[] = [
  { id: "ficoBand",        label: "Risk band" },
  { id: "status",          label: "Loan status", format: (v) => v.replace(/_/g, " ") },
  { id: "state",           label: "State", searchable: true },
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
const INCOME_LABELS: Record<string, string> = {
  lt50: "< $50K", "50_75": "$50K–$75K", "75_100": "$75K–$100K",
  "100_150": "$100K–$150K", "150_200": "$150K–$200K", gt200: "$200K+",
};
const DTI_LABELS: Record<string, string> = {
  lt20: "Under 20%", "20_30": "20–30%", "30_40": "30–40%", gt40: "40%+",
};
const FICO_SWATCH: Record<string, string> = {
  A: "#10b981", B: "#84cc16", C: "#f59e0b", D: "#f97316", E: "#ef4444",
};
const STATUS_SWATCH: Record<string, string> = {
  current: "#10b981", late_30: "#f59e0b", late_60: "#f59e0b",
  late_90: "#ef4444", paid_off: "#71717a", charged_off: "#ef4444",
};

function humanize(groupId: string, value: string): string {
  if (groupId === "incomeBand") return INCOME_LABELS[value] ?? value;
  if (groupId === "dtiBand") return DTI_LABELS[value] ?? value;
  if (groupId === "ficoBand") return `Band ${value}`;
  const grp = FILTER_GROUPS.find((g) => g.id === groupId);
  return grp?.format ? grp.format(value) : value;
}
function swatchFor(groupId: string, value: string): string | null {
  if (groupId === "ficoBand") return FICO_SWATCH[value] ?? null;
  if (groupId === "status") return STATUS_SWATCH[value] ?? null;
  return null;
}

// Date range: the runtime's DateRangeValue / DateRangePreset + DateRangeFilter
// provide presets + relative/custom tabs + nice calendar. We use those directly.
type DateRange = DateRangeValue;
// Default: Last 12 months. The preview parquet's anchor is end-of-2026 so the
// trailing-12-month window has dense data in both preview and live builds.
function initialDateRange(): DateRange {
  const preset =
    DEFAULT_DATE_RANGE_PRESETS.find((p) => p.key === "last12m") ??
    DEFAULT_DATE_RANGE_PRESETS[0];
  return preset.compute();
}

// ── Tiny SVG primitives ───────────────────────────────────────────────────
function Sparkline({ vals, color, w = 90, h = 32 }: { vals: number[]; color: string; w?: number; h?: number }) {
  if (!vals.length) return <svg width={w} height={h} />;
  const mx = Math.max(...vals), mn = Math.min(...vals);
  const rng = mx - mn || 1;
  const pts = vals
    .map((v, i) => `${(i / (vals.length - 1)) * w},${h - ((v - mn) / rng) * h}`)
    .join(" ");
  const lastX = w;
  const lastY = h - ((vals[vals.length - 1] - mn) / rng) * h;
  return (
    <svg width={w} height={h} style={{ display: "block", overflow: "visible" }}>
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={lastX} cy={lastY} r="2.5" fill={color} />
    </svg>
  );
}

function SkeletonShimmer({ P, w = "100%", h = 16, r = 4 }: { P: Palette; w?: number | string; h?: number; r?: number }) {
  return (
    <div
      style={{
        width: w, height: h, borderRadius: r,
        background: `linear-gradient(90deg, ${P.elev} 0%, ${P.border} 50%, ${P.elev} 100%)`,
        backgroundSize: "200% 100%",
        animation: "skeletonShimmer 1.4s ease-in-out infinite",
      }}
    />
  );
}

// ── Drill drawer ──────────────────────────────────────────────────────────
type DrillEntity = {
  kind: "kpi" | "row" | "chart";
  id: string;
  title: string;
  value?: string;
  subvalue?: string;
  breadcrumb?: string;
  sql?: string;
  stats?: Array<[string, string]>;
  narrative?: string;
  breakdown?: Array<{ label: string; value: number }>;
};

function DrillDrawer({ P, entity, onClose }: { P: Palette; entity: DrillEntity | null; onClose: () => void }) {
  useEffect(() => {
    if (!entity) return;
    document.body.style.overflow = "hidden";
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => {
      document.body.style.overflow = "";
      window.removeEventListener("keydown", onKey);
    };
  }, [entity, onClose]);

  if (!entity) return null;
  const maxBar = entity.breakdown ? Math.max(...entity.breakdown.map((b) => Math.abs(b.value))) || 1 : 1;

  return (
    <>
      <div onClick={onClose} style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)",
        zIndex: 1500, backdropFilter: "blur(4px)", animation: "sdFade 0.18s ease-out",
      }} />
      <div style={{
        position: "fixed", top: 0, right: 0, bottom: 0, width: 520, maxWidth: "92vw",
        background: P.sidebar, zIndex: 1501, color: P.text,
        boxShadow: "-24px 0 60px rgba(0,0,0,0.35)",
        display: "flex", flexDirection: "column", fontFamily: SANS,
        borderLeft: `1px solid ${P.border}`, animation: "sdSlide 0.22s ease-out",
      }}>
        <div style={{ padding: "20px 24px 18px", borderBottom: `1px solid ${P.border}` }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, fontSize: 11, color: P.dim }}>
                <span style={{ fontFamily: MONO, letterSpacing: "0.1em", textTransform: "uppercase", color: P.accent }}>
                  {entity.kind === "kpi" ? "KPI" : entity.kind === "chart" ? "Chart" : "Row"}
                </span>
                <span>·</span>
                <span>{entity.breadcrumb || "Portfolio"}</span>
              </div>
              <div style={{ fontSize: 18, fontWeight: 600, letterSpacing: "-0.01em", lineHeight: 1.3 }}>{entity.title}</div>
              {entity.value && (
                <div style={{ fontSize: 32, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 10, color: P.accent }}>
                  {entity.value}
                </div>
              )}
              {entity.subvalue && (
                <div style={{ fontSize: 12, color: P.sub, marginTop: 4, fontFamily: MONO }}>{entity.subvalue}</div>
              )}
            </div>
            <button onClick={onClose} style={{
              background: P.elev, border: `1px solid ${P.border}`, borderRadius: 6,
              padding: "4px 10px", color: P.sub, cursor: "pointer", fontSize: 12, fontFamily: SANS,
            }}>ESC</button>
          </div>
        </div>
        <div style={{ flex: 1, overflowY: "auto", padding: "18px 24px" }}>
          {entity.narrative && (
            <div style={{ fontSize: 13, color: P.sub, lineHeight: 1.55, marginBottom: 20 }}>{entity.narrative}</div>
          )}
          {entity.stats && entity.stats.length > 0 && (
            <div style={{ marginBottom: 22 }}>
              <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 8, fontWeight: 600 }}>
                Computed
              </div>
              {entity.stats.map(([k, v]) => (
                <div key={k} style={{
                  display: "flex", justifyContent: "space-between", alignItems: "baseline",
                  padding: "7px 0", borderBottom: `1px solid ${P.border}`, fontSize: 12,
                }}>
                  <span style={{ color: P.sub }}>{k}</span>
                  <span style={{ fontFamily: MONO, color: P.text }}>{v}</span>
                </div>
              ))}
            </div>
          )}
          {entity.breakdown && entity.breakdown.length > 0 && (
            <div style={{ marginBottom: 22 }}>
              <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 10, fontWeight: 600 }}>
                Breakdown
              </div>
              {entity.breakdown.map((b) => (
                <div key={b.label} style={{ marginBottom: 8 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 3 }}>
                    <span style={{ color: P.sub }}>{b.label}</span>
                    <span style={{ fontFamily: MONO, color: P.text }}>{fmtMoney(b.value)}</span>
                  </div>
                  <div style={{ height: 4, background: P.elev, borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ height: "100%", width: `${(Math.abs(b.value) / maxBar) * 100}%`, background: P.accent, borderRadius: 2 }} />
                  </div>
                </div>
              ))}
            </div>
          )}
          {entity.sql && (
            <div>
              <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 8, fontWeight: 600 }}>
                SQL
              </div>
              <pre style={{
                margin: 0, padding: 12, background: P.elev, border: `1px solid ${P.border}`,
                borderRadius: 6, fontSize: 11, fontFamily: MONO, color: P.sub,
                whiteSpace: "pre-wrap", lineHeight: 1.5,
              }}>{entity.sql}</pre>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

// ── KPI card ──────────────────────────────────────────────────────────────
function KpiCardRv2({ P, title, value, delta, up, sub, spark, accent, onClick, loading }: {
  P: Palette; title: string; value: string; delta: string; up: boolean;
  sub: string; spark: number[]; accent: string;
  onClick?: () => void; loading?: boolean;
}) {
  const cardStyle: React.CSSProperties = {
    background: P.card, border: `1px solid ${P.border}`, borderRadius: 10,
    padding: 16, position: "relative", overflow: "hidden", cursor: onClick ? "pointer" : "default",
    transition: "transform 0.15s, border-color 0.15s",
  };
  return (
    <div
      onClick={onClick}
      onMouseEnter={(e) => { if (onClick) { e.currentTarget.style.transform = "translateY(-1px)"; e.currentTarget.style.borderColor = P.rule; } }}
      onMouseLeave={(e) => { e.currentTarget.style.transform = "none"; e.currentTarget.style.borderColor = P.border; }}
      style={cardStyle}
    >
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: `linear-gradient(90deg, ${accent}, transparent)` }} />
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
        <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>{title}</div>
        {!loading && spark.length > 0 && <Sparkline vals={spark} color={accent} />}
      </div>
      {loading ? (
        <>
          <SkeletonShimmer P={P} w={120} h={28} r={6} />
          <div style={{ height: 10 }} />
          <SkeletonShimmer P={P} w={140} h={12} />
        </>
      ) : (
        <>
          <div style={{ fontSize: 28, fontWeight: 600, letterSpacing: "-0.02em", lineHeight: 1, marginBottom: 8 }}>{value}</div>
          <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
            <span style={{ color: up ? P.ok : P.bad, background: up ? P.okSoft : P.badSoft, padding: "2px 7px", borderRadius: 4, fontFamily: MONO, fontWeight: 500 }}>
              {up ? "↑" : "↓"} {delta}
            </span>
            <span style={{ color: P.dim }}>{sub}</span>
          </div>
        </>
      )}
    </div>
  );
}

// (Date-range trigger now comes from the runtime: DateRangeFilter with
// popoverPlacement="right-start" so the 560px popover flies out of the sidebar
// into the main content area.)

// ── Cache popover ─────────────────────────────────────────────────────────
function CachePopover({ P, isLoading, rowCount, cache, onRefresh }: {
  P: Palette; isLoading: boolean; rowCount: number | null;
  cache: { loadTimeMs: number | null; fromCache: boolean; sourceLabel: string; cacheTtlHours: number } | null | undefined;
  onRefresh: () => Promise<void> | void;
}) {
  const [open, setOpen] = useState(false);
  const loadSec = cache?.loadTimeMs != null ? (cache.loadTimeMs / 1000).toFixed(2) + "s" : "—";
  return (
    <div style={{ position: "relative" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          display: "flex", alignItems: "center", gap: 6, fontSize: 11, padding: "6px 10px",
          background: isLoading ? P.accentSoft : P.okSoft,
          color: isLoading ? P.accent : P.ok,
          borderRadius: 6, fontFamily: MONO,
          border: `1px solid ${open ? (isLoading ? P.accent : P.ok) : "transparent"}`,
          cursor: "pointer",
        }}
      >
        <span style={{
          width: 6, height: 6, borderRadius: "50%",
          background: isLoading ? P.accent : P.ok,
          boxShadow: `0 0 8px ${isLoading ? P.accent : P.ok}`,
          animation: isLoading ? "sdPulse 0.9s ease-in-out infinite" : "none",
        }} />
        {isLoading ? "Loading…" : cache?.fromCache ? "Cached" : "Live"}
        <span style={{ marginLeft: 2, opacity: 0.7, fontSize: 9 }}>▾</span>
      </button>
      {open && (
        <>
          <div onClick={() => setOpen(false)} style={{ position: "fixed", inset: 0, zIndex: 40 }} />
          <div style={{
            position: "absolute", top: "calc(100% + 6px)", right: 0, zIndex: 50,
            width: 320, background: P.card, border: `1px solid ${P.border}`, borderRadius: 10,
            boxShadow: "0 20px 60px rgba(0,0,0,0.35)", padding: 14, fontFamily: SANS, color: P.text,
          }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ width: 7, height: 7, borderRadius: "50%", background: P.ok, boxShadow: `0 0 10px ${P.ok}` }} />
                <div style={{ fontSize: 13, fontWeight: 600 }}>DuckDB WASM</div>
              </div>
              <div style={{ fontSize: 10, color: P.dim, fontFamily: MONO }}>v1.29.0</div>
            </div>
            <div style={{ fontSize: 11, color: P.sub, lineHeight: 1.5, marginBottom: 12 }}>
              Query results cached in-browser via IndexedDB. Subsequent loads of the same slice render
              instantly without hitting the warehouse.
            </div>
            {[
              ["Rows cached", rowCount != null ? rowCount.toLocaleString() : "—"],
              ["Source", cache?.sourceLabel ?? "—"],
              ["Load time", loadSec],
              ["From cache", cache?.fromCache ? "yes" : "no"],
              ["TTL", `${cache?.cacheTtlHours ?? 24}h`],
            ].map(([k, v]) => (
              <div key={k} style={{
                display: "flex", justifyContent: "space-between", alignItems: "baseline",
                padding: "6px 0", borderTop: `1px solid ${P.border}`,
              }}>
                <div style={{ fontSize: 11, color: P.sub }}>{k}</div>
                <div style={{ fontSize: 12, fontWeight: 500, fontFamily: MONO }}>{v}</div>
              </div>
            ))}
            <button
              onClick={async () => { await onRefresh(); setOpen(false); }}
              style={{
                width: "100%", marginTop: 12, fontSize: 11, padding: "6px 10px", borderRadius: 5,
                background: P.accent, border: `1px solid ${P.accent}`, color: "#fff",
                cursor: "pointer", fontFamily: SANS, fontWeight: 500,
              }}
            >
              Clear cache & reload
            </button>
          </div>
        </>
      )}
    </div>
  );
}

// ── Sidebar: filters accordion ────────────────────────────────────────────
type FilterOption = { id: string; label: string; hint?: string; swatch?: string };
type FilterGroupData = FilterGroup & { options: FilterOption[] };

function FiltersPanel({
  P, groups, filters, setFilters, search, setSearch,
}: {
  P: Palette;
  groups: FilterGroupData[];
  filters: Filters;
  setFilters: React.Dispatch<React.SetStateAction<Filters>>;
  search: string;
  setSearch: (s: string) => void;
}) {
  const [expanded, setExpanded] = useState<Record<string, boolean>>({ ficoBand: true });
  const [groupSearch, setGroupSearch] = useState<Record<string, string>>({});

  const toggle = (gid: string, optId: string) => {
    setFilters((f) => {
      const cur = f[gid] || [];
      const next = cur.includes(optId) ? cur.filter((x) => x !== optId) : [...cur, optId];
      const copy = { ...f };
      if (next.length === 0) delete copy[gid]; else copy[gid] = next;
      return copy;
    });
  };
  const clearGroup = (gid: string) => setFilters((f) => { const c = { ...f }; delete c[gid]; return c; });

  const q = search.toLowerCase();

  return (
    <>
      <div style={{ padding: "0 10px 8px", position: "relative" }}>
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search filters…"
          style={{
            width: "100%", padding: "6px 10px 6px 26px", fontSize: 11,
            background: P.elev, border: `1px solid ${P.border}`, color: P.text,
            borderRadius: 5, outline: "none", fontFamily: SANS,
          }}
        />
        <span style={{ position: "absolute", left: 19, top: 6, fontSize: 11, color: P.faint, pointerEvents: "none" }}>⌕</span>
      </div>
      <div style={{ flex: 1, overflowY: "auto", marginRight: -4, paddingRight: 4 }}>
        {groups.map((grp) => {
          const selectedIds = filters[grp.id] || [];
          const isOpen = expanded[grp.id] || selectedIds.length > 0 || q.length > 0 || (groupSearch[grp.id] || "").length > 0;
          const matches = q
            ? grp.options.filter((o) => o.label.toLowerCase().includes(q) || grp.label.toLowerCase().includes(q))
            : grp.options;
          if (q && matches.length === 0 && !grp.label.toLowerCase().includes(q)) return null;
          const globalFiltered = q && !grp.label.toLowerCase().includes(q) ? matches : grp.options;
          const hasGroupSearch = grp.options.length >= 8;
          const gq = (groupSearch[grp.id] || "").toLowerCase();
          const visible = gq
            ? globalFiltered.filter((o) => o.label.toLowerCase().includes(gq))
            : globalFiltered;

          return (
            <div key={grp.id} style={{ marginBottom: 2 }}>
              <button
                onClick={() => setExpanded((e) => ({ ...e, [grp.id]: !e[grp.id] }))}
                style={{
                  width: "100%", display: "flex", alignItems: "center",
                  padding: "6px 10px", background: "transparent",
                  border: "none", cursor: "pointer", color: P.sub, textAlign: "left",
                  fontSize: 12, fontFamily: SANS, borderRadius: 5, gap: 8,
                }}
                onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
                onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
              >
                <span style={{
                  fontSize: 9, color: P.faint, width: 8, display: "inline-block",
                  transform: isOpen ? "rotate(90deg)" : "none", transition: "transform 0.15s",
                }}>▶</span>
                <span style={{ flex: 1, fontWeight: 500, color: P.text }}>{grp.label}</span>
                {selectedIds.length > 0 ? (
                  <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 8, background: P.accent, color: "#fff", fontFamily: MONO, fontWeight: 600 }}>
                    {selectedIds.length}
                  </span>
                ) : (
                  <span style={{ fontSize: 10, color: P.faint, fontFamily: MONO }}>{grp.options.length}</span>
                )}
              </button>
              {isOpen && (
                <div style={{
                  padding: "2px 6px 8px 22px",
                  maxHeight: visible.length > 7 ? 200 : "auto",
                  overflowY: visible.length > 7 ? "auto" : "visible",
                }}>
                  {hasGroupSearch && (
                    <div style={{ position: "relative", marginBottom: 4, marginTop: 2 }}>
                      <input
                        value={groupSearch[grp.id] || ""}
                        onChange={(e) => setGroupSearch((s) => ({ ...s, [grp.id]: e.target.value }))}
                        placeholder={`Search ${grp.label.toLowerCase()}…`}
                        style={{
                          width: "100%", padding: "4px 22px 4px 22px", fontSize: 11,
                          background: P.elev, border: `1px solid ${P.border}`, color: P.text,
                          borderRadius: 4, outline: "none", fontFamily: SANS,
                        }}
                      />
                      <span style={{ position: "absolute", left: 7, top: 4, fontSize: 10, color: P.faint, pointerEvents: "none" }}>⌕</span>
                    </div>
                  )}
                  {selectedIds.length > 0 && (
                    <button onClick={() => clearGroup(grp.id)} style={{
                      fontSize: 10, color: P.dim, background: "none", border: "none",
                      cursor: "pointer", padding: "2px 4px", marginBottom: 2, fontFamily: SANS,
                    }}>Clear {grp.label.toLowerCase()}</button>
                  )}
                  {visible.length === 0 && (
                    <div style={{ fontSize: 11, color: P.faint, padding: "6px 4px", fontStyle: "italic" }}>No matches</div>
                  )}
                  {visible.map((opt) => {
                    const checked = selectedIds.includes(opt.id);
                    return (
                      <label key={opt.id} style={{
                        display: "flex", alignItems: "center", gap: 8,
                        padding: "4px 6px", borderRadius: 4, cursor: "pointer", fontSize: 12,
                        color: checked ? P.text : P.sub,
                        background: checked ? P.accentSoft : "transparent",
                      }}
                        onMouseEnter={(e) => { if (!checked) e.currentTarget.style.background = P.elev; }}
                        onMouseLeave={(e) => { if (!checked) e.currentTarget.style.background = "transparent"; }}
                      >
                        <span style={{
                          width: 13, height: 13, borderRadius: 3,
                          border: `1px solid ${checked ? P.accent : P.border}`,
                          background: checked ? P.accent : "transparent",
                          display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                        }}>
                          {checked && <span style={{ color: "#fff", fontSize: 9, lineHeight: 1, fontWeight: 700 }}>✓</span>}
                        </span>
                        <input type="checkbox" checked={checked} onChange={() => toggle(grp.id, opt.id)} style={{ display: "none" }} />
                        {opt.swatch && <span style={{ width: 7, height: 7, borderRadius: 2, background: opt.swatch, flexShrink: 0 }} />}
                        <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {opt.label}
                        </span>
                        {opt.hint != null && (
                          <span style={{ fontFamily: MONO, fontSize: 10, color: P.faint, flexShrink: 0 }}>{opt.hint}</span>
                        )}
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </>
  );
}

// ── App ───────────────────────────────────────────────────────────────────
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

const NAV_ITEMS: Array<{ id: string; label: string; icon: string }> = [
  { id: "overview",     label: "Overview",     icon: "◧" },
  { id: "loans",        label: "Loans",        icon: "≣" },
  { id: "risk",         label: "Risk",         icon: "◎" },
  { id: "originations", label: "Originations", icon: "↗" },
  { id: "delinquency",  label: "Delinquency",  icon: "⚠" },
  { id: "geography",    label: "Geography",    icon: "◔" },
];

export default function App() {
  const { theme, toggleTheme } = useTheme();
  const P = useMemo(() => buildPalette(theme), [theme]);
  const data = useDataset("loans");
  const riskBands = useJsonResource<Array<{ band: string; range: string; apr: number; defaultRate: number; color: string }>>("riskBands");

  const [view, setView] = useState("overview");
  const [dateRange, setDateRange] = useState<DateRange>(initialDateRange);
  const [filters, setFilters] = useState<Filters>({});
  const [filterSearch, setFilterSearch] = useState("");
  const [drill, setDrill] = useState<DrillEntity | null>(null);
  const [sortKey, setSortKey] = useState<string>("originated");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [loanSearch, setLoanSearch] = useState("");

  const where = useMemo(() => buildWhere(filters, dateRange.from, dateRange.to), [filters, dateRange.from, dateRange.to]);
  const t = data.tableRef;

  // Dim counts (for filter hints) — one query per expanded-enough shape; we just load all since 2,588 rows
  const dimCounts = useSqlQuery<DimRow[]>(
    data,
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

  // Build filter groups from observed values
  const filterGroupsData: FilterGroupData[] = useMemo(() => {
    const rows = dimCounts.data || [];
    const byGroup: Record<string, FilterOption[]> = {};
    for (const r of rows) {
      const [gid, val] = String(r.id).split("||");
      if (!gid || !val) continue;
      (byGroup[gid] ||= []).push({
        id: val,
        label: humanize(gid, val),
        hint: (r.cnt as number).toLocaleString(),
        swatch: swatchFor(gid, val) || undefined,
      });
    }
    return FILTER_GROUPS.map((g) => ({
      ...g,
      options: (byGroup[g.id] || []).sort((a, b) => {
        // Preserve a sensible order for known bands
        if (g.id === "ficoBand") return a.id.localeCompare(b.id);
        if (g.id === "status") {
          const order = ["current", "late_30", "late_60", "late_90", "paid_off", "charged_off"];
          return order.indexOf(a.id) - order.indexOf(b.id);
        }
        if (g.id === "incomeBand") {
          const order = ["lt50", "50_75", "75_100", "100_150", "150_200", "gt200"];
          return order.indexOf(a.id) - order.indexOf(b.id);
        }
        if (g.id === "dtiBand") {
          const order = ["lt20", "20_30", "30_40", "gt40"];
          return order.indexOf(a.id) - order.indexOf(b.id);
        }
        if (g.id === "term") return Number(a.id) - Number(b.id);
        return (Number(b.hint?.replace(/,/g, "") || 0)) - (Number(a.hint?.replace(/,/g, "") || 0));
      }),
    }));
  }, [dimCounts.data]);

  // KPIs — respect filters + date range
  const kpis = useSqlQuery<KpiRow[]>(
    data,
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

  // Origination timeseries (last 12 months)
  const monthly = useSqlQuery<MonthRow[]>(
    data,
    t ? `
      SELECT originatedMonth, SUM(amount)::BIGINT AS vol, COUNT(*)::INTEGER AS cnt
      FROM ${t}${where}
      GROUP BY 1 ORDER BY 1
    ` : "",
    [where],
  );

  // Status breakdown
  const statusAgg = useSqlQuery<StatusRow[]>(
    data,
    t ? `
      SELECT status, COUNT(*)::INTEGER AS cnt, SUM(balance)::BIGINT AS vol
      FROM ${t}${where}
      GROUP BY 1
    ` : "",
    [where],
  );

  // Band breakdown
  const bandAgg = useSqlQuery<BandRow[]>(
    data,
    t ? `
      SELECT ficoBand, COUNT(*)::INTEGER AS cnt, SUM(balance)::BIGINT AS vol, ROUND(AVG(rate), 2) AS avgRate
      FROM ${t}${where}
      GROUP BY 1 ORDER BY 1
    ` : "",
    [where],
  );

  // State breakdown
  const stateAgg = useSqlQuery<StateRow[]>(
    data,
    t ? `
      SELECT state, COUNT(*)::INTEGER AS cnt, SUM(balance)::BIGINT AS vol
      FROM ${t}${where}
      GROUP BY 1 ORDER BY vol DESC LIMIT 8
    ` : "",
    [where],
  );

  // Recent originations
  const recent = useSqlQuery<RecentRow[]>(
    data,
    t ? `
      SELECT loanId, borrower, amount, fico, status, state, originated
      FROM ${t}${where}
      ORDER BY originated DESC LIMIT 8
    ` : "",
    [where],
  );

  // Loans table — server-sort, limit 500 rows in view; client search on borrower/id
  const loansSql = useMemo(() => {
    if (!t) return "";
    let clause = where;
    if (loanSearch.trim()) {
      const esc1 = esc(loanSearch.trim());
      const extra = `(borrower ILIKE '%${esc1}%' OR loanId ILIKE '%${esc1}%')`;
      clause = where ? `${where} AND ${extra}` : ` WHERE ${extra}`;
    }
    const dir = sortDir.toUpperCase();
    return `
      SELECT loanId, borrower, amount, balance, fico, ficoBand, rate, term, status, state, vintage, originated
      FROM ${t}${clause}
      ORDER BY ${sortKey} ${dir}
      LIMIT 500
    `;
  }, [t, where, loanSearch, sortKey, sortDir]);
  const loansTable = useSqlQuery<Array<Record<string, unknown>>>(data, loansSql, [loansSql]);

  // ── Loading gate ──
  if (data.loading) return <LoadingState message="Loading loan book…" />;
  if (data.error) return <ErrorState title="Dataset failed to load" message={data.error} />;

  const loading = kpis.loading || monthly.loading || statusAgg.loading || bandAgg.loading || stateAgg.loading || recent.loading;
  const k0 = kpis.data?.[0];
  const activeFilterCount = Object.values(filters).reduce((a, b) => a + b.length, 0);

  // Per-view breadcrumb
  const navItem = NAV_ITEMS.find((n) => n.id === view) ?? NAV_ITEMS[0];

  // Sparkline shared values (use cnt over time)
  const sparkVals = (monthly.data || []).map((m) => Number(m.cnt) || 0);
  const lastMonthVol = (monthly.data || []).slice(-1)[0]?.vol ?? 0;
  const maxMonthVol = Math.max(1, ...((monthly.data || []).map((m) => Number(m.vol) || 0)));

  const openDrill = (e: DrillEntity) => setDrill(e);

  return (
    <div style={{ background: P.bg, color: P.text, fontFamily: SANS, fontSize: 14, minHeight: "100vh", display: "flex" }}>
      <style>{`
        @keyframes skeletonShimmer {
          0% { background-position: 200% 0; }
          100% { background-position: -200% 0; }
        }
        @keyframes sdPulse { 0%, 100% { opacity: 0.3; } 50% { opacity: 1; } }
        @keyframes sdFade { from { opacity: 0 } to { opacity: 1 } }
        @keyframes sdSlide { from { transform: translateX(100%) } to { transform: translateX(0) } }
        ::-webkit-scrollbar { width: 10px; height: 10px; }
        ::-webkit-scrollbar-thumb { background: ${P.border}; border-radius: 5px; }
        ::-webkit-scrollbar-track { background: transparent; }
        * { box-sizing: border-box; }
      `}</style>

      {/* Sidebar */}
      <div style={{
        width: 240, background: P.sidebar, borderRight: `1px solid ${P.border}`,
        padding: "20px 10px", display: "flex", flexDirection: "column", gap: 2,
        position: "sticky", top: 0, height: "100vh", zIndex: 50,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 8px 20px" }}>
          <div style={{
            width: 28, height: 28, borderRadius: 7,
            background: `linear-gradient(135deg, ${P.accent}, ${P.grad2})`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 13, fontWeight: 700, color: "#fff",
          }}>◆</div>
          <div>
            <div style={{ fontSize: 13, fontWeight: 600 }}>Loan Portfolio</div>
            <div style={{ fontSize: 11, color: P.dim }}>Fintech · Production</div>
          </div>
        </div>
        <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", padding: "8px 10px 6px", fontWeight: 600 }}>
          Views
        </div>
        {NAV_ITEMS.map((n) => {
          const active = view === n.id;
          const badge = n.id === "loans" ? (k0?.totalCount ?? null) : null;
          return (
            <button key={n.id} onClick={() => setView(n.id)} style={{
              display: "flex", alignItems: "center", gap: 10, textAlign: "left",
              padding: "8px 10px", borderRadius: 6,
              background: active ? P.accentSoft : "transparent",
              color: active ? P.accent : P.sub,
              border: "none", cursor: "pointer", fontSize: 13, fontWeight: active ? 500 : 400, fontFamily: SANS,
            }}
              onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = P.elev; }}
              onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = "transparent"; }}
            >
              <span style={{ width: 14, textAlign: "center", opacity: 0.8 }}>{n.icon}</span>
              <span style={{ flex: 1 }}>{n.label}</span>
              {badge != null && (
                <span style={{
                  fontSize: 10, padding: "1px 6px", borderRadius: 8,
                  background: active ? P.accent + "30" : P.elev,
                  color: active ? P.accent : P.dim,
                  fontFamily: MONO, fontWeight: 500,
                }}>{badge.toLocaleString()}</span>
              )}
            </button>
          );
        })}

        {/* Date range — uses the runtime's DateRangeFilter (presets + relative +
            custom tabs, 560px popover). popoverPlacement="right-start" escapes the
            sidebar and anchors the popover next to the trigger in the main area. */}
        <div style={{ padding: "14px 10px 4px" }}>
          <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", fontWeight: 600, marginBottom: 6 }}>
            Date range
          </div>
          <DateRangeFilter
            value={dateRange}
            onChange={setDateRange}
            label={null}
            popoverPlacement="right-start"
            triggerStyle={{
              width: "100%",
              minWidth: 0,
              justifyContent: "space-between",
              padding: "7px 10px",
              fontSize: 12,
            }}
          />
        </div>

        {/* Filters header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "20px 10px 6px" }}>
          <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", fontWeight: 600 }}>
            Filters {activeFilterCount > 0 && <span style={{ color: P.accent, marginLeft: 4 }}>· {activeFilterCount}</span>}
          </div>
          {activeFilterCount > 0 && (
            <button onClick={() => setFilters({})} style={{
              fontSize: 10, color: P.dim, background: "none", border: "none", cursor: "pointer",
              fontFamily: SANS, padding: "2px 4px",
            }}>Clear</button>
          )}
        </div>
        {activeFilterCount > 0 && (
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4, padding: "0 10px 8px" }}>
            {Object.entries(filters).map(([gid, opts]) => {
              const grp = filterGroupsData.find((g) => g.id === gid);
              if (!grp || !opts?.length) return null;
              const single = opts.length === 1;
              const label = single ? humanize(gid, opts[0]) : `${grp.label} · ${opts.length}`;
              return (
                <button key={gid}
                  onClick={() => setFilters((f) => { const c = { ...f }; delete c[gid]; return c; })}
                  style={{
                    display: "inline-flex", alignItems: "center", gap: 5,
                    padding: "3px 6px 3px 7px", borderRadius: 10, fontSize: 10,
                    background: P.accentSoft, color: P.accent,
                    border: `1px solid ${P.accent}30`, cursor: "pointer",
                    lineHeight: 1.4, maxWidth: "100%",
                  }}
                >
                  <span style={{ fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{label}</span>
                  <span style={{ fontSize: 11, opacity: 0.7 }}>×</span>
                </button>
              );
            })}
          </div>
        )}

        <FiltersPanel
          P={P}
          groups={filterGroupsData}
          filters={filters}
          setFilters={setFilters}
          search={filterSearch}
          setSearch={setFilterSearch}
        />

        <div style={{ marginTop: "auto", paddingTop: 14, borderTop: `1px solid ${P.border}`, display: "flex", flexDirection: "column", gap: 10 }}>
          <div style={{ display: "flex", gap: 3, background: P.elev, borderRadius: 6, padding: 3 }}>
            {(["dark", "light"] as const).map((tkey) => (
              <button key={tkey}
                onClick={() => { if (theme !== tkey) toggleTheme(); }}
                style={{
                  flex: 1, padding: "5px 8px", fontSize: 11, border: "none", borderRadius: 4, cursor: "pointer",
                  background: theme === tkey ? P.card : "transparent",
                  color: theme === tkey ? P.text : P.sub,
                  fontFamily: SANS, textTransform: "capitalize", fontWeight: theme === tkey ? 500 : 400,
                }}
              >{tkey === "dark" ? "◐ Dark" : "◑ Light"}</button>
            ))}
          </div>
          <div style={{ fontSize: 10, color: P.dim, fontFamily: MONO, lineHeight: 1.5, padding: "0 4px" }}>
            Live DuckDB · {(k0?.totalCount ?? 0).toLocaleString()} rows<br />
            Cached 24h TTL
          </div>
        </div>
      </div>

      {/* Main */}
      <div style={{ flex: 1, minWidth: 0, padding: "28px 36px 48px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 24 }}>
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
              <span style={{ fontSize: 12, color: P.dim }}>Portfolio</span>
              <span style={{ color: P.faint }}>/</span>
              <span style={{ fontSize: 12, color: P.text }}>{navItem.label}</span>
            </div>
            <h1 style={{ fontSize: 28, fontWeight: 600, letterSpacing: "-0.02em", margin: 0 }}>{navItem.label}</h1>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <CachePopover
              P={P}
              isLoading={loading}
              rowCount={data.cache?.rowCount ?? k0?.totalCount ?? null}
              cache={data.cache}
              onRefresh={data.refresh}
            />
            <button style={{
              fontSize: 12, padding: "6px 12px", borderRadius: 6,
              background: P.accent, border: `1px solid ${P.accent}`, color: "#fff",
              cursor: "pointer", fontWeight: 500,
            }}>Export</button>
          </div>
        </div>

        {view === "overview" && (
          <OverviewView
            P={P} loading={loading} k0={k0}
            monthly={monthly.data || []}
            statusAgg={statusAgg.data || []}
            bandAgg={bandAgg.data || []}
            recent={recent.data || []}
            sparkVals={sparkVals}
            dateRange={dateRange}
            openDrill={openDrill}
            where={where}
          />
        )}
        {view === "loans" && (
          <LoansView
            P={P}
            loading={loansTable.loading}
            rows={loansTable.data || []}
            sortKey={sortKey} sortDir={sortDir}
            onSort={(k) => { if (k === sortKey) setSortDir(sortDir === "asc" ? "desc" : "asc"); else { setSortKey(k); setSortDir("desc"); } }}
            search={loanSearch} setSearch={setLoanSearch}
            openDrill={openDrill}
          />
        )}
        {view === "risk" && (
          <RiskView
            P={P} loading={loading}
            bands={bandAgg.data || []}
            riskMeta={riskBands.data || []}
            openDrill={openDrill}
          />
        )}
        {view === "originations" && (
          <OriginationsView P={P} loading={monthly.loading} monthly={monthly.data || []} k0={k0} lastMonthVol={lastMonthVol} maxMonthVol={maxMonthVol} openDrill={openDrill} />
        )}
        {view === "delinquency" && (
          <DelinquencyView P={P} loading={loading} statusAgg={statusAgg.data || []} k0={k0} openDrill={openDrill} />
        )}
        {view === "geography" && (
          <GeographyView P={P} loading={stateAgg.loading} states={stateAgg.data || []} openDrill={openDrill} />
        )}
      </div>

      <DrillDrawer P={P} entity={drill} onClose={() => setDrill(null)} />
    </div>
  );
}

// ── Views ─────────────────────────────────────────────────────────────────
function OverviewView({ P, loading, k0, monthly, statusAgg, bandAgg, recent, sparkVals, dateRange, openDrill, where }: {
  P: Palette; loading: boolean; k0: KpiRow | undefined;
  monthly: MonthRow[]; statusAgg: StatusRow[]; bandAgg: BandRow[]; recent: RecentRow[];
  sparkVals: number[]; dateRange: DateRange;
  openDrill: (e: DrillEntity) => void; where: string;
}) {
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
      stats: [
        ["Active", fmtNum(k0?.activeCount ?? 0)],
        ["Total", fmtNum(k0?.totalCount ?? 0)],
      ] as Array<[string, string]>,
    },
    {
      id: "avg_principal", title: "Avg principal",
      value: fmtMoney(k0?.avgPrincipal ?? 0),
      delta: "+2.1%", up: true, sub: "trailing 90 days", accent: P.ok,
      stats: [["Avg", fmtMoneyFull(k0?.avgPrincipal ?? 0)]] as Array<[string, string]>,
    },
    {
      id: "delinquency", title: "90+ delinquent",
      value: fmtPct(pctDelinq, 2) + "",
      delta: "+0.12pp", up: false, sub: "vs. prior month", accent: P.warn,
      stats: [
        ["90+ DPD", fmtNum(k0?.delinquent90Count ?? 0)],
        ["Rate", fmtPct(pctDelinq, 2)],
      ] as Array<[string, string]>,
    },
  ];

  // Origination chart path (SVG, normalized)
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
          <KpiCardRv2
            key={k.id}
            P={P}
            title={k.title}
            value={k.value}
            delta={k.delta}
            up={k.up}
            sub={k.sub}
            spark={sparkVals.slice(-12)}
            accent={k.accent}
            loading={loading}
            onClick={() => openDrill({
              kind: "kpi", id: k.id, title: k.title, value: k.value,
              breadcrumb: "Overview", stats: k.stats,
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
          onClick={() => openDrill({
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
            <div style={{ fontSize: 11, color: P.sub, fontFamily: MONO }}>
              peak {peak ? fmtMoney(peak.vol) : "—"}
            </div>
          </div>
          <div style={{ height: 200, position: "relative" }}>
            {loading ? (
              <SkeletonShimmer P={P} w="100%" h={200} r={6} />
            ) : (
              <svg viewBox="0 0 100 100" preserveAspectRatio="none" style={{ width: "100%", height: "100%" }}>
                <defs>
                  <linearGradient id="rv2Grad" x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor={P.accent} stopOpacity="0.28" />
                    <stop offset="100%" stopColor={P.accent} stopOpacity="0" />
                  </linearGradient>
                </defs>
                {[25, 50, 75].map((y) => (
                  <line key={y} x1="0" y1={y} x2="100" y2={y} stroke={P.rule} strokeWidth="0.3" vectorEffect="non-scaling-stroke" />
                ))}
                {area && <path d={area} fill="url(#rv2Grad)" />}
                {path && <path d={path} stroke={P.accent} strokeWidth="0.6" fill="none" vectorEffect="non-scaling-stroke" />}
              </svg>
            )}
          </div>
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontFamily: MONO, fontSize: 10, color: P.faint }}>
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
                <div key={i} style={{ marginBottom: 10 }}><SkeletonShimmer P={P} w="100%" h={14} /></div>
              ))
            : statusAgg.map((s) => {
                const pct = (Number(s.vol) / totalStatus) * 100;
                const c = statusTone(s.status, P);
                return (
                  <div key={s.status}
                    onClick={() => openDrill({
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
                      <span style={{ fontFamily: MONO, fontSize: 11, color: P.sub }}>
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
            ? Array.from({ length: 5 }).map((_, i) => <div key={i} style={{ marginBottom: 8 }}><SkeletonShimmer P={P} w="100%" h={26} /></div>)
            : bandAgg.map((b) => {
                const pct = totalCount > 0 ? (Number(b.cnt) / totalCount) * 100 : 0;
                const color = FICO_SWATCH[b.ficoBand] || P.accent;
                return (
                  <div key={b.ficoBand}
                    onClick={() => openDrill({
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
                    <div style={{ textAlign: "right", fontFamily: MONO, fontSize: 12, color: P.text }}>{fmtNum(b.cnt)}</div>
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
                    <tr key={i}><td colSpan={5} style={{ padding: "10px 18px" }}><SkeletonShimmer P={P} w="100%" h={14} /></td></tr>
                  ))
                : recent.map((r) => {
                    const c = statusTone(r.status, P);
                    const bg = statusSoft(r.status, P);
                    return (
                      <tr key={r.loanId}
                        onClick={() => openDrill({
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
                        <td style={{ padding: "10px 18px", fontFamily: MONO, fontSize: 11, color: P.sub }}>{r.loanId}</td>
                        <td style={{ padding: "10px 12px" }}>{r.borrower}</td>
                        <td style={{ padding: "10px 12px", textAlign: "right", fontFamily: MONO, fontWeight: 500 }}>{fmtMoneyFull(r.amount)}</td>
                        <td style={{ padding: "10px 12px", textAlign: "right", fontFamily: MONO, color: r.fico < 650 ? P.bad : P.sub }}>{r.fico}</td>
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

function LoansView({ P, loading, rows, sortKey, sortDir, onSort, search, setSearch, openDrill }: {
  P: Palette; loading: boolean; rows: Array<Record<string, unknown>>;
  sortKey: string; sortDir: "asc" | "desc"; onSort: (k: string) => void;
  search: string; setSearch: (s: string) => void;
  openDrill: (e: DrillEntity) => void;
}) {
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
      const c = statusTone(s, P); const bg = statusSoft(s, P);
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
        <div style={{ display: "flex", gap: 6, fontSize: 11, color: P.dim, fontFamily: MONO }}>
          <input
            value={search} onChange={(e) => setSearch(e.target.value)}
            placeholder="Search borrower or loan id…"
            style={{
              padding: "6px 10px", fontSize: 12, background: P.elev, border: `1px solid ${P.border}`,
              color: P.text, borderRadius: 6, outline: "none", fontFamily: SANS, width: 260,
            }}
          />
        </div>
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
                  onClick={() => openDrill({
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
                  style={{
                    borderTop: `1px solid ${P.border}`, cursor: "pointer",
                    background: i % 2 === 0 ? "transparent" : "transparent",
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
                >
                  {cols.map((c) => (
                    <td key={c.key} style={{
                      padding: "8px 14px",
                      textAlign: c.align ?? "left",
                      fontFamily: c.mono ? MONO : SANS,
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

function RiskView({ P, loading, bands, riskMeta, openDrill }: {
  P: Palette; loading: boolean; bands: BandRow[];
  riskMeta: Array<{ band: string; range: string; apr: number; defaultRate: number; color: string }>;
  openDrill: (e: DrillEntity) => void;
}) {
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const maxCount = Math.max(1, ...bands.map((b) => Number(b.cnt) || 0));
  const metaByBand = Object.fromEntries(riskMeta.map((m) => [m.band, m]));
  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 12 }}>
        {loading
          ? Array.from({ length: 5 }).map((_, i) => <div key={i} style={{ ...cardStyle, padding: 16 }}><SkeletonShimmer P={P} w="100%" h={90} /></div>)
          : bands.map((b) => {
              const meta = metaByBand[b.ficoBand];
              const color = FICO_SWATCH[b.ficoBand] || P.accent;
              return (
                <div key={b.ficoBand}
                  onClick={() => openDrill({
                    kind: "kpi", id: `band_${b.ficoBand}`,
                    title: `Band ${b.ficoBand}`,
                    value: fmtNum(b.cnt),
                    subvalue: meta ? `FICO ${meta.range}` : undefined,
                    breadcrumb: "Risk",
                    stats: [
                      ["Contracts", fmtNum(b.cnt)],
                      ["Volume", fmtMoneyFull(b.vol)],
                      ["Avg APR", Number(b.avgRate).toFixed(2) + "%"],
                      ...(meta ? [["Default rate" as string, meta.defaultRate.toFixed(1) + "%"]] as Array<[string, string]> : []),
                    ],
                  })}
                  style={{ ...cardStyle, padding: 16, cursor: "pointer" }}
                >
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                    <div style={{ width: 28, height: 28, borderRadius: 6, background: color, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13, fontWeight: 700, color: "#fff" }}>{b.ficoBand}</div>
                    <div style={{ fontFamily: MONO, fontSize: 10, color: P.dim }}>{meta?.range ?? ""}</div>
                  </div>
                  <div style={{ fontSize: 24, fontWeight: 600, letterSpacing: "-0.02em" }}>{fmtNum(b.cnt)}</div>
                  <div style={{ fontSize: 11, color: P.dim, marginTop: 2 }}>contracts</div>
                  <div style={{ marginTop: 12, height: 3, background: P.elev, borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ height: "100%", width: `${(Number(b.cnt) / maxCount) * 100}%`, background: color }} />
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 10, fontSize: 11 }}>
                    <span style={{ color: P.sub }}>APR</span>
                    <span style={{ fontFamily: MONO, color: P.text }}>{Number(b.avgRate).toFixed(2)}%</span>
                  </div>
                  {meta && (
                    <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, fontSize: 11 }}>
                      <span style={{ color: P.sub }}>Default</span>
                      <span style={{ fontFamily: MONO, color: meta.defaultRate > 5 ? P.bad : P.text }}>{meta.defaultRate.toFixed(1)}%</span>
                    </div>
                  )}
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
              {[25, 50, 75].map((y) => <line key={"h" + y} x1="0" y1={y} x2="100" y2={y} stroke={P.rule} strokeWidth="0.3" vectorEffect="non-scaling-stroke" />)}
              {[25, 50, 75].map((x) => <line key={"v" + x} x1={x} y1="0" x2={x} y2="100" stroke={P.rule} strokeWidth="0.3" vectorEffect="non-scaling-stroke" />)}
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
          <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontSize: 10, color: P.faint, fontFamily: MONO }}>
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
              return (
                <div key={b.ficoBand}
                  title={`${b.ficoBand}: ${pct.toFixed(1)}%`}
                  style={{ width: `${pct}%`, background: FICO_SWATCH[b.ficoBand] || P.accent }}
                />
              );
            })}
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 12 }}>
            {bands.map((b) => {
              const total = bands.reduce((s, x) => s + Number(x.vol || 0), 0) || 1;
              const pct = (Number(b.vol) / total) * 100;
              return (
                <div key={b.ficoBand} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: P.sub }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: FICO_SWATCH[b.ficoBand] || P.accent }} />
                  <span style={{ fontFamily: MONO }}>{b.ficoBand} · {pct.toFixed(1)}%</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </>
  );
}

function OriginationsView({ P, loading, monthly, k0, lastMonthVol, maxMonthVol, openDrill }: {
  P: Palette; loading: boolean; monthly: MonthRow[]; k0: KpiRow | undefined;
  lastMonthVol: number; maxMonthVol: number; openDrill: (e: DrillEntity) => void;
}) {
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const totalVol = monthly.reduce((s, m) => s + Number(m.vol || 0), 0);
  const kpis = [
    { id: "l_vol", label: "Period volume", value: fmtMoney(totalVol), delta: "+18%", up: true },
    { id: "new_cnt", label: "New contracts", value: fmtNum(monthly.reduce((s, m) => s + Number(m.cnt || 0), 0)), delta: "+312", up: true },
    { id: "last_vol", label: "Most recent month", value: fmtMoney(lastMonthVol), delta: "", up: true },
  ];

  return (
    <>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12, marginBottom: 12 }}>
        {kpis.map((k) => (
          <div key={k.id} style={{ ...cardStyle, padding: 16 }}>
            <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>{k.label}</div>
            <div style={{ fontSize: 26, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 8 }}>{k.value}</div>
            {k.delta && (
              <div style={{ fontSize: 11, marginTop: 8, color: k.up ? P.ok : P.bad, fontFamily: MONO }}>
                {k.up ? "↑" : "↓"} {k.delta}
              </div>
            )}
          </div>
        ))}
      </div>
      <div
        onClick={() => openDrill({
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
          {loading ? <SkeletonShimmer P={P} w="100%" h={220} r={4} /> : monthly.map((o, i) => {
            const h = (Number(o.vol) / maxMonthVol) * 100;
            const isRecent = i >= monthly.length - 12;
            return <div key={i} title={`${o.originatedMonth}: ${fmtMoney(o.vol)}`}
              style={{ flex: 1, height: `${h}%`, background: isRecent ? P.accent : P.accentSoft, opacity: isRecent ? 1 : 0.7, borderRadius: "2px 2px 0 0" }} />;
          })}
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8, fontSize: 10, color: P.faint, fontFamily: MONO }}>
          <span>{monthly[0]?.originatedMonth ?? ""}</span>
          <span>{monthly[monthly.length - 1]?.originatedMonth ?? ""}</span>
        </div>
      </div>
    </>
  );
}

function DelinquencyView({ P, loading, statusAgg, k0, openDrill }: {
  P: Palette; loading: boolean; statusAgg: StatusRow[]; k0: KpiRow | undefined;
  openDrill: (e: DrillEntity) => void;
}) {
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const delinq = statusAgg.filter((s) => String(s.status).includes("late") || s.status === "charged_off");
  const pct = k0?.totalCount ? (Number(k0.delinquent90Count ?? 0) / Number(k0.totalCount)) * 100 : 0;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
      {loading ? (
        Array.from({ length: 5 }).map((_, i) => <div key={i} style={{ ...cardStyle, padding: 16 }}><SkeletonShimmer P={P} w="100%" h={70} /></div>)
      ) : (
        <>
          {delinq.map((s) => {
            const c = statusTone(s.status, P);
            return (
              <div key={s.status}
                onClick={() => openDrill({
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
                <div style={{ fontSize: 11, color: P.dim, marginTop: 6, fontFamily: MONO }}>{fmtMoney(s.vol)}</div>
              </div>
            );
          })}
          <div style={{ ...cardStyle, padding: 16, borderLeft: `3px solid ${P.warn}` }}>
            <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>90+ DPD rate</div>
            <div style={{ fontSize: 26, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 8, color: P.warn }}>{pct.toFixed(2)}%</div>
            <div style={{ fontSize: 11, color: P.bad, marginTop: 6, fontFamily: MONO }}>↑ +0.12pp MoM</div>
          </div>
        </>
      )}
    </div>
  );
}

function GeographyView({ P, loading, states, openDrill }: {
  P: Palette; loading: boolean; states: StateRow[];
  openDrill: (e: DrillEntity) => void;
}) {
  const cardStyle: React.CSSProperties = { background: P.card, border: `1px solid ${P.border}`, borderRadius: 10 };
  const maxVol = Math.max(1, ...states.map((s) => Number(s.vol) || 0));
  return (
    <div style={{ ...cardStyle, padding: 18 }}>
      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 4 }}>Top 8 states</div>
      <div style={{ fontSize: 11, color: P.dim, marginBottom: 18 }}>Ranked by total volume · click to drill</div>
      {loading
        ? Array.from({ length: 8 }).map((_, i) => <div key={i} style={{ marginBottom: 8 }}><SkeletonShimmer P={P} w="100%" h={22} /></div>)
        : states.map((s, i) => (
            <div key={s.state}
              onClick={() => openDrill({
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
              <span style={{ fontFamily: MONO, fontSize: 11, color: P.faint }}>#{i + 1}</span>
              <span style={{ fontFamily: MONO, fontSize: 12, fontWeight: 600 }}>{s.state}</span>
              <div style={{ height: 6, background: P.elev, borderRadius: 3, overflow: "hidden" }}>
                <div style={{ height: "100%", width: `${(Number(s.vol) / maxVol) * 100}%`, background: P.accent, borderRadius: 3 }} />
              </div>
              <span style={{ fontFamily: MONO, fontSize: 12, textAlign: "right" }}>{fmtMoney(s.vol)}</span>
              <span style={{ fontFamily: MONO, fontSize: 11, color: P.dim, textAlign: "right" }}>{fmtNum(s.cnt)} loans</span>
            </div>
          ))}
    </div>
  );
}
