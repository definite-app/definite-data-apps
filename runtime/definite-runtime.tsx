import React, { useEffect, useMemo, useRef, useState } from "react";

import { SourceMapConsumer, type RawSourceMap } from "source-map-js";

type DataAppMode = "auto" | "live" | "snapshot";
type PerspectiveTheme = "light" | "dark";
type DatasetKind = "table" | "database";
type DataAppErrorSeverity = "warning" | "error" | "fatal";
type DataAppErrorPhase = "bridge" | "resolve" | "fetch" | "duckdb" | "perspective" | "render" | "uncaught";

type DefiniteContext = {
  publicMode: boolean;
  driveFile: string | null;
  appVersion: "v1" | "v2";
};

type ResourceCacheDetails = {
  resourceKey: string;
  resourceKind: "dataset" | "json";
  sourceLabel: string;
  loadTimeMs: number | null;
  rowCount: number | null;
  cacheTtlHours: number;
  cacheKey: string | null;
  fromCache: boolean;
};

type PreviewData = {
  context?: Partial<DefiniteContext>;
  datasets?: Record<string, unknown>;
  resources?: Record<string, unknown>;
};

type DatasetPayload =
  | { type: "arrow-buffer"; buffer: ArrayBuffer }
  | { type: "binary-buffer"; buffer: ArrayBuffer; format: "duckdb" | "parquet" }
  | { type: "json"; data: Array<Record<string, unknown>> }
  | { type: "url"; url: string };

type ResolvedSnapshotRef = {
  format?: string | null;
  drivePath?: string | null;
  url?: string | null;
  alias?: string | null;
  table?: string | null;
};

type ResolvedResource = {
  key: string;
  kind: "dataset" | "json";
  resolvedMode: "live" | "snapshot";
  source?: Record<string, unknown> | null;
  snapshot?: ResolvedSnapshotRef | null;
  downloadUrl?: string | null;
  manifestVersion: number;
  payload?: DatasetPayload | { type: "json"; data: unknown } | { type: "url"; url: string };
};

type DataAppRuntimeError = {
  code: string;
  message: string;
  severity: DataAppErrorSeverity;
  phase: DataAppErrorPhase;
  resourceKey?: string | null;
  resourceKind?: "dataset" | "json" | null;
  component?: string | null;
  requestId?: string | null;
  details?: Record<string, unknown> | null;
};

export type ResolvedStackFrame = {
  source: string | null;
  line: number | null;
  column: number | null;
  name: string | null;
  /** Original frame text (browser-formatted), preserved as a fallback when resolution fails. */
  raw: string;
};

type DefiniteBridge = {
  loadDataset(request: { key: string; mode?: DataAppMode; target?: "table" | "database" }): Promise<ResolvedResource>;
  loadResource(request: { key: string; mode?: DataAppMode }): Promise<ResolvedResource>;
  getContext(): Promise<DefiniteContext>;
  reportError?: (error: string | DataAppRuntimeError) => void;
};

type DatasetHandle = {
  key: string;
  kind: DatasetKind;
  db: any | null;
  conn: any | null;
  tableName: string | null;
  tableRef: string | null;
  perspectiveTable: string | null;
  databaseAlias: string | null;
  loading: boolean;
  error: string | null;
  cache: ResourceCacheDetails | null;
  refresh: () => Promise<void>;
};

type QueryState<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
  cache: ResourceCacheDetails | null;
  refresh: () => Promise<void>;
};

type CachedEntry<T> = {
  ts: number;
  value: T;
};

const APACHE_ARROW_URL = "https://storage.googleapis.com/definite-public/libs/apache-arrow@17.0.0/apache-arrow.esm.js";
const DUCKDB_ESM_URL = "https://storage.googleapis.com/definite-public/libs/duckdb-wasm@1.29.0/duckdb-wasm.esm.js";
const DUCKDB_CDN_ROOT = "https://storage.googleapis.com/definite-public/libs/duckdb-wasm@1.29.0/dist";
const PERSPECTIVE_VIEWER_URL = "https://cdn.jsdelivr.net/npm/@perspective-dev/viewer@4.3.0/dist/cdn/perspective-viewer.js";
const PERSPECTIVE_VIEWER_DATAGRID_URL = "https://cdn.jsdelivr.net/npm/@perspective-dev/viewer-datagrid@4.3.0/dist/cdn/perspective-viewer-datagrid.js";
const PERSPECTIVE_VIEWER_D3FC_URL = "https://cdn.jsdelivr.net/npm/@perspective-dev/viewer-d3fc@4.3.0/dist/cdn/perspective-viewer-d3fc.js";
const PERSPECTIVE_CLIENT_URL = "https://cdn.jsdelivr.net/npm/@perspective-dev/client@4.3.0/dist/cdn/perspective.js";
const PERSPECTIVE_DUCKDB_HANDLER_URL = "https://cdn.jsdelivr.net/npm/@perspective-dev/client@4.3.0/dist/esm/virtual_servers/duckdb.js";
// IndexedDB name stays as "data-apps-v2-cache" even after the data-apps-v2 → data-apps
// namespace rename. Changing it would invalidate every deployed data app's local cache
// and force a cold reload against DuckLake on next open. Not worth the cost for a string
// that never appears in the UI.
const CACHE_DB = "data-apps-v2-cache";
const CACHE_STORE = "resources";
const CACHE_TTL = 24 * 60 * 60 * 1000;

let duckDbRuntimePromise: Promise<{ db: any; conn: any; duckdb: any }> | null = null;
let arrowModulePromise: Promise<any> | null = null;
let perspectiveAssetsPromise: Promise<void> | null = null;
let perspectiveClientPromise: Promise<any> | null = null;
let perspectiveDuckDbPromise: Promise<any> | null = null;
let cacheDbPromise: Promise<IDBDatabase> | null = null;
let previewBridgeCache: DefiniteBridge | null | undefined;
const jsonResourceCache = new Map<string, { value: unknown; cache: ResourceCacheDetails }>();
let embeddedManifestCache: Record<string, unknown> | null | undefined;
let embeddedBridgeCache: DefiniteBridge | null | undefined;

function getEmbedContext(): { token: string; driveFile: string } | null {
  // Injected by the Definite embed route before the app script runs.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const embed = (window as any).__DEFINITE_EMBED;
  if (!embed || typeof embed !== "object") {
    return null;
  }
  const token = typeof embed.token === "string" ? embed.token : null;
  const driveFile = typeof embed.driveFile === "string" ? embed.driveFile : null;
  if (!token || !driveFile) {
    return null;
  }
  return { token, driveFile };
}

function getApiBase(): string {
  // Same origin as the embed HTML; resolves to api.definite.app (or staging)
  // when the app was served via /v4/data-apps/embed. Local dev: window.location.origin.
  return window.location.origin;
}

async function queryEmbeddedResource(
  key: string,
  ctx: { token: string; driveFile: string },
): Promise<ResolvedResource> {
  const resp = await fetch(`${getApiBase()}/v4/data-apps/query`, {
    method: "POST",
    credentials: "omit",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${ctx.token}`,
    },
    body: JSON.stringify({ drive_file: ctx.driveFile, resource_key: key }),
  });

  if (!resp.ok) {
    let message = `data-apps /query failed: ${resp.status} ${resp.statusText}`;
    try {
      const text = await resp.text();
      if (text) {
        message += `: ${text}`;
      }
    }
    catch {
      // ignore
    }
    throw new Error(message);
  }

  const contentType = resp.headers.get("content-type") ?? "";
  if (contentType.includes("arrow")) {
    const buffer = await resp.arrayBuffer();
    return {
      key,
      kind: "dataset",
      resolvedMode: "live",
      source: { type: "sql", embedded: true },
      snapshot: null,
      downloadUrl: null,
      manifestVersion: 2,
      payload: { type: "arrow-buffer", buffer },
    };
  }

  const json = await resp.json();
  return {
    key,
    kind: "dataset",
    resolvedMode: "live",
    source: { type: "cube", embedded: true },
    snapshot: null,
    downloadUrl: null,
    manifestVersion: 2,
    payload: { type: "json", data: Array.isArray(json) ? json : (json?.data ?? []) },
  };
}

function getEmbeddedBridge(): DefiniteBridge | null {
  if (embeddedBridgeCache !== undefined) {
    return embeddedBridgeCache;
  }
  const ctx = getEmbedContext();
  if (!ctx) {
    embeddedBridgeCache = null;
    return embeddedBridgeCache;
  }

  embeddedBridgeCache = {
    async loadDataset({ key }) {
      return await queryEmbeddedResource(key, ctx);
    },
    async loadResource({ key }) {
      const resolved = await queryEmbeddedResource(key, ctx);
      return { ...resolved, kind: "json" };
    },
    async getContext() {
      return {
        publicMode: false,
        driveFile: ctx.driveFile,
        appVersion: "v2",
      } satisfies DefiniteContext;
    },
    reportError(error) {
      console.error("[data-apps embedded]", error);
    },
  };

  return embeddedBridgeCache;
}

function getBridge(): DefiniteBridge {
  if (window.Definite) {
    return window.Definite;
  }
  const embeddedBridge = getEmbeddedBridge();
  if (embeddedBridge) {
    return embeddedBridge;
  }
  const previewBridge = getPreviewBridge();
  if (previewBridge) {
    return previewBridge;
  }
  throw new Error("window.Definite is not available in this app");
}

function emitRuntimeError(error: DataAppRuntimeError) {
  try {
    getBridge().reportError?.(error);
  }
  catch {
    // Ignore secondary bridge failures while surfacing the original error.
  }
}

function emitUnknownRuntimeError(
  error: unknown,
  context: Omit<DataAppRuntimeError, "message"> & { message?: string },
) {
  const stack = error instanceof Error && typeof error.stack === "string" ? error.stack : null;
  const resolvedStack = stack ? resolveStack(stack) : null;
  const baseDetails = context.details ?? {};
  const details: Record<string, unknown> = { ...baseDetails };
  if (stack) details.stack = stack;
  if (resolvedStack) details.resolvedStack = resolvedStack;

  emitRuntimeError({
    ...context,
    message: context.message ?? (error instanceof Error ? error.message : String(error)),
    details: Object.keys(details).length > 0 ? details : null,
  });
}

// =============================================================================
// Stack frame resolution via inline source map
// =============================================================================
// The data-app bundle ships with esbuild's inline source map. Parsing it lets
// us rewrite browser stack frames (which point inside the bundle) to original
// App.tsx positions before the error reaches the host. This is the difference
// between the agent seeing "component=window" and seeing
// "src/App.tsx:215:42 in OverviewView" — the latter is something the agent
// can Read and Edit directly.
//
// The wax dataBridge.ts reads `window.__DEFINITE_RESOLVE_STACK` from inside
// the iframe and uses it to enrich uncaught errors before postMessage. Both
// scripts live in the same iframe so window globals are shared.

let sourceMapConsumer: SourceMapConsumer | null | undefined = undefined;

function getSourceMapConsumer(): SourceMapConsumer | null {
  if (sourceMapConsumer !== undefined) return sourceMapConsumer;

  const SOURCE_MAP_RE = /\/\/# sourceMappingURL=data:application\/json[^,]*;base64,([A-Za-z0-9+/=]+)/;
  try {
    const scripts = Array.from(document.querySelectorAll("script[type='module']"));
    for (const script of scripts) {
      const text = script.textContent ?? "";
      const m = SOURCE_MAP_RE.exec(text);
      if (!m) continue;
      const json = JSON.parse(atob(m[1])) as RawSourceMap;
      sourceMapConsumer = new SourceMapConsumer(json);
      return sourceMapConsumer;
    }
  }
  catch {
    // Source map missing or malformed — give up cleanly; callers fall back to raw frames.
  }

  sourceMapConsumer = null;
  return sourceMapConsumer;
}

// Browser-specific frame formats:
//   Chrome/Edge: "    at OverviewView (https://app.definite.app/docs/x:1234:56)"
//                "    at https://app.definite.app/docs/x:1234:56"   (anonymous)
//   Firefox/Safari: "OverviewView@https://app.definite.app/docs/x:1234:56"
const CHROME_FRAME_RE = /^\s*at\s+(?:(.+?)\s+\()?([^()\s]+):(\d+):(\d+)\)?\s*$/;
const FIREFOX_FRAME_RE = /^(.+?)@(.+?):(\d+):(\d+)\s*$/;

type RawFrame = { name: string | null; line: number; column: number; raw: string };

function parseStackFrames(stack: string): Array<RawFrame> {
  const out: Array<RawFrame> = [];
  for (const line of stack.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("Error")) continue;
    const chrome = CHROME_FRAME_RE.exec(line);
    if (chrome) {
      out.push({ name: chrome[1] ?? null, line: parseInt(chrome[3], 10), column: parseInt(chrome[4], 10), raw: trimmed });
      continue;
    }
    const firefox = FIREFOX_FRAME_RE.exec(line);
    if (firefox) {
      out.push({ name: firefox[1] || null, line: parseInt(firefox[3], 10), column: parseInt(firefox[4], 10), raw: trimmed });
    }
  }
  return out;
}

export function resolveStack(stack: string): Array<ResolvedStackFrame> | null {
  const consumer = getSourceMapConsumer();
  if (!consumer) return null;

  const raw = parseStackFrames(stack);
  if (raw.length === 0) return null;

  return raw.map((frame) => {
    try {
      const pos = consumer.originalPositionFor({ line: frame.line, column: frame.column });
      return {
        source: pos.source ?? null,
        line: pos.line ?? null,
        column: pos.column ?? null,
        name: pos.name ?? frame.name,
        raw: frame.raw,
      };
    }
    catch {
      return { source: null, line: null, column: null, name: frame.name, raw: frame.raw };
    }
  });
}

declare global {
  interface Window {
    __DEFINITE_RESOLVE_STACK?: (stack: string) => Array<ResolvedStackFrame> | null;
  }
}

// Expose for wax's dataBridge.ts to call when window.error/unhandledrejection
// fire. Defined unconditionally — dataBridge falls back to raw stack if it's
// undefined (e.g., the runtime hasn't loaded yet, or an error fires before
// this module executes).
window.__DEFINITE_RESOLVE_STACK = resolveStack;

function hashKey(value: string): string {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
  }
  return `r_${Math.abs(hash).toString(36)}`;
}

function getPreviewData(): PreviewData | null {
  const script = document.getElementById("definite-app-preview-data");
  if (!script?.textContent) {
    return null;
  }

  try {
    const parsed = JSON.parse(script.textContent);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed as PreviewData
      : null;
  }
  catch {
    return null;
  }
}

function getPreviewBridge(): DefiniteBridge | null {
  if (previewBridgeCache !== undefined) {
    return previewBridgeCache;
  }

  const previewData = getPreviewData();
  if (!previewData) {
    previewBridgeCache = null;
    return previewBridgeCache;
  }

  previewBridgeCache = {
    async loadDataset({ key }) {
      const data = previewData.datasets?.[key];
      if (data === undefined || data === null) {
        throw new Error(`Preview dataset ${key} is missing`);
      }
      if (Array.isArray(data)) {
        return {
          key,
          kind: "dataset",
          resolvedMode: "snapshot",
          source: { type: "preview" },
          snapshot: { format: "json" },
          downloadUrl: null,
          manifestVersion: 2,
          payload: { type: "json", data: normalizeRows(data) },
        };
      }
      if (typeof data === "object" && "base64" in data && typeof (data as { base64: unknown }).base64 === "string") {
        const { format, base64 } = data as { format?: "parquet" | "duckdb"; base64: string };
        const binary = atob(base64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
        return {
          key,
          kind: "dataset",
          resolvedMode: "snapshot",
          source: { type: "preview" },
          snapshot: { format: format ?? "parquet" },
          downloadUrl: null,
          manifestVersion: 2,
          payload: { type: "binary-buffer", buffer: bytes.buffer, format: format ?? "parquet" },
        };
      }
      throw new Error(`Preview dataset ${key} is not a row array or binary payload`);
    },
    async loadResource({ key }) {
      const data = previewData.resources?.[key];
      if (data === undefined) {
        throw new Error(`Preview resource ${key} is missing`);
      }
      return {
        key,
        kind: "json",
        resolvedMode: "snapshot",
        source: { type: "preview" },
        snapshot: { format: "json" },
        downloadUrl: null,
        manifestVersion: 2,
        payload: { type: "json", data: normalizeRows(data) },
      };
    },
    async getContext() {
      return {
        publicMode: false,
        driveFile: "preview://data-apps-v2",
        appVersion: "v2",
        ...(previewData.context ?? {}),
      } satisfies DefiniteContext;
    },
    reportError(error) {
      console.error("[data-apps-v2 preview]", error);
    },
  };

  return previewBridgeCache;
}

function getEmbeddedManifest(): Record<string, unknown> | null {
  if (embeddedManifestCache !== undefined) {
    return embeddedManifestCache;
  }

  const script = document.getElementById("definite-app-manifest");
  if (!script?.textContent) {
    embeddedManifestCache = null;
    return embeddedManifestCache;
  }

  try {
    const parsed = JSON.parse(script.textContent);
    embeddedManifestCache = parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : null;
  }
  catch {
    embeddedManifestCache = null;
  }

  return embeddedManifestCache;
}

function getManifestResourceDefinition(key: string): unknown {
  const manifest = getEmbeddedManifest();
  const resources = manifest?.resources;
  if (!resources || typeof resources !== "object" || Array.isArray(resources)) {
    return null;
  }

  const resource = (resources as Record<string, unknown>)[key];
  return resource ?? null;
}

// Sentinel substrings that ship in template default app.json files. While the agent
// is wiring up a freshly scaffolded app, the published HTML still embeds these
// placeholder queries — running them against the warehouse returns a Catalog Error.
// Detecting the marker lets the runtime swap the red error UI for a friendly
// "still being built" state until the agent rebuilds with real SQL.
const PLACEHOLDER_SQL_MARKERS = [
  "LAKE.SCHEMA.my_table",
];

function resourceUsesPlaceholderSql(resource: unknown): boolean {
  if (!resource || typeof resource !== "object" || Array.isArray(resource)) {
    return false;
  }
  const source = (resource as Record<string, unknown>).source;
  if (!source || typeof source !== "object" || Array.isArray(source)) {
    return false;
  }
  const sql = (source as Record<string, unknown>).sql;
  if (typeof sql !== "string") {
    return false;
  }
  return PLACEHOLDER_SQL_MARKERS.some((marker) => sql.includes(marker));
}

export function hasPlaceholderResource(): boolean {
  const manifest = getEmbeddedManifest();
  const resources = manifest?.resources;
  if (!resources || typeof resources !== "object" || Array.isArray(resources)) {
    return false;
  }
  return Object.values(resources as Record<string, unknown>).some(resourceUsesPlaceholderSql);
}

async function buildResourceCacheKey(kind: "dataset" | "json", key: string, mode: DataAppMode): Promise<string> {
  const context = await getBridge().getContext();
  const fingerprint = JSON.stringify({
    version: 1,
    driveFile: context.driveFile ?? "inline",
    kind,
    key,
    mode,
    resource: getManifestResourceDefinition(key),
  });
  return hashKey(fingerprint);
}

async function openCacheDB(): Promise<IDBDatabase> {
  if (typeof indexedDB === "undefined") {
    throw new Error("IndexedDB unavailable");
  }

  cacheDbPromise ??= new Promise((resolve, reject) => {
    try {
      const request = indexedDB.open(CACHE_DB, 1);
      request.onupgradeneeded = () => {
        if (!request.result.objectStoreNames.contains(CACHE_STORE)) {
          request.result.createObjectStore(CACHE_STORE);
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error("Failed to open IndexedDB"));
    }
    catch (error) {
      reject(error);
    }
  });

  return await cacheDbPromise;
}

async function getCachedEntry<T>(cacheKey: string): Promise<CachedEntry<T> | null> {
  try {
    const db = await openCacheDB();
    return await new Promise((resolve) => {
      const request = db.transaction(CACHE_STORE).objectStore(CACHE_STORE).get(cacheKey);
      request.onsuccess = () => {
        const entry = request.result as CachedEntry<T> | undefined;
        if (!entry || Date.now() - entry.ts >= CACHE_TTL) {
          if (entry) {
            void deleteCachedValue(cacheKey);
          }
          resolve(null);
          return;
        }
        resolve(entry);
      };
      request.onerror = () => resolve(null);
    });
  }
  catch {
    return null;
  }
}

async function getCachedValue<T>(cacheKey: string): Promise<T | null> {
  const entry = await getCachedEntry<T>(cacheKey);
  return entry?.value ?? null;
}

async function setCachedValue<T>(cacheKey: string, value: T): Promise<void> {
  try {
    const db = await openCacheDB();
    await new Promise<void>((resolve) => {
      const tx = db.transaction(CACHE_STORE, "readwrite");
      tx.objectStore(CACHE_STORE).put({ ts: Date.now(), value } satisfies CachedEntry<T>, cacheKey);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
      tx.onabort = () => resolve();
    });
  }
  catch {
    // Ignore cache persistence failures in sandboxed iframes.
  }
}

async function deleteCachedValue(cacheKey: string): Promise<void> {
  try {
    const db = await openCacheDB();
    await new Promise<void>((resolve) => {
      const tx = db.transaction(CACHE_STORE, "readwrite");
      tx.objectStore(CACHE_STORE).delete(cacheKey);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
      tx.onabort = () => resolve();
    });
  }
  catch {
    // Ignore cache deletion failures in sandboxed iframes.
  }
}

// Global event target for signaling cache clears to mounted hooks.
const cacheInvalidationBus = new EventTarget();
let clearInFlight = false;

async function clearAllCache(): Promise<void> {
  if (clearInFlight) return;
  clearInFlight = true;
  try {
    try {
      if (cacheDbPromise) {
        const db = await cacheDbPromise;
        db.close();
      }
    }
    catch { /* best effort */ }
    cacheDbPromise = null;

    await new Promise<void>((resolve) => {
      try {
        const req = indexedDB.deleteDatabase(CACHE_DB);
        req.onsuccess = () => resolve();
        req.onerror = () => resolve();
        req.onblocked = () => {
          console.warn("[definite-runtime] IndexedDB delete blocked — proceeding anyway");
          resolve();
        };
      }
      catch { resolve(); }
    });

    jsonResourceCache.clear();
    embeddedManifestCache = undefined;

    cacheInvalidationBus.dispatchEvent(new Event("clear"));
  }
  finally {
    clearInFlight = false;
  }
}

// Listen for the bridge-forwarded event from the parent
window.addEventListener("definite:clear-cache", () => {
  void clearAllCache();
});

async function importModule<T>(url: string): Promise<T> {
  return await import(/* @vite-ignore */ url) as T;
}

function sanitizeIdentifier(value: string): string {
  const sanitized = value.replace(/[^a-zA-Z0-9_]/g, "_");
  return /^[A-Za-z_]/.test(sanitized) ? sanitized : `r_${sanitized}`;
}

function escapeLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function normalizeScalar(value: unknown): unknown {
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (Array.isArray(value)) {
    return value.map(normalizeScalar);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalizeScalar(item)]));
  }
  return value;
}

function normalizeRows<T>(rows: T): T {
  return normalizeScalar(rows) as T;
}

function getOptionalString(record: Record<string, unknown> | null | undefined, key: string): string | null {
  const value = record?.[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

function describeResourceSource(
  resource: ResolvedResource,
  currentSource: "indexeddb" | "memory" | "network",
): string {
  if (currentSource === "memory") {
    return "Memory cache";
  }
  if (currentSource === "indexeddb") {
    const upstream = resource.resolvedMode === "snapshot" ? "snapshot" : getOptionalString(resource.source ?? undefined, "type") ?? "live";
    return `IndexedDB cache (${upstream})`;
  }

  if (resource.resolvedMode === "snapshot") {
    const format = resource.snapshot?.format;
    return format ? `Snapshot (${format})` : "Snapshot";
  }

  const sourceType = getOptionalString(resource.source ?? undefined, "type");
  if (sourceType === "sql") {
    return "Live SQL";
  }
  if (sourceType === "cube") {
    return "Live semantic layer";
  }
  if (sourceType === "duckdbFile") {
    return ".duckdb file";
  }
  if (sourceType === "preview") {
    return "Preview data";
  }
  return "Live data";
}

function createCacheDetails(args: {
  resource: ResolvedResource;
  resourceKey: string;
  resourceKind: "dataset" | "json";
  currentSource: "indexeddb" | "memory" | "network";
  loadTimeMs: number | null;
  rowCount: number | null;
  cacheKey: string | null;
  fromCache: boolean;
}): ResourceCacheDetails {
  return {
    resourceKey: args.resourceKey,
    resourceKind: args.resourceKind,
    sourceLabel: describeResourceSource(args.resource, args.currentSource),
    loadTimeMs: args.loadTimeMs === null ? null : Math.round(args.loadTimeMs),
    rowCount: args.rowCount,
    cacheTtlHours: CACHE_TTL / (60 * 60 * 1000),
    cacheKey: args.cacheKey,
    fromCache: args.fromCache,
  };
}

function resolveDatasetFormat(resource: ResolvedResource): string | null {
  const snapshotFormat = resource.snapshot?.format;
  if (typeof snapshotFormat === "string" && snapshotFormat.length > 0) {
    return snapshotFormat;
  }
  const sourceType = getOptionalString(resource.source ?? undefined, "type");
  if (sourceType === "duckdbFile") {
    return "duckdb";
  }
  if (sourceType === "arrow" || sourceType === "parquet" || sourceType === "json") {
    return sourceType;
  }
  return null;
}

async function getArrowModule(): Promise<any> {
  arrowModulePromise ??= importModule<any>(APACHE_ARROW_URL);
  return await arrowModulePromise;
}

async function getDuckDbRuntime(): Promise<{ db: any; conn: any; duckdb: any }> {
  duckDbRuntimePromise ??= (async () => {
    const duckdb = await importModule<any>(DUCKDB_ESM_URL);
    const bundle = await duckdb.selectBundle({
      mvp: {
        mainModule: `${DUCKDB_CDN_ROOT}/duckdb-mvp.wasm`,
        mainWorker: `${DUCKDB_CDN_ROOT}/duckdb-browser-mvp.worker.js`,
      },
      eh: {
        mainModule: `${DUCKDB_CDN_ROOT}/duckdb-eh.wasm`,
        mainWorker: `${DUCKDB_CDN_ROOT}/duckdb-browser-eh.worker.js`,
      },
    });
    const workerUrl = URL.createObjectURL(new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }));
    const worker = new Worker(workerUrl);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    const conn = await db.connect();
    return { db, conn, duckdb };
  })();

  return await duckDbRuntimePromise;
}

async function ensurePerspectiveAssets(): Promise<void> {
  perspectiveAssetsPromise ??= (async () => {
    await Promise.all([
      importModule(PERSPECTIVE_VIEWER_URL),
      importModule(PERSPECTIVE_VIEWER_DATAGRID_URL),
      importModule(PERSPECTIVE_VIEWER_D3FC_URL),
    ]);
  })();

  await perspectiveAssetsPromise;
}

async function getPerspectiveClient(): Promise<any> {
  perspectiveClientPromise ??= (async () => {
    const mod = await importModule<any>(PERSPECTIVE_CLIENT_URL);
    return mod.default ?? mod;
  })();
  return await perspectiveClientPromise;
}

async function getPerspectiveDuckDbHandler(): Promise<any> {
  perspectiveDuckDbPromise ??= importModule<any>(PERSPECTIVE_DUCKDB_HANDLER_URL);
  return await perspectiveDuckDbPromise;
}

async function fetchArrayBuffer(url: string): Promise<ArrayBuffer> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: HTTP ${response.status}`);
  }
  return await response.arrayBuffer();
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: HTTP ${response.status}`);
  }
  return normalizeRows(await response.json());
}

function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, "\"\"")}"`;
}

function findHugeIntColumns(result: any): string[] {
  const fields = result?.schema?.fields;
  if (!Array.isArray(fields)) {
    return [];
  }
  const names: string[] = [];
  for (const field of fields) {
    const type = field?.type;
    if (!type || typeof field.name !== "string") {
      continue;
    }
    // Apache Arrow JS: Type.Int === 2. DuckDB's HUGEINT / UHUGEINT both arrive as
    // 128-bit Int. Detect by typeId or constructor name to be resilient across
    // Arrow JS minor versions.
    const isInt = type.typeId === 2 || type.constructor?.name === "Int";
    const bitWidth = typeof type.bitWidth === "number" ? type.bitWidth : null;
    if (isInt && bitWidth === 128) {
      names.push(field.name);
    }
  }
  return names;
}

function arrowRowsToObjects(result: any): Array<Record<string, unknown>> {
  return result.toArray().map((row: { toJSON?: () => Record<string, unknown> }) => row.toJSON?.() ?? row);
}

async function queryRows(conn: any, sql: string): Promise<Array<Record<string, unknown>>> {
  const result = await conn.query(sql);
  const hugeIntColumns = findHugeIntColumns(result);
  if (hugeIntColumns.length === 0) {
    return normalizeRows(arrowRowsToObjects(result));
  }

  // DuckDB WASM 1.29.0's Arrow row.toJSON() silently returns 0 for HUGEINT / UHUGEINT
  // values (no error, no warning). Re-issue the query with affected columns cast to
  // DOUBLE so they round-trip into JS numbers. Loses precision above 2^53; acceptable
  // for typical aggregate cases (counts, summed currency amounts).
  const hugeIntSet = new Set(hugeIntColumns);
  const projections = (result.schema.fields as Array<{ name: string }>).map((field) => {
    const quoted = quoteIdentifier(field.name);
    return hugeIntSet.has(field.name) ? `${quoted}::DOUBLE AS ${quoted}` : quoted;
  });
  const wrappedSql = `SELECT ${projections.join(", ")} FROM (${sql})`;

  try {
    const wrappedResult = await conn.query(wrappedSql);
    return normalizeRows(arrowRowsToObjects(wrappedResult));
  }
  catch (error) {
    emitUnknownRuntimeError(error, {
      code: "SQL_QUERY_HUGEINT_CAST_FALLBACK",
      severity: "warning",
      phase: "duckdb",
      component: "queryRows",
      message: "Failed to cast HUGEINT columns to DOUBLE; affected values may be returned as 0.",
      details: { sql, wrappedSql, columns: hugeIntColumns },
    });
    return normalizeRows(arrowRowsToObjects(result));
  }
}

async function getTableRowCount(conn: any, tableRef: string | null): Promise<number | null> {
  if (!tableRef) {
    return null;
  }

  try {
    const rows = await queryRows(conn, `SELECT COUNT(*)::INTEGER AS rowCount FROM ${tableRef}`);
    const value = rows[0]?.rowCount;
    if (typeof value === "number") {
      return value;
    }
    if (typeof value === "bigint") {
      return Number(value);
    }
    if (typeof value === "string" && value.length > 0) {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    }
    return null;
  }
  catch {
    return null;
  }
}

async function createOrReplaceJsonTable(db: any, conn: any, tableName: string, rows: Array<Record<string, unknown>>): Promise<void> {
  const fileName = `${tableName}.json`;
  await db.registerFileBuffer(fileName, new TextEncoder().encode(JSON.stringify(rows)));
  await conn.query(`CREATE OR REPLACE TABLE "${tableName}" AS SELECT * FROM read_json_auto('${escapeLiteral(fileName)}')`);
}

async function createOrReplaceArrowTable(conn: any, tableName: string, buffer: ArrayBuffer): Promise<void> {
  await conn.query(`DROP TABLE IF EXISTS "${tableName}"`);
  const arrow = await getArrowModule();
  const arrowTable = arrow.tableFromIPC(new Uint8Array(buffer));
  const streamBytes = arrow.tableToIPC(arrowTable, "stream");
  await conn.insertArrowFromIPCStream(streamBytes, { name: tableName, create: true });
}

async function createOrReplaceParquetTable(db: any, conn: any, tableName: string, buffer: ArrayBuffer): Promise<void> {
  const fileName = `${tableName}.parquet`;
  await db.registerFileBuffer(fileName, new Uint8Array(buffer));
  await conn.query(`CREATE OR REPLACE TABLE "${tableName}" AS SELECT * FROM read_parquet('${escapeLiteral(fileName)}')`);
}

async function attachDuckDbFile(
  db: any,
  conn: any,
  buffer: ArrayBuffer,
  alias: string,
): Promise<void> {
  const fileName = `${alias}.duckdb`;
  await db.registerFileBuffer(fileName, new Uint8Array(buffer));
  try {
    await conn.query(`DETACH "${alias}"`);
  }
  catch {
    // Ignore detach failures when the alias is not yet attached.
  }
  await conn.query(`ATTACH '${escapeLiteral(fileName)}' AS "${alias}" (READ_ONLY)`);
}

async function hydrateDatasetHandle(
  key: string,
  resource: ResolvedResource,
  cacheDetails: Omit<ResourceCacheDetails, "rowCount">,
): Promise<DatasetHandle> {
  const { db, conn } = await getDuckDbRuntime();
  const tableName = sanitizeIdentifier(key);
  const tableRef = `"${tableName}"`;
  const sourceAlias = sanitizeIdentifier(
    getOptionalString(resource.snapshot ?? undefined, "alias")
      ?? getOptionalString(resource.source ?? undefined, "alias")
      ?? key,
  );
  const sourceTable = sanitizeIdentifier(
    getOptionalString(resource.snapshot ?? undefined, "table")
      ?? getOptionalString(resource.source ?? undefined, "table")
      ?? tableName,
  );

  if (resource.payload?.type === "arrow-buffer") {
    await createOrReplaceArrowTable(conn, tableName, resource.payload.buffer);
    const rowCount = await getTableRowCount(conn, tableRef);
    return {
      key,
      kind: "table",
      db,
      conn,
      tableName,
      tableRef,
      perspectiveTable: `memory.${tableName}`,
      databaseAlias: null,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  if (resource.payload?.type === "json") {
    const rows = Array.isArray(resource.payload.data) ? normalizeRows(resource.payload.data) : [];
    await createOrReplaceJsonTable(db, conn, tableName, rows as Array<Record<string, unknown>>);
    const rowCount = await getTableRowCount(conn, tableRef);
    return {
      key,
      kind: "table",
      db,
      conn,
      tableName,
      tableRef,
      perspectiveTable: `memory.${tableName}`,
      databaseAlias: null,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  const url = resource.payload?.type === "url" ? resource.payload.url : resource.downloadUrl;
  if (resource.payload?.type === "binary-buffer") {
    if (resource.payload.format === "duckdb") {
      await attachDuckDbFile(db, conn, resource.payload.buffer, sourceAlias);
      const tableRefValue = `"${sourceAlias}".main."${sourceTable}"`;
      const rowCount = await getTableRowCount(conn, tableRefValue);
      return {
        key,
        kind: "database",
        db,
        conn,
        tableName: sourceTable,
        tableRef: tableRefValue,
        perspectiveTable: `${sourceAlias}.main.${sourceTable}`,
        databaseAlias: sourceAlias,
        loading: false,
        error: null,
        cache: { ...cacheDetails, rowCount },
        refresh: async () => {},
      };
    }

    await createOrReplaceParquetTable(db, conn, tableName, resource.payload.buffer);
    const rowCount = await getTableRowCount(conn, tableRef);
    return {
      key,
      kind: "table",
      db,
      conn,
      tableName,
      tableRef,
      perspectiveTable: `memory.${tableName}`,
      databaseAlias: null,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  if (!url) {
    throw new Error(`Resource ${key} did not return a dataset payload or download URL`);
  }

  const format = resolveDatasetFormat(resource);
  if (format === "duckdb") {
    const buffer = await fetchArrayBuffer(url);
    await attachDuckDbFile(db, conn, buffer, sourceAlias);
    const tableRefValue = `"${sourceAlias}".main."${sourceTable}"`;
    const rowCount = await getTableRowCount(conn, tableRefValue);
    return {
      key,
      kind: "database",
      db,
      conn,
      tableName: sourceTable,
      tableRef: tableRefValue,
      perspectiveTable: `${sourceAlias}.main.${sourceTable}`,
      databaseAlias: sourceAlias,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  if (format === "parquet") {
    const buffer = await fetchArrayBuffer(url);
    await createOrReplaceParquetTable(db, conn, tableName, buffer);
    const rowCount = await getTableRowCount(conn, tableRef);
    return {
      key,
      kind: "table",
      db,
      conn,
      tableName,
      tableRef,
      perspectiveTable: `memory.${tableName}`,
      databaseAlias: null,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  if (format === "arrow") {
    const buffer = await fetchArrayBuffer(url);
    await createOrReplaceArrowTable(conn, tableName, buffer);
    const rowCount = await getTableRowCount(conn, tableRef);
    return {
      key,
      kind: "table",
      db,
      conn,
      tableName,
      tableRef,
      perspectiveTable: `memory.${tableName}`,
      databaseAlias: null,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  if (format === "json") {
    const payload = await fetchJson(url);
    const rows = Array.isArray(payload) ? payload as Array<Record<string, unknown>> : [];
    await createOrReplaceJsonTable(db, conn, tableName, rows);
    const rowCount = await getTableRowCount(conn, tableRef);
    return {
      key,
      kind: "table",
      db,
      conn,
      tableName,
      tableRef,
      perspectiveTable: `memory.${tableName}`,
      databaseAlias: null,
      loading: false,
      error: null,
      cache: { ...cacheDetails, rowCount },
      refresh: async () => {},
    };
  }

  throw new Error(`Unsupported dataset format for ${key}`);
}

async function loadDatasetHandle(
  key: string,
  mode: DataAppMode = "auto",
  opts?: { bypassCache?: boolean },
): Promise<DatasetHandle> {
  const startedAt = performance.now();
  const cacheKey = await buildResourceCacheKey("dataset", key, mode);
  if (opts?.bypassCache) {
    await deleteCachedValue(cacheKey);
  }
  else {
    const cached = await getCachedEntry<ResolvedResource>(cacheKey);
    if (cached) {
      return await hydrateDatasetHandle(
        key,
        cached.value,
        createCacheDetails({
          resource: cached.value,
          resourceKey: key,
          resourceKind: "dataset",
          currentSource: "indexeddb",
          loadTimeMs: performance.now() - startedAt,
          rowCount: null,
          cacheKey,
          fromCache: true,
        }),
      );
    }
  }

  const bridge = getBridge();
  const resource = await bridge.loadDataset({ key, mode });
  let cacheableResource = resource;

  if (resource.payload?.type === "arrow-buffer" || resource.payload?.type === "json") {
    await setCachedValue(cacheKey, resource);
    return await hydrateDatasetHandle(
      key,
      resource,
      createCacheDetails({
        resource,
        resourceKey: key,
        resourceKind: "dataset",
        currentSource: "network",
        loadTimeMs: performance.now() - startedAt,
        rowCount: null,
        cacheKey,
        fromCache: false,
      }),
    );
  }

  const url = resource.payload?.type === "url" ? resource.payload.url : resource.downloadUrl;
  const format = resolveDatasetFormat(resource);
  if (url && format === "arrow") {
    const buffer = await fetchArrayBuffer(url);
    cacheableResource = { ...resource, downloadUrl: null, payload: { type: "arrow-buffer", buffer } };
  }
  else if (url && format === "json") {
    const payload = await fetchJson(url);
    cacheableResource = {
      ...resource,
      downloadUrl: null,
      payload: { type: "json", data: Array.isArray(payload) ? payload as Array<Record<string, unknown>> : [] },
    };
  }
  else if (url && (format === "duckdb" || format === "parquet")) {
    const buffer = await fetchArrayBuffer(url);
    cacheableResource = {
      ...resource,
      downloadUrl: null,
      payload: { type: "binary-buffer", buffer, format },
    };
  }

  await setCachedValue(cacheKey, cacheableResource);
  return await hydrateDatasetHandle(
    key,
    cacheableResource,
    createCacheDetails({
      resource: cacheableResource,
      resourceKey: key,
      resourceKind: "dataset",
      currentSource: "network",
      loadTimeMs: performance.now() - startedAt,
      rowCount: null,
      cacheKey,
      fromCache: false,
    }),
  );
}

async function loadJsonResource<T>(
  key: string,
  mode: DataAppMode = "auto",
  opts?: { bypassCache?: boolean },
): Promise<{ data: T; cache: ResourceCacheDetails }> {
  const startedAt = performance.now();
  const cacheKey = await buildResourceCacheKey("json", key, mode);
  if (opts?.bypassCache) {
    jsonResourceCache.delete(cacheKey);
    await deleteCachedValue(cacheKey);
  }

  const inMemory = jsonResourceCache.get(cacheKey);
  if (inMemory) {
    return {
      data: inMemory.value as T,
      cache: {
        ...inMemory.cache,
        sourceLabel: "Memory cache",
        loadTimeMs: Math.round(performance.now() - startedAt),
        fromCache: true,
      },
    };
  }

  if (!opts?.bypassCache) {
    const cached = await getCachedEntry<T>(cacheKey);
    if (cached !== null) {
      const cache = createCacheDetails({
        resource: {
          key,
          kind: "json",
          resolvedMode: "snapshot",
          source: { type: "cached" },
          manifestVersion: 2,
        },
        resourceKey: key,
        resourceKind: "json",
        currentSource: "indexeddb",
        loadTimeMs: performance.now() - startedAt,
        rowCount: Array.isArray(cached.value) ? cached.value.length : null,
        cacheKey,
        fromCache: true,
      });
      jsonResourceCache.set(cacheKey, { value: cached.value, cache });
      return { data: cached.value, cache };
    }
  }

  const bridge = getBridge();
  const resource = await bridge.loadResource({ key, mode });
  if (resource.payload?.type === "json") {
    const value = normalizeRows(resource.payload.data) as T;
    const cache = createCacheDetails({
      resource,
      resourceKey: key,
      resourceKind: "json",
      currentSource: "network",
      loadTimeMs: performance.now() - startedAt,
      rowCount: Array.isArray(value) ? value.length : null,
      cacheKey,
      fromCache: false,
    });
    jsonResourceCache.set(cacheKey, { value, cache });
    await setCachedValue(cacheKey, value);
    return { data: value, cache };
  }

  const url = resource.payload?.type === "url" ? resource.payload.url : resource.downloadUrl;
  if (!url) {
    throw new Error(`Resource ${key} did not return JSON data or a download URL`);
  }

  const value = await fetchJson(url) as T;
  const cache = createCacheDetails({
    resource,
    resourceKey: key,
    resourceKind: "json",
    currentSource: "network",
    loadTimeMs: performance.now() - startedAt,
    rowCount: Array.isArray(value) ? value.length : null,
    cacheKey,
    fromCache: false,
  });
  jsonResourceCache.set(cacheKey, { value, cache });
  await setCachedValue(cacheKey, value);
  return { data: value, cache };
}

export function useDataset(key: string, opts?: { mode?: DataAppMode }): DatasetHandle {
  const mode = opts?.mode ?? "auto";
  const [state, setState] = useState<DatasetHandle>({
    key,
    kind: "table",
    db: null,
    conn: null,
    tableName: null,
    tableRef: null,
    perspectiveTable: null,
    databaseAlias: null,
    loading: true,
    error: null,
    cache: null,
    refresh: async () => {},
  });

  useEffect(() => {
    let cancelled = false;

    // Skip placeholder resources entirely — the agent is still scaffolding,
    // and running the placeholder SQL against the warehouse would throw a
    // Catalog Error which surfaces as host-side "Data app error" toasts.
    // Stay in loading state; LoadingState detects the placeholder and
    // renders ScaffoldingState in the iframe.
    if (resourceUsesPlaceholderSql(getManifestResourceDefinition(key))) {
      setState((current) => ({ ...current, key, loading: true, error: null, cache: null }));
      return () => {};
    }

    const load = async (bypassCache = false) => {
      setState((current) => ({ ...current, key, loading: true, error: null }));
      try {
        const dataset = await loadDatasetHandle(key, mode, { bypassCache });
        if (!cancelled) {
          setState({
            ...dataset,
            refresh: async () => await load(true),
          });
        }
      }
      catch (error) {
        emitUnknownRuntimeError(error, {
          code: "DATASET_LOAD_FAILED",
          severity: "error",
          phase: "resolve",
          resourceKey: key,
          resourceKind: "dataset",
          component: "useDataset",
        });
        if (!cancelled) {
          setState((current) => ({
            ...current,
            key,
            loading: false,
            error: error instanceof Error ? error.message : String(error),
            cache: null,
            refresh: async () => await load(true),
          }));
        }
      }
    };

    void load(false);

    const onClear = () => { void load(true); };
    cacheInvalidationBus.addEventListener("clear", onClear);
    return () => {
      cancelled = true;
      cacheInvalidationBus.removeEventListener("clear", onClear);
    };
  }, [key, mode]);

  return state;
}

export function useJsonResource<T = unknown>(key: string, opts?: { mode?: DataAppMode }): QueryState<T> {
  const mode = opts?.mode ?? "auto";
  const [state, setState] = useState<QueryState<T>>({
    data: null,
    loading: true,
    error: null,
    cache: null,
    refresh: async () => {},
  });

  useEffect(() => {
    let cancelled = false;

    // Skip placeholder resources entirely — see useDataset for the same
    // short-circuit. Prevents Catalog Errors from leaking out to host toasts
    // while the agent is scaffolding.
    if (resourceUsesPlaceholderSql(getManifestResourceDefinition(key))) {
      setState((current) => ({ ...current, loading: true, error: null, cache: null }));
      return () => {};
    }

    const load = async (bypassCache = false) => {
      setState((current) => ({ ...current, loading: true, error: null }));
      try {
        const { data, cache } = await loadJsonResource<T>(key, mode, { bypassCache });
        if (!cancelled) {
          setState({ data, loading: false, error: null, cache, refresh: async () => await load(true) });
        }
      }
      catch (error) {
        emitUnknownRuntimeError(error, {
          code: "JSON_RESOURCE_LOAD_FAILED",
          severity: "error",
          phase: "resolve",
          resourceKey: key,
          resourceKind: "json",
          component: "useJsonResource",
        });
        if (!cancelled) {
          setState({
            data: null,
            loading: false,
            error: error instanceof Error ? error.message : String(error),
            cache: null,
            refresh: async () => await load(true),
          });
        }
      }
    };

    void load(false);

    const onClear = () => { void load(true); };
    cacheInvalidationBus.addEventListener("clear", onClear);
    return () => {
      cancelled = true;
      cacheInvalidationBus.removeEventListener("clear", onClear);
    };
  }, [key, mode]);

  return state;
}

export function useSqlQuery<T = Array<Record<string, unknown>>>(
  dataset: Pick<DatasetHandle, "conn" | "loading" | "error">,
  sql: string,
  deps: Array<unknown> = [],
): QueryState<T> {
  const [state, setState] = useState<QueryState<T>>({
    data: null,
    loading: false,
    error: null,
    cache: null,
    refresh: async () => {},
  });

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      if (!dataset.conn || dataset.loading || dataset.error || !sql) {
        setState((current) => ({
          ...current,
          loading: false,
          error: dataset.error,
          refresh: load,
        }));
        return;
      }

      setState((current) => ({ ...current, loading: true, error: null }));
      try {
        const data = await queryRows(dataset.conn, sql) as T;
        if (!cancelled) {
          setState({ data, loading: false, error: null, cache: null, refresh: load });
        }
      }
      catch (error) {
        emitUnknownRuntimeError(error, {
          code: "SQL_QUERY_FAILED",
          severity: "error",
          phase: "duckdb",
          component: "useSqlQuery",
          details: { sql },
        });
        if (!cancelled) {
          setState({
            data: null,
            loading: false,
            error: error instanceof Error ? error.message : String(error),
            cache: null,
            refresh: load,
          });
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [dataset.conn, dataset.loading, dataset.error, sql, ...deps]);

  return state;
}

export function useTheme(): { theme: PerspectiveTheme; toggleTheme: () => void } {
  const [theme, setTheme] = useState<PerspectiveTheme>(() => {
    return document.documentElement.classList.contains("light") ? "light" : "dark";
  });

  useEffect(() => {
    const queryTheme = new URLSearchParams(window.location.search).get("theme");
    if (queryTheme === "light" || queryTheme === "dark") {
      applyTheme(queryTheme);
      setTheme(queryTheme);
      return;
    }

    try {
      const storedTheme = window.localStorage.getItem("theme");
      if (storedTheme === "light" || storedTheme === "dark") {
        applyTheme(storedTheme);
        setTheme(storedTheme);
      }
    }
    catch {
      // localStorage is not always available in sandboxed iframes.
    }
  }, []);

  const toggleTheme = () => {
    const nextTheme: PerspectiveTheme = theme === "dark" ? "light" : "dark";
    applyTheme(nextTheme);
    setTheme(nextTheme);
    try {
      window.localStorage.setItem("theme", nextTheme);
    }
    catch {
      // Ignore localStorage failures in sandboxed iframes.
    }
  };

  return { theme, toggleTheme };
}

function applyTheme(theme: PerspectiveTheme) {
  document.documentElement.classList.toggle("light", theme === "light");
}

export function usePerspective(dataset: Pick<DatasetHandle, "key" | "conn" | "loading" | "error">): {
  client: any | null;
  loading: boolean;
  error: string | null;
} {
  const [state, setState] = useState<{ client: any | null; loading: boolean; error: string | null }>({
    client: null,
    loading: false,
    error: null,
  });

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      if (!dataset.conn || dataset.loading || dataset.error) {
        setState({ client: null, loading: false, error: dataset.error });
        return;
      }

      setState({ client: null, loading: true, error: null });
      try {
        await ensurePerspectiveAssets();
        const perspective = await getPerspectiveClient();
        const handlerModule = await getPerspectiveDuckDbHandler();
        const DuckDBHandler = handlerModule.DuckDBHandler;
        const handler = new DuckDBHandler(dataset.conn);
        const server = perspective.createMessageHandler(handler);
        const client = await perspective.worker(server);
        if (!cancelled) {
          setState({ client, loading: false, error: null });
        }
      }
      catch (error) {
        emitUnknownRuntimeError(error, {
          code: "PERSPECTIVE_CLIENT_FAILED",
          severity: "error",
          phase: "perspective",
          resourceKey: dataset.key,
          resourceKind: "dataset",
          component: "usePerspective",
        });
        if (!cancelled) {
          setState({
            client: null,
            loading: false,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [dataset.conn, dataset.loading, dataset.error]);

  return state;
}

function getPerspectiveTheme(theme: PerspectiveTheme): string {
  return theme === "light" ? "Pro Light" : "Pro Dark";
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function sanitizePerspectiveConfig(
  rawConfig: Record<string, unknown>,
  schemaKeys: Array<string>,
): { config: Record<string, unknown>; dropped: Record<string, Array<string>> } {
  if (!schemaKeys.length) {
    return { config: rawConfig, dropped: {} };
  }

  const allowed = new Set(schemaKeys);
  const config = { ...rawConfig };
  const dropped: Record<string, Array<string>> = {};

  const sanitizeStringArray = (field: "columns" | "group_by" | "split_by") => {
    const value = config[field];
    if (!Array.isArray(value)) {
      return;
    }

    const kept = value.filter((entry): entry is string => typeof entry === "string" && allowed.has(entry));
    const removed = value.filter((entry): entry is string => typeof entry === "string" && !allowed.has(entry));
    if (removed.length) {
      dropped[field] = removed;
    }
    if (kept.length) {
      config[field] = kept;
    }
    else {
      delete config[field];
    }
  };

  sanitizeStringArray("columns");
  sanitizeStringArray("group_by");
  sanitizeStringArray("split_by");

  const sortValue = config.sort;
  if (Array.isArray(sortValue)) {
    const kept = sortValue.filter(
      (entry): entry is Array<unknown> => Array.isArray(entry) && typeof entry[0] === "string" && allowed.has(entry[0]),
    );
    const removed = sortValue
      .filter((entry): entry is Array<unknown> => Array.isArray(entry) && typeof entry[0] === "string" && !allowed.has(entry[0]))
      .map((entry) => entry[0] as string);
    if (removed.length) {
      dropped.sort = removed;
    }
    if (kept.length) {
      config.sort = kept;
    }
    else {
      delete config.sort;
    }
  }

  const filterValue = config.filter;
  if (Array.isArray(filterValue)) {
    const kept = filterValue.filter(
      (entry): entry is Array<unknown> => Array.isArray(entry) && typeof entry[0] === "string" && allowed.has(entry[0]),
    );
    const removed = filterValue
      .filter((entry): entry is Array<unknown> => Array.isArray(entry) && typeof entry[0] === "string" && !allowed.has(entry[0]))
      .map((entry) => entry[0] as string);
    if (removed.length) {
      dropped.filter = removed;
    }
    if (kept.length) {
      config.filter = kept;
    }
    else {
      delete config.filter;
    }
  }

  return { config, dropped };
}

export function AppShell(props: {
  title: string;
  subtitle?: string;
  theme: PerspectiveTheme;
  onToggleTheme: () => void;
  meta?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <main
      className="min-h-screen px-6 py-6 lg:px-8"
      style={{ animation: "fade-up 0.4s ease-out" }}
    >
      <div className="mx-auto flex w-full max-w-[1480px] flex-col gap-6">
        <header
          className="flex flex-col gap-3 pb-5 lg:flex-row lg:items-end lg:justify-between"
          style={{ borderBottom: "1px solid var(--border)" }}
        >
          <div>
            <h1 className="text-[22px] font-semibold tracking-[-0.02em]">{props.title}</h1>
            {props.subtitle ? (
              <p className="mt-1 text-sm" style={{ color: "var(--text-muted)" }}>
                {props.subtitle}
              </p>
            ) : null}
            {props.meta ? (
              <div className="mt-2">
                {props.meta}
              </div>
            ) : null}
          </div>
          <button
            type="button"
            onClick={props.onToggleTheme}
            className="flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs font-medium"
            style={{
              background: "var(--bg-elevated)",
              border: "1px solid var(--border)",
              color: "var(--text-secondary)",
              cursor: "pointer",
            }}
            onMouseEnter={(e) => { e.currentTarget.style.borderColor = "var(--border-hover)"; e.currentTarget.style.color = "var(--text-primary)"; }}
            onMouseLeave={(e) => { e.currentTarget.style.borderColor = "var(--border)"; e.currentTarget.style.color = "var(--text-secondary)"; }}
          >
            <span style={{ fontSize: "14px" }}>{props.theme === "dark" ? "\u2600" : "\u263E"}</span>
            {props.theme === "dark" ? "Light" : "Dark"}
          </button>
        </header>
        {props.children}
      </div>
    </main>
  );
}

function formatCompactNumber(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US").format(value);
}

export function ResourceCacheBadge(props: {
  rows?: number | null;
  cache: ResourceCacheDetails | null;
  onClearAndReload: () => Promise<void>;
}) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const containerRef = useRef<HTMLSpanElement | null>(null);
  const rows = props.rows ?? props.cache?.rowCount ?? null;

  useEffect(() => {
    if (!open) {
      return;
    }

    const onDocumentClick = (event: MouseEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };

    document.addEventListener("click", onDocumentClick);
    return () => document.removeEventListener("click", onDocumentClick);
  }, [open]);

  const handleClear = async () => {
    setBusy(true);
    try {
      await props.onClearAndReload();
      setOpen(false);
    }
    finally {
      setBusy(false);
    }
  };

  return (
    <span
      ref={containerRef}
      className="inline-flex items-center gap-1 text-xs"
      style={{ color: "var(--text-muted)", position: "relative" }}
    >
      <span>{formatCompactNumber(rows)} rows</span>
      <span>&middot;</span>
      <button
        type="button"
        className="rounded-md px-1.5 py-0.5 text-xs"
        style={{ color: "var(--text-secondary)", background: "transparent", border: "none", cursor: "pointer", textDecoration: "underline", textDecorationStyle: "dotted", textUnderlineOffset: "3px", textDecorationColor: "var(--text-muted)" }}
        onClick={() => setOpen((current) => !current)}
      >
        {props.cache?.sourceLabel ?? "Loading source..."}
      </button>
      {open ? (
        <div
          className="absolute left-0 top-full z-20 mt-2 min-w-[240px] rounded-xl p-3"
          style={{ background: "var(--bg-elevated)", boxShadow: "var(--shadow-card-hover)", animation: "fade-up 0.15s ease-out" }}
        >
          <div className="mb-2 flex items-center justify-between gap-4 text-[11px]" style={{ color: "var(--text-secondary)" }}>
            <span style={{ color: "var(--text-muted)" }}>Source</span>
            <span>{props.cache?.sourceLabel ?? "-"}</span>
          </div>
          <div className="mb-2 flex items-center justify-between gap-4 text-[11px]" style={{ color: "var(--text-secondary)" }}>
            <span style={{ color: "var(--text-muted)" }}>Load time</span>
            <span>{props.cache?.loadTimeMs !== null && props.cache?.loadTimeMs !== undefined ? `${props.cache.loadTimeMs} ms` : "-"}</span>
          </div>
          <div className="mb-2 flex items-center justify-between gap-4 text-[11px]" style={{ color: "var(--text-secondary)" }}>
            <span style={{ color: "var(--text-muted)" }}>Cache TTL</span>
            <span>{props.cache ? `${props.cache.cacheTtlHours} hours` : "-"}</span>
          </div>
          <div className="mb-3 flex items-center justify-between gap-4 text-[11px]" style={{ color: "var(--text-secondary)" }}>
            <span style={{ color: "var(--text-muted)" }}>Rows</span>
            <span>{formatCompactNumber(rows)}</span>
          </div>
          <button
            type="button"
            className="w-full rounded-lg border px-3 py-1.5 text-[11px] font-semibold"
            style={{
              background: "var(--bg-card)",
              borderColor: "var(--border)",
              color: "var(--text-primary)",
              cursor: busy ? "wait" : "pointer",
              transition: "background 120ms ease, border-color 120ms ease",
            }}
            onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)"; e.currentTarget.style.borderColor = "var(--border-hover)"; }}
            onMouseLeave={(e) => { e.currentTarget.style.background = "var(--bg-card)"; e.currentTarget.style.borderColor = "var(--border)"; }}
            onClick={() => void handleClear()}
            disabled={busy}
          >
            {busy ? "Clearing..." : "Clear cache & reload"}
          </button>
        </div>
      ) : null}
    </span>
  );
}

export function Card(props: {
  children: React.ReactNode;
  title?: string;
  headerRight?: React.ReactNode;
  noPadding?: boolean;
  className?: string;
}) {
  return (
    <div
      className={`overflow-hidden rounded-xl ${props.className ?? ""}`}
      style={{ background: "var(--bg-card)", boxShadow: "var(--shadow-card)" }}
    >
      {props.title || props.headerRight ? (
        <div
          className="flex items-center justify-between gap-3 px-5 py-3.5"
          style={{ borderBottom: "1px solid var(--border)" }}
        >
          {props.title ? <h2 className="text-sm font-semibold">{props.title}</h2> : <span />}
          {props.headerRight ?? null}
        </div>
      ) : null}
      {props.noPadding ? props.children : (
        <div className="p-5">{props.children}</div>
      )}
    </div>
  );
}

export function TabGroup(props: {
  tabs: Array<string>;
  activeTab: string;
  onTabChange: (tab: string) => void;
  children?: React.ReactNode;
}) {
  return (
    <div>
      <div
        className="flex items-center gap-0"
        style={{ borderBottom: "1px solid var(--border)" }}
      >
        {props.tabs.map((tab) => {
          const active = tab === props.activeTab;
          return (
            <button
              key={tab}
              type="button"
              className="relative px-4 py-2.5 text-sm font-medium"
              style={{
                background: "none",
                border: "none",
                color: active ? "var(--text-primary)" : "var(--text-muted)",
                cursor: "pointer",
                transition: "color 120ms ease",
              }}
              onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--text-secondary)"; }}
              onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--text-muted)"; }}
              onClick={() => props.onTabChange(tab)}
            >
              {tab}
              {active ? (
                <span
                  style={{
                    position: "absolute",
                    bottom: -1,
                    left: 16,
                    right: 16,
                    height: 2,
                    background: "var(--accent)",
                    borderRadius: 1,
                  }}
                />
              ) : null}
            </button>
          );
        })}
      </div>
      {props.children ? <div className="pt-5">{props.children}</div> : null}
    </div>
  );
}

type BadgeVariant = "default" | "success" | "warning" | "error" | "info";

const badgeStyles: Record<BadgeVariant, { bg: string; text: string; dot: string }> = {
  default: { bg: "var(--bg-elevated)", text: "var(--text-secondary)", dot: "var(--text-muted)" },
  success: { bg: "rgba(34,197,94,0.1)", text: "#4ade80", dot: "#22c55e" },
  warning: { bg: "rgba(234,179,8,0.1)", text: "#facc15", dot: "#eab308" },
  error: { bg: "rgba(239,68,68,0.1)", text: "#f87171", dot: "#ef4444" },
  info: { bg: "var(--accent-muted)", text: "var(--accent)", dot: "var(--accent)" },
};

export function Badge(props: {
  children: React.ReactNode;
  variant?: BadgeVariant;
  dot?: boolean;
}) {
  const variant = props.variant ?? "default";
  const style = badgeStyles[variant];
  return (
    <span
      className="inline-flex items-center gap-1.5 rounded-md px-2 py-0.5 text-xs font-medium"
      style={{ background: style.bg, color: style.text }}
    >
      {props.dot !== false ? (
        <span
          className="h-1.5 w-1.5 rounded-full"
          style={{ background: style.dot, flexShrink: 0 }}
        />
      ) : null}
      {props.children}
    </span>
  );
}

export function Select(props: {
  options: Array<{ value: string; label: string }>;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  label?: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (!containerRef.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const selectedOption = props.options.find((o) => o.value === props.value);
  const displayLabel = selectedOption?.label ?? props.placeholder ?? "Select...";

  return (
    <div ref={containerRef} className="relative" style={{ minWidth: 160 }}>
      {props.label ? (
        <div
          className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.06em]"
          style={{ color: "var(--text-muted)" }}
        >
          {props.label}
        </div>
      ) : null}
      <button
        type="button"
        className="flex w-full items-center justify-between rounded-lg px-3 py-2 text-sm"
        style={{
          background: "var(--bg-elevated)",
          border: "1px solid var(--border)",
          color: props.value ? "var(--text-primary)" : "var(--text-muted)",
          cursor: "pointer",
          transition: "border-color 120ms ease",
        }}
        onMouseEnter={(e) => { e.currentTarget.style.borderColor = "var(--border-hover)"; }}
        onMouseLeave={(e) => { if (!open) e.currentTarget.style.borderColor = "var(--border)"; }}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="truncate">{displayLabel}</span>
        <svg
          width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
          style={{ flexShrink: 0, marginLeft: 8, opacity: 0.5, transform: open ? "rotate(180deg)" : "rotate(0)", transition: "transform 150ms ease" }}
        >
          <path d="M6 9l6 6 6-6" />
        </svg>
      </button>
      {open ? (
        <div
          className="absolute left-0 top-full z-30 mt-1.5 w-full overflow-hidden rounded-xl"
          style={{
            background: "var(--bg-card)",
            boxShadow: "var(--shadow-card-hover)",
            animation: "fade-up 0.12s ease-out",
            maxHeight: 260,
            overflowY: "auto",
          }}
        >
          {props.placeholder ? (
            <button
              type="button"
              className="flex w-full items-center px-3 py-2 text-left text-sm"
              style={{
                background: !props.value ? "var(--bg-hover)" : "transparent",
                color: "var(--text-muted)",
                border: "none",
                cursor: "pointer",
                transition: "background 80ms ease",
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)"; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = !props.value ? "var(--bg-hover)" : "transparent"; }}
              onClick={() => { props.onChange(""); setOpen(false); }}
            >
              {props.placeholder}
            </button>
          ) : null}
          {props.options.map((option) => {
            const active = option.value === props.value;
            return (
              <button
                key={option.value}
                type="button"
                className="flex w-full items-center justify-between px-3 py-2 text-left text-sm"
                style={{
                  background: active ? "var(--bg-hover)" : "transparent",
                  color: "var(--text-primary)",
                  border: "none",
                  cursor: "pointer",
                  transition: "background 80ms ease",
                }}
                onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)"; }}
                onMouseLeave={(e) => { e.currentTarget.style.background = active ? "var(--bg-hover)" : "transparent"; }}
                onClick={() => { props.onChange(option.value); setOpen(false); }}
              >
                <span>{option.label}</span>
                {active ? (
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                ) : null}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

export function TextInput(props: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  label?: React.ReactNode;
  icon?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={props.className}>
      {props.label ? (
        <div
          className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.06em]"
          style={{ color: "var(--text-muted)" }}
        >
          {props.label}
        </div>
      ) : null}
      <div className="relative">
        {props.icon ? (
          <div
            className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2"
            style={{ color: "var(--text-muted)" }}
          >
            {props.icon}
          </div>
        ) : null}
        <input
          className={`w-full rounded-lg py-2 text-sm outline-none ${props.icon ? "pl-9 pr-3" : "px-3"}`}
          style={{
            background: "var(--bg-elevated)",
            border: "1px solid var(--border)",
            color: "var(--text-primary)",
            transition: "border-color 120ms ease, box-shadow 120ms ease",
          }}
          onFocus={(e) => { e.currentTarget.style.borderColor = "var(--accent)"; e.currentTarget.style.boxShadow = "0 0 0 3px var(--accent-muted)"; }}
          onBlur={(e) => { e.currentTarget.style.borderColor = "var(--border)"; e.currentTarget.style.boxShadow = "none"; }}
          placeholder={props.placeholder}
          value={props.value}
          onChange={(e) => props.onChange(e.target.value)}
        />
      </div>
    </div>
  );
}

export function ScaffoldingState(props: { message?: string } = {}) {
  return (
    <div
      className="flex min-h-screen flex-col items-center justify-center px-6"
      style={{ animation: "fade-up 0.3s ease-out" }}
    >
      <div className="flex max-w-md flex-col items-center gap-4 text-center">
        <div className="flex items-center gap-1.5">
          {[0, 1, 2].map((i) => (
            <div
              key={i}
              className="h-2 w-2 rounded-full"
              style={{
                background: "var(--accent)",
                animation: "pulse-dot 1.2s ease-in-out infinite",
                animationDelay: `${i * 0.2}s`,
              }}
            />
          ))}
        </div>
        <div className="text-base font-semibold" style={{ color: "var(--text-primary)" }}>
          Setting up your data app
        </div>
        <div className="text-sm leading-relaxed" style={{ color: "var(--text-secondary)" }}>
          {props.message ?? "Wiring up your data and views. This page will refresh when the next build lands."}
        </div>
      </div>
    </div>
  );
}

export function LoadingState(props: { message?: string }) {
  if (hasPlaceholderResource()) {
    return <ScaffoldingState />;
  }
  return (
    <div
      className="flex min-h-screen flex-col items-center justify-center px-6"
      style={{ animation: "fade-up 0.3s ease-out" }}
    >
      <div className="flex flex-col items-center gap-4">
        <div className="flex items-center gap-1.5">
          {[0, 1, 2].map((i) => (
            <div
              key={i}
              className="h-2 w-2 rounded-full"
              style={{
                background: "var(--accent)",
                animation: "pulse-dot 1.2s ease-in-out infinite",
                animationDelay: `${i * 0.2}s`,
              }}
            />
          ))}
        </div>
        <div className="text-sm font-medium" style={{ color: "var(--text-muted)" }}>
          {props.message ?? "Loading..."}
        </div>
      </div>
    </div>
  );
}

export function ErrorState(props: { title: string; message: string }) {
  if (hasPlaceholderResource()) {
    return <ScaffoldingState />;
  }
  return (
    <div
      className="flex min-h-screen items-center justify-center px-6"
      style={{ animation: "fade-up 0.3s ease-out" }}
    >
      <div
        className="max-w-lg rounded-xl px-6 py-5"
        style={{
          background: "var(--bg-card)",
          boxShadow: "var(--shadow-card)",
          borderLeft: "3px solid #ef4444",
        }}
      >
        <div className="flex items-start gap-3">
          <span style={{ color: "#ef4444", fontSize: "18px", lineHeight: "1.4" }}>{"\u26A0"}</span>
          <div>
            <h1 className="text-base font-semibold">{props.title}</h1>
            <p className="mt-1.5 text-sm leading-relaxed" style={{ color: "var(--text-secondary)" }}>
              {props.message}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export function IssueBanner(props: {
  title: string;
  message: string;
  severity?: Exclude<DataAppErrorSeverity, "fatal">;
}) {
  const borderColor = props.severity === "warning" ? "#f59e0b" : "#ef4444";

  return (
    <div
      className="rounded-xl px-4 py-3"
      style={{
        background: "var(--bg-card)",
        boxShadow: "var(--shadow-card)",
        borderLeft: `3px solid ${borderColor}`,
      }}
    >
      <div className="flex items-start gap-3">
        <span style={{ color: borderColor, fontSize: "16px", lineHeight: "1.4" }}>{"\u26A0"}</span>
        <div>
          <div className="text-sm font-semibold">{props.title}</div>
          <div className="mt-1 text-sm leading-relaxed" style={{ color: "var(--text-secondary)" }}>
            {props.message}
          </div>
        </div>
      </div>
    </div>
  );
}

export function PanelErrorState(props: {
  title: string;
  message: string;
  actionLabel?: string;
  onAction?: () => void;
}) {
  return (
    <div
      className="flex h-full min-h-[220px] items-center justify-center px-5 py-6"
      style={{ background: "var(--bg-elevated)" }}
    >
      <div className="max-w-md text-center">
        <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full" style={{ background: "rgba(239, 68, 68, 0.12)", color: "#ef4444" }}>
          {"\u26A0"}
        </div>
        <h3 className="text-sm font-semibold">{props.title}</h3>
        <p className="mt-2 text-sm leading-relaxed" style={{ color: "var(--text-secondary)" }}>
          {props.message}
        </p>
        {props.onAction ? (
          <button
            type="button"
            className="mt-4 rounded-lg border px-3 py-1.5 text-xs font-semibold"
            style={{
              background: "var(--bg-card)",
              borderColor: "var(--border)",
              color: "var(--text-primary)",
              cursor: "pointer",
            }}
            onClick={props.onAction}
          >
            {props.actionLabel ?? "Retry"}
          </button>
        ) : null}
      </div>
    </div>
  );
}

function formatValue(value: unknown, format: "number" | "currency" | "percent"): string {
  if (typeof value === "string" && value.length > 0 && Number.isNaN(Number(value))) return value;
  const numericValue = typeof value === "number" ? value : Number(value ?? 0);
  if (!isFinite(numericValue)) return "\u2014";
  if (format === "currency") {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(numericValue);
  }
  if (format === "percent") {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(numericValue) + "%";
  }
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(numericValue);
}

export function KpiCard(props: {
  title: string;
  value: unknown;
  format: "number" | "currency" | "percent";
  loading?: boolean;
  detail?: React.ReactNode;
}) {
  return (
    <div
      className="relative overflow-hidden rounded-xl px-5 py-4"
      style={{
        background: "var(--bg-card)",
        boxShadow: "var(--shadow-card)",
        cursor: "default",
        transition: "transform 180ms ease, box-shadow 180ms ease",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.transform = "translateY(-2px)";
        e.currentTarget.style.boxShadow = "var(--shadow-card-hover)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.transform = "translateY(0)";
        e.currentTarget.style.boxShadow = "var(--shadow-card)";
      }}
    >
      <div
        style={{
          position: "absolute",
          top: 0,
          left: 0,
          right: 0,
          height: "2px",
          background: "var(--accent)",
          opacity: 0.5,
        }}
      />
      <div
        className="text-[11px] font-semibold uppercase tracking-[0.06em]"
        style={{ color: "var(--text-muted)" }}
      >
        {props.title}
      </div>
      <div
        className="mt-2 text-[28px] font-semibold tracking-[-0.02em]"
        style={{ fontFeatureSettings: '"tnum"' }}
      >
        {props.loading ? (
          <span
            className="inline-block h-7 w-24 rounded-md"
            style={{
              background: "linear-gradient(90deg, var(--bg-elevated) 25%, var(--bg-hover) 50%, var(--bg-elevated) 75%)",
              backgroundSize: "200% 100%",
              animation: "shimmer 1.5s infinite",
            }}
          />
        ) : (
          formatValue(props.value, props.format)
        )}
      </div>
      {props.detail && !props.loading ? (
        <div className="mt-2">{props.detail}</div>
      ) : null}
    </div>
  );
}

export function DataTable(props: {
  columns: Array<{ key: string; label: string }>;
  rows: Array<Record<string, unknown>>;
  emptyState?: string;
  pageSize?: number;
}) {
  const [page, setPage] = useState(1);

  const total = props.rows.length;
  const paginated = props.pageSize != null && props.pageSize > 0;
  const pageSize = paginated ? (props.pageSize as number) : total;
  const pageCount = paginated ? Math.max(1, Math.ceil(total / pageSize)) : 1;

  useEffect(() => {
    if (page > pageCount) setPage(1);
  }, [total, props.pageSize, pageCount, page]);

  if (!props.rows.length) {
    return (
      <div
        className="flex items-center justify-center rounded-xl px-4 py-8 text-sm"
        style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)", color: "var(--text-muted)" }}
      >
        {props.emptyState ?? "No rows to display."}
      </div>
    );
  }

  const start = paginated ? (page - 1) * pageSize : 0;
  const end = paginated ? Math.min(start + pageSize, total) : total;
  const visibleRows = paginated ? props.rows.slice(start, end) : props.rows;

  const Chevron = ({ dir }: { dir: "prev" | "next" }) => (
    <svg
      width="12"
      height="12"
      viewBox="0 0 12 12"
      fill="none"
      style={{ opacity: 0.7, transform: dir === "prev" ? "rotate(90deg)" : "rotate(-90deg)" }}
    >
      <path d="M3 4.5L6 7.5L9 4.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );

  const pagerButtonStyle = (disabled: boolean): React.CSSProperties => ({
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    padding: "6px 10px",
    borderRadius: 6,
    border: "1px solid var(--border)",
    background: "none",
    color: disabled ? "var(--text-muted)" : "var(--text-primary)",
    cursor: disabled ? "not-allowed" : "pointer",
    opacity: disabled ? 0.5 : 1,
    fontSize: 12,
    fontWeight: 500,
    transition: "background 120ms ease, border-color 120ms ease",
  });

  return (
    <div className="overflow-hidden rounded-xl" style={{ boxShadow: "var(--shadow-card)" }}>
      <table className="min-w-full border-collapse">
        <thead>
          <tr style={{ background: "var(--bg-elevated)" }}>
            {props.columns.map((column) => (
              <th
                key={column.key}
                className="px-3 py-2.5 text-left text-[11px] font-semibold uppercase tracking-[0.06em]"
                style={{ borderBottom: "1px solid var(--border)", color: "var(--text-muted)" }}
              >
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {visibleRows.map((row, index) => (
            <tr
              key={index}
              style={{
                background: "var(--bg-card)",
                borderBottom: index < visibleRows.length - 1 ? "1px solid var(--border)" : "none",
                transition: "background 120ms ease",
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)"; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = "var(--bg-card)"; }}
            >
              {props.columns.map((column) => (
                <td
                  key={column.key}
                  className="px-3 py-2 text-sm"
                  style={{ color: "var(--text-primary)" }}
                >
                  {String(row[column.key] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {paginated ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "10px 12px",
            background: "var(--bg-elevated)",
            borderTop: "1px solid var(--border)",
            color: "var(--text-muted)",
            fontSize: 12,
          }}
        >
          <div>
            Showing {total === 0 ? 0 : start + 1}–{end} of {total.toLocaleString()} rows
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <button
              type="button"
              disabled={page <= 1}
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              style={pagerButtonStyle(page <= 1)}
            >
              <Chevron dir="prev" />
              Prev
            </button>
            <span style={{ color: "var(--text-muted)", minWidth: 70, textAlign: "center" }}>
              Page {page} of {pageCount}
            </span>
            <button
              type="button"
              disabled={page >= pageCount}
              onClick={() => setPage((p) => Math.min(pageCount, p + 1))}
              style={pagerButtonStyle(page >= pageCount)}
            >
              Next
              <Chevron dir="next" />
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function MultiSelect<T extends Record<string, unknown>>(props: {
  options: Array<T>;
  selected: Array<T>;
  onChange: (selected: Array<T>) => void;
  labelKey: string;
  valueKey: string;
  placeholder?: string;
  searchPlaceholder?: string;
  label?: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const containerRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (!containerRef.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const selectedValues = new Set(props.selected.map((item) => String(item[props.valueKey])));

  const filtered = props.options.filter((option) => {
    if (!search) return true;
    return String(option[props.labelKey]).toLowerCase().includes(search.toLowerCase());
  });

  const toggle = (option: T) => {
    const val = String(option[props.valueKey]);
    if (selectedValues.has(val)) {
      props.onChange(props.selected.filter((s) => String(s[props.valueKey]) !== val));
    } else {
      props.onChange([...props.selected, option]);
    }
  };

  const displayLabel = props.selected.length === 0
    ? (props.placeholder ?? "Select...")
    : props.selected.length === 1
      ? String(props.selected[0][props.labelKey])
      : `${props.selected.length} selected`;

  return (
    <div ref={containerRef} className="relative" style={{ minWidth: 180 }}>
      {props.label ? (
        <div
          className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.06em]"
          style={{ color: "var(--text-muted)" }}
        >
          {props.label}
        </div>
      ) : null}
      <button
        type="button"
        className="flex w-full items-center justify-between rounded-lg px-3 py-2 text-sm"
        style={{
          background: "var(--bg-elevated)",
          border: "1px solid var(--border)",
          color: props.selected.length > 0 ? "var(--text-primary)" : "var(--text-muted)",
          cursor: "pointer",
          transition: "border-color 120ms ease",
        }}
        onMouseEnter={(e) => { e.currentTarget.style.borderColor = "var(--border-hover)"; }}
        onMouseLeave={(e) => { if (!open) e.currentTarget.style.borderColor = "var(--border)"; }}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="truncate">{displayLabel}</span>
        <svg
          width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
          style={{ flexShrink: 0, marginLeft: 8, opacity: 0.5, transform: open ? "rotate(180deg)" : "rotate(0)", transition: "transform 150ms ease" }}
        >
          <path d="M6 9l6 6 6-6" />
        </svg>
      </button>
      {open ? (
        <div
          className="absolute left-0 top-full z-30 mt-1.5 w-full overflow-hidden rounded-xl"
          style={{
            background: "var(--bg-card)",
            boxShadow: "var(--shadow-card-hover)",
            animation: "fade-up 0.12s ease-out",
            maxHeight: 320,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div className="px-2.5 pt-2.5 pb-1.5">
            <input
              ref={inputRef}
              className="w-full rounded-md px-2.5 py-1.5 text-sm outline-none"
              style={{
                background: "var(--bg-elevated)",
                border: "1px solid var(--border)",
                color: "var(--text-primary)",
              }}
              placeholder={props.searchPlaceholder ?? "Search..."}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              onFocus={(e) => { e.currentTarget.style.borderColor = "var(--accent)"; e.currentTarget.style.boxShadow = "0 0 0 3px var(--accent-muted)"; }}
              onBlur={(e) => { e.currentTarget.style.borderColor = "var(--border)"; e.currentTarget.style.boxShadow = "none"; }}
            />
          </div>
          <div style={{ overflowY: "auto", flex: 1, maxHeight: 220 }}>
            {filtered.length === 0 ? (
              <div className="px-3 py-3 text-sm" style={{ color: "var(--text-muted)" }}>
                No results
              </div>
            ) : (
              filtered.map((option) => {
                const val = String(option[props.valueKey]);
                const checked = selectedValues.has(val);
                return (
                  <button
                    key={val}
                    type="button"
                    className="flex w-full items-center gap-2.5 px-3 py-2 text-left text-sm"
                    style={{
                      background: "transparent",
                      color: "var(--text-primary)",
                      border: "none",
                      cursor: "pointer",
                      transition: "background 80ms ease",
                    }}
                    onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg-hover)"; }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
                    onClick={() => toggle(option)}
                  >
                    <span
                      className="flex h-4 w-4 shrink-0 items-center justify-center rounded"
                      style={{
                        border: checked ? "none" : "1px solid var(--border-hover)",
                        background: checked ? "var(--accent)" : "transparent",
                        transition: "background 100ms ease, border-color 100ms ease",
                      }}
                    >
                      {checked ? (
                        <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
                          <polyline points="20 6 9 17 4 12" />
                        </svg>
                      ) : null}
                    </span>
                    <span className="truncate">{String(option[props.labelKey])}</span>
                  </button>
                );
              })
            )}
          </div>
          {props.selected.length > 0 ? (
            <div
              className="flex items-center justify-between px-3 py-2 text-xs"
              style={{ borderTop: "1px solid var(--border)", color: "var(--text-muted)" }}
            >
              <span>{props.selected.length} selected</span>
              <button
                type="button"
                className="text-xs font-medium"
                style={{ color: "var(--text-secondary)", cursor: "pointer", background: "none", border: "none" }}
                onMouseEnter={(e) => { e.currentTarget.style.color = "var(--text-primary)"; }}
                onMouseLeave={(e) => { e.currentTarget.style.color = "var(--text-secondary)"; }}
                onClick={() => { props.onChange([]); }}
              >
                Clear
              </button>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

export function PerspectivePanel(props: {
  client: any | null;
  table: string | null;
  theme: PerspectiveTheme;
  config?: Record<string, unknown>;
  resourceKey?: string | null;
  loading?: boolean;
  error?: string | null;
  onSelect?: (row: Record<string, unknown> | null) => void;
}) {
  const viewerRef = useRef<any>(null);
  const onSelectRef = useRef(props.onSelect);
  onSelectRef.current = props.onSelect;
  const serializedConfig = useMemo(() => JSON.stringify(props.config ?? {}), [props.config]);
  const [panelError, setPanelError] = useState<string | null>(null);
  const [retryToken, setRetryToken] = useState(0);

  useEffect(() => {
    let cancelled = false;

    const load = async () => {
      setPanelError(null);

      if (props.loading) {
        console.info("[PerspectivePanel] Waiting for perspective client", {
          table: props.table,
          resourceKey: props.resourceKey ?? null,
        });
        return;
      }

      if (props.error) {
        setPanelError(props.error);
        return;
      }

      if (!props.client || !props.table || !viewerRef.current) {
        console.info("[PerspectivePanel] Skipping load", {
          hasClient: Boolean(props.client),
          table: props.table,
          hasViewer: Boolean(viewerRef.current),
        });
        return;
      }

      const restoreConfig = {
        ...(props.config ?? {}),
        table: props.table,
        theme: getPerspectiveTheme(props.theme),
      };

      console.info("[PerspectivePanel] Starting load", {
        table: props.table,
        theme: props.theme,
        config: restoreConfig,
      });

      try {
        await ensurePerspectiveAssets();
        let schemaKeys: Array<string> = [];
        try {
          if (typeof props.client.open_table === "function") {
            const tableHandle = await props.client.open_table(props.table);
            const schema = asRecord(await tableHandle?.schema?.());
            schemaKeys = schema ? Object.keys(schema) : [];
            console.info("[PerspectivePanel] Hosted table schema", {
              table: props.table,
              schemaKeys,
            });
          }
        }
        catch (schemaError) {
          console.warn("[PerspectivePanel] Failed to inspect hosted table schema", {
            table: props.table,
            error: schemaError,
          });
        }

        const { config: sanitizedConfig, dropped } = sanitizePerspectiveConfig(restoreConfig, schemaKeys);
        if (Object.keys(dropped).length > 0) {
          console.warn("[PerspectivePanel] Dropped invalid config fields", {
            table: props.table,
            dropped,
            schemaKeys,
          });
        }

        const viewer = viewerRef.current;
        await viewer.load(props.client);
        if (cancelled) {
          console.info("[PerspectivePanel] Cancelled after load", { table: props.table });
          return;
        }
        console.info("[PerspectivePanel] Restoring viewer config", {
          table: props.table,
          config: sanitizedConfig,
        });
        try {
          await viewer.restore(sanitizedConfig);
        }
        catch (restoreError) {
          const fallbackConfig = {
            table: props.table,
            theme: getPerspectiveTheme(props.theme),
            ...(typeof props.config?.plugin === "string" ? { plugin: props.config.plugin } : {}),
          };
          console.warn("[PerspectivePanel] Restore failed; retrying minimal config", {
            table: props.table,
            config: sanitizedConfig,
            fallbackConfig,
            error: restoreError,
          });
          await viewer.resetError?.();
          await viewer.restore(fallbackConfig);
        }
        console.info("[PerspectivePanel] Restore completed", { table: props.table });
        viewer.notifyResize?.();
      }
      catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error("[PerspectivePanel] Load failed", {
          table: props.table,
          config: restoreConfig,
          error,
        });
        setPanelError(message);
        emitUnknownRuntimeError(error, {
          code: "PERSPECTIVE_LOAD_FAILED",
          severity: "error",
          phase: "perspective",
          resourceKey: props.resourceKey ?? null,
          resourceKind: "dataset",
          component: "PerspectivePanel",
          details: {
            table: props.table,
            theme: props.theme,
            config: restoreConfig,
          },
        });
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [props.client, props.table, props.theme, props.resourceKey, props.loading, props.error, serializedConfig, retryToken]);

  useEffect(() => {
    const viewer = viewerRef.current;
    if (!viewer) return;
    const handler = (e: any) => {
      const row = e.detail?.row ?? null;
      onSelectRef.current?.(row);
    };
    viewer.addEventListener("perspective-select", handler);
    return () => viewer.removeEventListener("perspective-select", handler);
  }, []);

  if (props.loading) {
    return (
      <PanelErrorState
        title="Preparing chart runtime"
        message="Loading Perspective and connecting it to DuckDB..."
      />
    );
  }

  if (props.error || panelError) {
    return (
      <PanelErrorState
        title="Chart unavailable"
        message={props.error ?? panelError ?? "Perspective could not be initialized."}
        actionLabel={!props.error && panelError ? "Retry chart" : undefined}
        onAction={!props.error && panelError ? () => setRetryToken((current) => current + 1) : undefined}
      />
    );
  }

  return React.createElement("perspective-viewer", {
    ref: viewerRef,
    style: { width: "100%", height: "100%", display: "block" },
  });
}

type ReportCellValue = string | number | { value: string | number; color?: string; bold?: boolean };

export function ReportTable(props: {
  headerGroups: Array<{
    label: string;
    colSpan?: number;
    color?: string;
    subHeaders: Array<{ key: string; label: string; align?: "left" | "right" }>;
  }>;
  rows: Array<{
    type?: "data" | "section" | "subtotal" | "total";
    indent?: boolean;
    cells: Record<string, ReportCellValue>;
  }>;
  emptyState?: string;
}) {
  const allSubHeaders = props.headerGroups.flatMap((g) => g.subHeaders);
  const totalCols = allSubHeaders.length;

  function renderCell(cell: ReportCellValue | undefined, align: "left" | "right" = "right") {
    if (cell === undefined || cell === null) return <td className="px-3 py-2 text-sm" style={{ textAlign: align, color: "var(--text-muted)" }}>{"\u2014"}</td>;
    if (typeof cell === "string" || typeof cell === "number") {
      return <td className="px-3 py-2 text-sm" style={{ textAlign: align, color: "var(--text-primary)" }}>{String(cell)}</td>;
    }
    return (
      <td
        className="px-3 py-2 text-sm"
        style={{
          textAlign: align,
          color: cell.color ?? "var(--text-primary)",
          fontWeight: cell.bold ? 600 : undefined,
        }}
      >
        {String(cell.value)}
      </td>
    );
  }

  if (!props.rows.length) {
    return (
      <div
        className="flex items-center justify-center rounded-xl px-4 py-8 text-sm"
        style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)", color: "var(--text-muted)" }}
      >
        {props.emptyState ?? "No rows to display."}
      </div>
    );
  }

  return (
    <div className="overflow-x-auto overflow-hidden rounded-xl" style={{ boxShadow: "var(--shadow-card)" }}>
      <table className="min-w-full border-collapse">
        <thead>
          <tr>
            {props.headerGroups.map((group, gi) => (
              <th
                key={gi}
                colSpan={group.colSpan ?? group.subHeaders.length}
                className="px-3 py-2.5 text-center text-xs font-semibold"
                style={{
                  background: group.color ?? "var(--bg-elevated)",
                  color: group.color ? "#fff" : "var(--text-primary)",
                  borderBottom: "1px solid var(--border)",
                  borderLeft: gi > 0 ? "1px solid rgba(255,255,255,0.15)" : undefined,
                }}
              >
                {group.label}
              </th>
            ))}
          </tr>
          <tr style={{ background: "var(--bg-elevated)" }}>
            {allSubHeaders.map((sh) => (
              <th
                key={sh.key}
                className="px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.04em]"
                style={{
                  textAlign: sh.align ?? "right",
                  borderBottom: "1px solid var(--border)",
                  color: "var(--text-muted)",
                }}
              >
                {sh.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {props.rows.map((row, ri) => {
            const type = row.type ?? "data";

            if (type === "section") {
              const label = row.cells[allSubHeaders[0]?.key ?? ""] ?? "";
              return (
                <tr key={ri} style={{ background: "var(--bg-elevated)" }}>
                  <td
                    colSpan={totalCols}
                    className="px-3 py-2.5 text-xs font-bold uppercase tracking-[0.04em]"
                    style={{ color: "var(--text-secondary)" }}
                  >
                    {typeof label === "object" && "value" in label ? String(label.value) : String(label)}
                  </td>
                </tr>
              );
            }

            const isTotal = type === "total";
            const isSub = type === "subtotal";

            return (
              <tr
                key={ri}
                style={{
                  background: isTotal ? "var(--bg-elevated)" : "var(--bg-card)",
                  borderTop: isTotal ? "2px solid var(--border)" : isSub ? "1px solid var(--border)" : undefined,
                  borderBottom: ri < props.rows.length - 1 ? "1px solid var(--border)" : "none",
                  fontWeight: isTotal || isSub ? 600 : undefined,
                  transition: type === "data" ? "background 120ms ease" : undefined,
                }}
                onMouseEnter={type === "data" ? (e) => { e.currentTarget.style.background = "var(--bg-hover)"; } : undefined}
                onMouseLeave={type === "data" ? (e) => { e.currentTarget.style.background = "var(--bg-card)"; } : undefined}
              >
                {allSubHeaders.map((sh, ci) => {
                  const cellValue = row.cells[sh.key];
                  if (ci === 0 && row.indent) {
                    const cv = cellValue === undefined || cellValue === null ? "\u2014"
                      : typeof cellValue === "object" && "value" in cellValue ? String(cellValue.value)
                      : String(cellValue);
                    return (
                      <td key={sh.key} className="px-3 py-2 pl-7 text-sm" style={{ textAlign: sh.align ?? "left", color: "var(--text-primary)" }}>
                        {cv}
                      </td>
                    );
                  }
                  return <React.Fragment key={sh.key}>{renderCell(cellValue, sh.align ?? (ci === 0 ? "left" : "right"))}</React.Fragment>;
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function EChart(props: {
  option: Record<string, unknown>;
  height?: number;
  theme?: PerspectiveTheme;
  onClick?: (params: unknown) => void;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const instanceRef = useRef<any>(null);
  const onClickRef = useRef(props.onClick);
  onClickRef.current = props.onClick;
  const serializedOption = useMemo(() => JSON.stringify(props.option), [props.option]);
  const echartsTheme = (props.theme ?? "dark") === "light" ? "light" : "dark";

  // Init / re-init on theme change
  useEffect(() => {
    const el = containerRef.current;
    if (!el || typeof (window as any).echarts === "undefined") return;

    if (instanceRef.current) {
      instanceRef.current.dispose();
      instanceRef.current = null;
    }

    const instance = (window as any).echarts.init(el, echartsTheme);
    instanceRef.current = instance;

    try {
      const opt = JSON.parse(serializedOption);
      if (!opt.backgroundColor) opt.backgroundColor = "transparent";
      instance.setOption(opt, true);
    } catch (e) {
      console.error("[EChart] setOption failed", e);
    }

    const clickHandler = (params: unknown) => onClickRef.current?.(params);
    instance.on("click", clickHandler);

    const ro = new ResizeObserver(() => instance.resize());
    ro.observe(el);

    return () => {
      ro.disconnect();
      instance.off("click", clickHandler);
      instance.dispose();
      instanceRef.current = null;
    };
  }, [echartsTheme]);

  // Update option without re-init
  useEffect(() => {
    if (!instanceRef.current) return;
    try {
      const opt = JSON.parse(serializedOption);
      if (!opt.backgroundColor) opt.backgroundColor = "transparent";
      instanceRef.current.setOption(opt, true);
    } catch (e) {
      console.error("[EChart] setOption update failed", e);
    }
  }, [serializedOption]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: props.height ?? 320 }}
    />
  );
}

export function FilterPills(props: {
  options: Array<{ value: string; label: string }>;
  value: string;
  onChange: (value: string) => void;
  label?: React.ReactNode;
}) {
  return (
    <div>
      {props.label ? (
        <div
          className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.06em]"
          style={{ color: "var(--text-muted)" }}
        >
          {props.label}
        </div>
      ) : null}
      <div className="flex flex-wrap items-center gap-1.5">
        {props.options.map((opt) => {
          const active = opt.value === props.value;
          return (
            <button
              key={opt.value}
              type="button"
              className="rounded-lg px-3 py-1.5 text-xs font-medium"
              style={{
                background: active ? "var(--accent-muted)" : "var(--bg-elevated)",
                color: active ? "var(--accent)" : "var(--text-muted)",
                border: active ? "1px solid var(--accent)" : "1px solid var(--border)",
                cursor: "pointer",
                transition: "background 120ms ease, color 120ms ease, border-color 120ms ease",
              }}
              onMouseEnter={(e) => {
                if (!active) {
                  e.currentTarget.style.color = "var(--text-secondary)";
                  e.currentTarget.style.borderColor = "var(--border-hover)";
                }
              }}
              onMouseLeave={(e) => {
                if (!active) {
                  e.currentTarget.style.color = "var(--text-muted)";
                  e.currentTarget.style.borderColor = "var(--border)";
                }
              }}
              onClick={() => props.onChange(opt.value)}
            >
              {opt.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export type DateMode = "previous" | "current" | "next";
export type DateUnit = "days" | "weeks" | "months" | "quarters" | "years";

export type DateRangeValue = {
  from: string;
  to: string;
  label: string;
  key?: string;
};

export type DateRangePreset = {
  key: string;
  label: string;
  mode?: DateMode;
  n?: number;
  unit?: DateUnit;
  includeCurrent?: boolean;
  compute: () => DateRangeValue;
};

function formatYmd(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

function formatHumanYmd(ymd: string): string {
  if (!ymd) return "";
  const [y, m, d] = ymd.split("-").map(Number);
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${MONTHS[m - 1]} ${d}, ${y}`;
}

function startOfUnit(d: Date, unit: DateUnit): Date {
  const x = new Date(d);
  x.setHours(0, 0, 0, 0);
  if (unit === "days") return x;
  if (unit === "weeks") {
    x.setDate(x.getDate() - x.getDay());
    return x;
  }
  if (unit === "months") {
    x.setDate(1);
    return x;
  }
  if (unit === "quarters") {
    const q = Math.floor(x.getMonth() / 3);
    x.setMonth(q * 3);
    x.setDate(1);
    return x;
  }
  if (unit === "years") {
    x.setMonth(0);
    x.setDate(1);
    return x;
  }
  return x;
}

function addUnits(d: Date, count: number, unit: DateUnit): Date {
  const x = new Date(d);
  if (unit === "days") {
    x.setDate(x.getDate() + count);
    return x;
  }
  if (unit === "weeks") {
    x.setDate(x.getDate() + count * 7);
    return x;
  }
  if (unit === "months" || unit === "quarters" || unit === "years") {
    // Clamp the day to the target month's last day to avoid JS setMonth/setFullYear
    // overflow, e.g. Jan 31 + 1 month rolling over to Mar 3.
    const monthStep = unit === "months" ? count : unit === "quarters" ? count * 3 : count * 12;
    const targetMonth = x.getMonth() + monthStep;
    const targetYear = x.getFullYear() + Math.floor(targetMonth / 12);
    const normalizedMonth = ((targetMonth % 12) + 12) % 12;
    const lastDay = new Date(targetYear, normalizedMonth + 1, 0).getDate();
    x.setDate(Math.min(x.getDate(), lastDay));
    x.setFullYear(targetYear, normalizedMonth);
    return x;
  }
  return x;
}

function endOfUnit(d: Date, unit: DateUnit): Date {
  const next = addUnits(startOfUnit(d, unit), 1, unit);
  next.setDate(next.getDate() - 1);
  return next;
}

export function computeRelativeRange(
  mode: DateMode,
  n: number,
  unit: DateUnit,
  includeCurrent: boolean,
  today: Date = new Date(),
): { from: string; to: string } {
  let from: Date;
  let to: Date;
  if (mode === "current") {
    from = startOfUnit(today, unit);
    to = unit === "days" ? new Date(today) : endOfUnit(today, unit);
  } else if (mode === "previous") {
    if (includeCurrent) {
      from = startOfUnit(addUnits(today, -(n - 1), unit), unit);
      to = unit === "days" ? new Date(today) : endOfUnit(today, unit);
    } else {
      from = startOfUnit(addUnits(today, -n, unit), unit);
      to = unit === "days" ? addUnits(today, -1, "days") : endOfUnit(addUnits(today, -1, unit), unit);
    }
  } else {
    if (includeCurrent) {
      from = startOfUnit(today, unit);
      to = unit === "days"
        ? addUnits(today, n - 1, "days")
        : endOfUnit(addUnits(today, n - 1, unit), unit);
    } else {
      from = startOfUnit(addUnits(today, 1, unit), unit);
      to = unit === "days"
        ? addUnits(today, n, "days")
        : endOfUnit(addUnits(today, n, unit), unit);
    }
  }
  return { from: formatYmd(from), to: formatYmd(to) };
}

export function buildRelativeLabel(mode: DateMode, n: number, unit: DateUnit, includeCurrent: boolean): string {
  const singular = unit.replace(/s$/, "");
  const plural = n === 1 ? singular : unit;
  if (mode === "current") return `This ${singular}`;
  if (mode === "previous") {
    const base = `Previous ${n} ${plural}`;
    return includeCurrent ? `${base} or this ${singular}` : base;
  }
  const baseN = `Next ${n} ${plural}`;
  return includeCurrent ? `${baseN} or this ${singular}` : baseN;
}

export function makePreset(
  key: string,
  label: string,
  mode: DateMode,
  n: number,
  unit: DateUnit,
  includeCurrent: boolean,
): DateRangePreset {
  return {
    key,
    label,
    mode,
    n,
    unit,
    includeCurrent,
    compute: () => {
      const r = computeRelativeRange(mode, n, unit, includeCurrent);
      return { from: r.from, to: r.to, label, key };
    },
  };
}

export const DEFAULT_DATE_RANGE_PRESETS: DateRangePreset[] = [
  makePreset("today", "Today", "current", 1, "days", false),
  makePreset("yesterday", "Yesterday", "previous", 1, "days", false),
  makePreset("last7", "Last 7 days", "previous", 7, "days", true),
  makePreset("last30", "Last 30 days", "previous", 30, "days", true),
  makePreset("last90", "Last 90 days", "previous", 90, "days", true),
  makePreset("last3m", "Last 3 months", "previous", 3, "months", true),
  makePreset("last6m", "Last 6 months", "previous", 6, "months", true),
  makePreset("last12m", "Previous 12 months or this month", "previous", 12, "months", true),
  makePreset("mtd", "Month to date", "current", 1, "months", false),
  makePreset("ytd", "Year to date", "current", 1, "years", false),
  { key: "all", label: "All time", compute: () => ({ from: "", to: "", label: "All time", key: "all" }) },
];

function DateRangeCalendarIcon({ small }: { small?: boolean } = {}) {
  const s = small ? 12 : 14;
  return (
    <svg width={s} height={s} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
      <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
      <line x1="16" y1="2" x2="16" y2="6" />
      <line x1="8" y1="2" x2="8" y2="6" />
      <line x1="3" y1="10" x2="21" y2="10" />
    </svg>
  );
}

function DateRangeChevronIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0, opacity: 0.6 }}>
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

export function DateRangeFilter(props: {
  value: DateRangeValue;
  onChange: (v: DateRangeValue) => void;
  label?: React.ReactNode;
  presets?: DateRangePreset[];
  className?: string;
  // "below-start" (default): popover anchored below the trigger, left-aligned — good for top-of-page filters.
  // "right-start": popover escapes the trigger's container and flies out to the right of it (e.g. sidebar → main content). Uses position:fixed.
  popoverPlacement?: "below-start" | "right-start";
  triggerStyle?: React.CSSProperties;
}) {
  const presets = props.presets ?? DEFAULT_DATE_RANGE_PRESETS;
  const placement = props.popoverPlacement ?? "below-start";
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const [popoverPos, setPopoverPos] = useState<{ top: number; left: number } | null>(null);
  const [open, setOpen] = useState(false);
  const [mainTab, setMainTab] = useState<"relative" | "custom">("relative");
  const [draftMode, setDraftMode] = useState<"previous" | "next">("previous");
  const [draftN, setDraftN] = useState(12);
  const [draftUnit, setDraftUnit] = useState<DateUnit>("months");
  const [draftIncludeCurrent, setDraftIncludeCurrent] = useState(true);
  const [draftCustomFrom, setDraftCustomFrom] = useState(props.value.from || "");
  const [draftCustomTo, setDraftCustomTo] = useState(props.value.to || "");

  // Sync custom-date drafts only on open transition, not on every value change —
  // otherwise an external onChange while the popover is open would clobber the
  // user's in-progress edits.
  const prevOpenRef = useRef(false);
  useEffect(() => {
    if (open && !prevOpenRef.current) {
      setDraftCustomFrom(props.value.from || "");
      setDraftCustomTo(props.value.to || "");
    }
    prevOpenRef.current = open;
  }, [open, props.value.from, props.value.to]);

  // For "right-start" placement: measure the trigger rect on open so the popover
  // can escape its container (e.g. sidebar) and anchor next to it via position:fixed.
  useEffect(() => {
    if (!open || placement !== "right-start" || !triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    setPopoverPos({ top: rect.top, left: rect.right + 8 });
  }, [open, placement]);

  const relativePreview = useMemo(
    () => computeRelativeRange(draftMode, draftN, draftUnit, draftIncludeCurrent),
    [draftMode, draftN, draftUnit, draftIncludeCurrent],
  );

  const previewFrom = mainTab === "relative" ? relativePreview.from : draftCustomFrom;
  const previewTo = mainTab === "relative" ? relativePreview.to : draftCustomTo;

  const canApply = mainTab === "relative" ? draftN > 0 : !!(draftCustomFrom || draftCustomTo);

  const applyActive = () => {
    if (mainTab === "relative") {
      props.onChange({
        from: relativePreview.from,
        to: relativePreview.to,
        label: buildRelativeLabel(draftMode, draftN, draftUnit, draftIncludeCurrent),
        key: "relative",
      });
    } else {
      if (!draftCustomFrom && !draftCustomTo) return;
      const label = draftCustomFrom && draftCustomTo
        ? `${formatHumanYmd(draftCustomFrom)} → ${formatHumanYmd(draftCustomTo)}`
        : draftCustomFrom
          ? `After ${formatHumanYmd(draftCustomFrom)}`
          : `Before ${formatHumanYmd(draftCustomTo)}`;
      props.onChange({ from: draftCustomFrom, to: draftCustomTo, label, key: "custom" });
    }
    setOpen(false);
  };

  const applyPreset = (p: DateRangePreset) => {
    props.onChange(p.compute());
    if (p.mode && p.mode !== "current") setDraftMode(p.mode);
    if (typeof p.n === "number") setDraftN(p.n);
    if (p.unit) setDraftUnit(p.unit);
    if (typeof p.includeCurrent === "boolean") setDraftIncludeCurrent(p.includeCurrent);
    if (p.mode && p.mode !== "current") setMainTab("relative");
    setOpen(false);
  };

  return (
    <div className={props.className} style={{ position: "relative" }}>
      {props.label !== null ? (
        <div
          className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.06em]"
          style={{ color: "var(--text-muted)" }}
        >
          {props.label ?? "Date Range"}
        </div>
      ) : null}
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen((o) => !o)}
        onMouseEnter={(e) => { e.currentTarget.style.borderColor = "var(--border-hover)"; }}
        onMouseLeave={(e) => { e.currentTarget.style.borderColor = "var(--border)"; }}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 8,
          padding: "8px 12px",
          background: "var(--bg-card)",
          border: "1px solid var(--border)",
          borderRadius: 8,
          color: "var(--text-primary)",
          fontSize: 13,
          cursor: "pointer",
          minWidth: 200,
          fontWeight: 500,
          ...(props.triggerStyle ?? {}),
        }}
      >
        <DateRangeCalendarIcon />
        <span style={{ flex: 1, textAlign: "left", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {props.value.label || "Select date range"}
        </span>
        <DateRangeChevronIcon />
      </button>

      {open ? (
        <>
          <div
            style={{ position: "fixed", inset: 0, zIndex: 9998 }}
            onClick={() => setOpen(false)}
          />
          <div
            style={
              placement === "right-start"
                ? {
                    position: "fixed",
                    top: popoverPos?.top ?? 0,
                    left: popoverPos?.left ?? 0,
                    visibility: popoverPos ? "visible" : "hidden",
                    zIndex: 9999,
                    width: 560,
                    background: "var(--bg-card)",
                    border: "1px solid var(--border)",
                    borderRadius: 12,
                    boxShadow: "0 20px 40px rgba(0, 0, 0, 0.35), 0 0 0 1px var(--border)",
                    display: "grid",
                    gridTemplateColumns: "160px 1fr",
                    overflow: "hidden",
                  }
                : {
                    position: "absolute",
                    top: "calc(100% + 6px)",
                    left: 0,
                    zIndex: 9999,
                    width: 560,
                    background: "var(--bg-card)",
                    border: "1px solid var(--border)",
                    borderRadius: 12,
                    boxShadow: "0 20px 40px rgba(0, 0, 0, 0.35), 0 0 0 1px var(--border)",
                    display: "grid",
                    gridTemplateColumns: "160px 1fr",
                    overflow: "hidden",
                  }
            }
          >
            <div style={{ background: "var(--bg-elevated)", padding: 8, borderRight: "1px solid var(--border)" }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", padding: "6px 10px 10px", letterSpacing: 0.5 }}>
                Presets
              </div>
              {presets.map((p) => {
                const active = props.value.key
                  ? props.value.key === p.key
                  : props.value.label === p.label;
                return (
                  <button
                    key={p.key}
                    type="button"
                    onClick={() => applyPreset(p)}
                    onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = "var(--bg-hover)"; }}
                    onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = "transparent"; }}
                    style={{
                      display: "block",
                      width: "100%",
                      textAlign: "left",
                      padding: "7px 10px",
                      borderRadius: 6,
                      background: active ? "var(--accent-muted)" : "transparent",
                      color: active ? "var(--accent-strong)" : "var(--text-primary)",
                      border: "none",
                      cursor: "pointer",
                      fontSize: 12,
                      fontWeight: active ? 600 : 400,
                      marginBottom: 2,
                    }}
                  >
                    {p.label}
                  </button>
                );
              })}
            </div>

            <div style={{ padding: 16, display: "flex", flexDirection: "column" }}>
              <div style={{ display: "flex", gap: 2, borderBottom: "1px solid var(--border)", marginBottom: 16 }}>
                {(["relative", "custom"] as const).map((tab) => (
                  <button
                    key={tab}
                    type="button"
                    onClick={() => setMainTab(tab)}
                    style={{
                      padding: "8px 14px",
                      background: "none",
                      border: "none",
                      borderBottom: mainTab === tab ? "2px solid var(--accent)" : "2px solid transparent",
                      color: mainTab === tab ? "var(--text-primary)" : "var(--text-muted)",
                      fontSize: 13,
                      fontWeight: 500,
                      cursor: "pointer",
                      textTransform: "capitalize",
                      marginBottom: -1,
                    }}
                  >
                    {tab}
                  </button>
                ))}
              </div>

              <div style={{ flex: 1 }}>
                {mainTab === "relative" ? (
                  <>
                    <div style={{ display: "flex", gap: 4, padding: 2, background: "var(--bg-elevated)", borderRadius: 8, marginBottom: 12 }}>
                      {(["previous", "next"] as const).map((m) => (
                        <button
                          key={m}
                          type="button"
                          onClick={() => setDraftMode(m)}
                          style={{
                            flex: 1,
                            padding: "6px 10px",
                            border: "none",
                            borderRadius: 6,
                            fontSize: 12,
                            fontWeight: 500,
                            cursor: "pointer",
                            background: draftMode === m ? "var(--bg-card)" : "transparent",
                            color: draftMode === m ? "var(--text-primary)" : "var(--text-muted)",
                            textTransform: "capitalize",
                          }}
                        >
                          {m}
                        </button>
                      ))}
                    </div>

                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
                      <input
                        type="number"
                        min={1}
                        value={draftN}
                        onChange={(e) => {
                          const parsed = parseInt(e.target.value, 10);
                          setDraftN(Number.isFinite(parsed) ? Math.max(1, parsed) : 1);
                        }}
                        style={{
                          width: 60,
                          padding: "7px 10px",
                          background: "var(--bg-elevated)",
                          border: "1px solid var(--border)",
                          borderRadius: 6,
                          color: "var(--text-primary)",
                          fontSize: 13,
                        }}
                      />
                      <select
                        value={draftUnit}
                        onChange={(e) => setDraftUnit(e.target.value as DateUnit)}
                        style={{
                          flex: 1,
                          padding: "7px 10px",
                          background: "var(--bg-elevated)",
                          border: "1px solid var(--border)",
                          borderRadius: 6,
                          color: "var(--text-primary)",
                          fontSize: 13,
                        }}
                      >
                        <option value="days">days</option>
                        <option value="weeks">weeks</option>
                        <option value="months">months</option>
                        <option value="quarters">quarters</option>
                        <option value="years">years</option>
                      </select>
                    </div>

                    <label style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12, color: "var(--text-secondary)", marginBottom: 4, cursor: "pointer" }}>
                      <input
                        type="checkbox"
                        checked={draftIncludeCurrent}
                        onChange={(e) => setDraftIncludeCurrent(e.target.checked)}
                      />
                      Include this {draftUnit.replace(/s$/, "")}
                    </label>
                  </>
                ) : (
                  <div style={{ display: "flex", gap: 8 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 4 }}>From</div>
                      <input
                        type="date"
                        value={draftCustomFrom}
                        onChange={(e) => setDraftCustomFrom(e.target.value)}
                        style={{
                          width: "100%",
                          padding: "7px 10px",
                          background: "var(--bg-elevated)",
                          border: "1px solid var(--border)",
                          borderRadius: 6,
                          color: "var(--text-primary)",
                          fontSize: 13,
                        }}
                      />
                    </div>
                    <div style={{ flex: 1 }}>
                      <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 4 }}>To</div>
                      <input
                        type="date"
                        value={draftCustomTo}
                        onChange={(e) => setDraftCustomTo(e.target.value)}
                        style={{
                          width: "100%",
                          padding: "7px 10px",
                          background: "var(--bg-elevated)",
                          border: "1px solid var(--border)",
                          borderRadius: 6,
                          color: "var(--text-primary)",
                          fontSize: 13,
                        }}
                      />
                    </div>
                  </div>
                )}
              </div>

              <div style={{ padding: "8px 12px", background: "var(--bg-elevated)", borderRadius: 6, fontSize: 12, color: "var(--text-secondary)", margin: "16px 0 12px", display: "flex", alignItems: "center" }}>
                <DateRangeCalendarIcon small />
                <span style={{ marginLeft: 6 }}>
                  {previewFrom ? formatHumanYmd(previewFrom) : "—"} → {previewTo ? formatHumanYmd(previewTo) : "—"}
                </span>
              </div>

              <div style={{ display: "flex", gap: 8 }}>
                <button
                  type="button"
                  onClick={() => setOpen(false)}
                  style={{
                    flex: 1,
                    padding: "8px 14px",
                    background: "transparent",
                    color: "var(--text-secondary)",
                    border: "1px solid var(--border)",
                    borderRadius: 6,
                    fontSize: 13,
                    fontWeight: 500,
                    cursor: "pointer",
                  }}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={applyActive}
                  disabled={!canApply}
                  style={{
                    flex: 1,
                    padding: "8px 14px",
                    background: canApply ? "var(--accent)" : "var(--bg-elevated)",
                    color: canApply ? "white" : "var(--text-muted)",
                    border: "none",
                    borderRadius: 6,
                    fontSize: 13,
                    fontWeight: 500,
                    cursor: canApply ? "pointer" : "not-allowed",
                  }}
                >
                  Apply
                </button>
              </div>
            </div>
          </div>
        </>
      ) : null}
    </div>
  );
}



export function DateInput(props: {
  value: string;
  onChange: (value: string) => void;
  label?: React.ReactNode;
  max?: string;
  min?: string;
  className?: string;
}) {
  return (
    <div className={props.className}>
      {props.label ? (
        <div
          className="mb-1.5 text-[11px] font-semibold uppercase tracking-[0.06em]"
          style={{ color: "var(--text-muted)" }}
        >
          {props.label}
        </div>
      ) : null}
      <input
        type="date"
        className="rounded-lg px-3 py-2 text-sm outline-none"
        style={{
          background: "var(--bg-elevated)",
          border: "1px solid var(--border)",
          color: "var(--text-primary)",
          colorScheme: "dark",
          transition: "border-color 120ms ease, box-shadow 120ms ease",
        }}
        value={props.value}
        onChange={(e) => props.onChange(e.target.value)}
        max={props.max}
        min={props.min}
        onFocus={(e) => { e.currentTarget.style.borderColor = "var(--accent)"; e.currentTarget.style.boxShadow = "0 0 0 3px var(--accent-muted)"; }}
        onBlur={(e) => { e.currentTarget.style.borderColor = "var(--border)"; e.currentTarget.style.boxShadow = "none"; }}
      />
    </div>
  );
}

export function Tooltip(props: {
  content: React.ReactNode;
  children: React.ReactNode;
  position?: "top" | "bottom";
  maxWidth?: number;
}) {
  const [show, setShow] = useState(false);
  const pos = props.position ?? "top";
  const maxW = props.maxWidth ?? 240;

  return (
    <span
      style={{ position: "relative", display: "inline-flex", alignItems: "center" }}
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      {props.children}
      {show ? (
        <span
          style={{
            position: "absolute",
            ...(pos === "top"
              ? { bottom: "calc(100% + 6px)" }
              : { top: "calc(100% + 6px)" }),
            left: "50%",
            transform: "translateX(-50%)",
            padding: "8px 12px",
            borderRadius: 8,
            fontSize: 12,
            lineHeight: 1.5,
            whiteSpace: "normal",
            maxWidth: maxW,
            width: "max-content",
            background: "var(--bg-elevated)",
            color: "var(--text-secondary)",
            border: "1px solid var(--border)",
            boxShadow: "var(--shadow-card-hover)",
            zIndex: 50,
            pointerEvents: "none",
            animation: "fade-up 0.12s ease-out",
          }}
        >
          {props.content}
        </span>
      ) : null}
    </span>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// Refined SaaS v2 — opinionated shell primitives
// ═══════════════════════════════════════════════════════════════════════════
// A second styling track alongside the CSS-vars-based components above. These
// primitives drive inline styles off a Palette object so customers can pass an
// accentColor at the ShellLayout root and every nested chrome element (KPI
// top-line, active nav, filter chip, loading dot) picks it up without CSS var
// leaks. Pair with <PaletteProvider>; inside that, call usePalette() from any
// descendant.

export type SaasPaletteTheme = "dark" | "light";

export type SaasPalette = {
  bg: string; sidebar: string; card: string; elev: string;
  text: string; sub: string; dim: string; faint: string;
  border: string; rule: string;
  accent: string; accentSoft: string;
  ok: string; okSoft: string;
  warn: string; warnSoft: string;
  bad: string; badSoft: string;
  grad2: string;
  sans: string; mono: string;
};

// Derive a soft alpha from a hex accent. Falls back to palette default.
function alphaOf(hex: string, alpha: number): string {
  const m = /^#?([0-9a-fA-F]{6})$/.exec(hex.trim());
  if (!m) return hex;
  const n = parseInt(m[1], 16);
  const r = (n >> 16) & 0xff, g = (n >> 8) & 0xff, b = n & 0xff;
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

export function buildPalette(
  theme: SaasPaletteTheme,
  opts?: { accent?: string },
): SaasPalette {
  const base = theme === "dark"
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
  const accent = opts?.accent ?? base.accent;
  const accentSoft = opts?.accent
    ? alphaOf(opts.accent, theme === "dark" ? 0.14 : 0.10)
    : base.accentSoft;
  return {
    ...base,
    accent,
    accentSoft,
    sans: '"Inter", system-ui, sans-serif',
    mono: '"JetBrains Mono", ui-monospace, monospace',
  };
}

const PaletteContext = React.createContext<SaasPalette | null>(null);

export function PaletteProvider(props: { value: SaasPalette; children: React.ReactNode }) {
  return <PaletteContext.Provider value={props.value}>{props.children}</PaletteContext.Provider>;
}

export function usePalette(): SaasPalette {
  const v = React.useContext(PaletteContext);
  if (!v) throw new Error("usePalette() requires a <PaletteProvider> ancestor (typically provided by <ShellLayout>).");
  return v;
}

// Keyframes used by skeletons, drill drawer, cache popover. Injected once per
// document by any component that mounts. Safe to re-inject — browsers dedupe.
const SAAS_KEYFRAMES = `
  @keyframes saasShimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
  @keyframes saasPulse { 0%, 100% { opacity: 0.3; } 50% { opacity: 1; } }
  @keyframes saasFade { from { opacity: 0; } to { opacity: 1; } }
  @keyframes saasSlide { from { transform: translateX(100%); } to { transform: translateX(0); } }
`;

function useSaasKeyframes() {
  useEffect(() => {
    const id = "saas-v2-keyframes";
    if (document.getElementById(id)) return;
    const s = document.createElement("style");
    s.id = id;
    s.textContent = SAAS_KEYFRAMES;
    document.head.appendChild(s);
  }, []);
}

// ── Sparkline ──────────────────────────────────────────────────────────────

export function Sparkline(props: {
  values: number[];
  color?: string;
  width?: number;
  height?: number;
}) {
  const P = React.useContext(PaletteContext);
  const color = props.color ?? P?.accent ?? "#0A99FF";
  const w = props.width ?? 90;
  const h = props.height ?? 32;
  if (!props.values.length) return <svg width={w} height={h} />;
  const mx = Math.max(...props.values);
  const mn = Math.min(...props.values);
  const rng = mx - mn || 1;
  const pts = props.values
    .map((v, i) => `${(i / (props.values.length - 1)) * w},${h - ((v - mn) / rng) * h}`)
    .join(" ");
  const last = props.values[props.values.length - 1];
  return (
    <svg width={w} height={h} style={{ display: "block", overflow: "visible" }}>
      <polyline points={pts} fill="none" stroke={color} strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />
      <circle cx={w} cy={h - ((last - mn) / rng) * h} r={2.5} fill={color} />
    </svg>
  );
}

// ── SkeletonShimmer ────────────────────────────────────────────────────────

export function SkeletonShimmer(props: {
  width?: number | string;
  height?: number | string;
  radius?: number;
  style?: React.CSSProperties;
}) {
  const P = usePalette();
  useSaasKeyframes();
  return (
    <div
      style={{
        width: props.width ?? "100%",
        height: props.height ?? 16,
        borderRadius: props.radius ?? 4,
        background: `linear-gradient(90deg, ${P.elev} 0%, ${P.border} 50%, ${P.elev} 100%)`,
        backgroundSize: "200% 100%",
        animation: "saasShimmer 1.4s ease-in-out infinite",
        ...(props.style ?? {}),
      }}
    />
  );
}

// ── Breadcrumb ─────────────────────────────────────────────────────────────

export function Breadcrumb(props: { trail: string[] }) {
  const P = usePalette();
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      {props.trail.map((t, i) => (
        <React.Fragment key={i}>
          {i > 0 ? <span style={{ color: P.faint }}>/</span> : null}
          <span style={{ fontSize: 12, color: i === props.trail.length - 1 ? P.text : P.dim }}>{t}</span>
        </React.Fragment>
      ))}
    </div>
  );
}

// ── SaasKpiCard ────────────────────────────────────────────────────────────

export function SaasKpiCard(props: {
  title: string;
  value: React.ReactNode;
  delta?: string;
  up?: boolean;
  sub?: string;
  spark?: number[];
  accent?: string;
  loading?: boolean;
  onClick?: () => void;
}) {
  const P = usePalette();
  const accent = props.accent ?? P.accent;
  const clickable = Boolean(props.onClick);
  return (
    <div
      onClick={props.onClick}
      onMouseEnter={(e) => {
        if (!clickable) return;
        e.currentTarget.style.transform = "translateY(-1px)";
        e.currentTarget.style.borderColor = P.rule;
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.transform = "none";
        e.currentTarget.style.borderColor = P.border;
      }}
      style={{
        background: P.card,
        border: `1px solid ${P.border}`,
        borderRadius: 10,
        padding: 16,
        position: "relative",
        overflow: "hidden",
        cursor: clickable ? "pointer" : "default",
        transition: "transform 0.15s, border-color 0.15s",
      }}
    >
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: `linear-gradient(90deg, ${accent}, transparent)` }} />
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
        <div style={{ fontSize: 12, color: P.sub, fontWeight: 500 }}>{props.title}</div>
        {!props.loading && props.spark && props.spark.length > 0
          ? <Sparkline values={props.spark} color={accent} />
          : null}
      </div>
      {props.loading ? (
        <>
          <SkeletonShimmer width={120} height={28} radius={6} />
          <div style={{ height: 10 }} />
          <SkeletonShimmer width={140} height={12} />
        </>
      ) : (
        <>
          <div style={{ fontSize: 28, fontWeight: 600, letterSpacing: "-0.02em", lineHeight: 1, marginBottom: 8 }}>{props.value}</div>
          {(props.delta || props.sub) ? (
            <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
              {props.delta ? (
                <span style={{
                  color: props.up ? P.ok : P.bad,
                  background: props.up ? P.okSoft : P.badSoft,
                  padding: "2px 7px", borderRadius: 4,
                  fontFamily: P.mono, fontWeight: 500,
                }}>
                  {props.up ? "↑" : "↓"} {props.delta}
                </span>
              ) : null}
              {props.sub ? <span style={{ color: P.dim }}>{props.sub}</span> : null}
            </div>
          ) : null}
        </>
      )}
    </div>
  );
}

// ── CachePopover ───────────────────────────────────────────────────────────

export function CachePopover(props: {
  isLoading: boolean;
  rowCount?: number | null;
  cache?: {
    loadTimeMs: number | null;
    fromCache: boolean;
    sourceLabel: string;
    cacheTtlHours: number;
  } | null;
  onRefresh: () => Promise<void> | void;
}) {
  const P = usePalette();
  useSaasKeyframes();
  const [open, setOpen] = useState(false);
  const loadSec = props.cache?.loadTimeMs != null ? (props.cache.loadTimeMs / 1000).toFixed(2) + "s" : "—";
  return (
    <div style={{ position: "relative" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          display: "flex", alignItems: "center", gap: 6, fontSize: 11, padding: "6px 10px",
          background: props.isLoading ? P.accentSoft : P.okSoft,
          color: props.isLoading ? P.accent : P.ok,
          borderRadius: 6, fontFamily: P.mono,
          border: `1px solid ${open ? (props.isLoading ? P.accent : P.ok) : "transparent"}`,
          cursor: "pointer",
        }}
      >
        <span style={{
          width: 6, height: 6, borderRadius: "50%",
          background: props.isLoading ? P.accent : P.ok,
          boxShadow: `0 0 8px ${props.isLoading ? P.accent : P.ok}`,
          animation: props.isLoading ? "saasPulse 0.9s ease-in-out infinite" : "none",
        }} />
        {props.isLoading ? "Loading…" : props.cache?.fromCache ? "Cached" : "Live"}
        <span style={{ marginLeft: 2, opacity: 0.7, fontSize: 9 }}>▾</span>
      </button>
      {open ? (
        <>
          <div onClick={() => setOpen(false)} style={{ position: "fixed", inset: 0, zIndex: 40 }} />
          <div style={{
            position: "absolute", top: "calc(100% + 6px)", right: 0, zIndex: 50,
            width: 320, background: P.card, border: `1px solid ${P.border}`, borderRadius: 10,
            boxShadow: "0 20px 60px rgba(0,0,0,0.35)", padding: 14, fontFamily: P.sans, color: P.text,
          }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ width: 7, height: 7, borderRadius: "50%", background: P.ok, boxShadow: `0 0 10px ${P.ok}` }} />
                <div style={{ fontSize: 13, fontWeight: 600 }}>DuckDB WASM</div>
              </div>
              <div style={{ fontSize: 10, color: P.dim, fontFamily: P.mono }}>v1.29.0</div>
            </div>
            <div style={{ fontSize: 11, color: P.sub, lineHeight: 1.5, marginBottom: 12 }}>
              Query results cached in-browser via IndexedDB. Subsequent loads of the same
              slice render instantly without hitting the warehouse.
            </div>
            {([
              ["Rows cached", props.rowCount != null ? props.rowCount.toLocaleString() : "—"],
              ["Source", props.cache?.sourceLabel ?? "—"],
              ["Load time", loadSec],
              ["From cache", props.cache?.fromCache ? "yes" : "no"],
              ["TTL", `${props.cache?.cacheTtlHours ?? 24}h`],
            ] as const).map(([k, v]) => (
              <div key={k} style={{
                display: "flex", justifyContent: "space-between", alignItems: "baseline",
                padding: "6px 0", borderTop: `1px solid ${P.border}`,
              }}>
                <div style={{ fontSize: 11, color: P.sub }}>{k}</div>
                <div style={{ fontSize: 12, fontWeight: 500, fontFamily: P.mono }}>{v}</div>
              </div>
            ))}
            <button
              onClick={async () => { await props.onRefresh(); setOpen(false); }}
              style={{
                width: "100%", marginTop: 12, fontSize: 11, padding: "6px 10px", borderRadius: 5,
                background: P.accent, border: `1px solid ${P.accent}`, color: "#fff",
                cursor: "pointer", fontFamily: P.sans, fontWeight: 500,
              }}
            >
              Clear cache & reload
            </button>
          </div>
        </>
      ) : null}
    </div>
  );
}

// ── FilterAccordion ────────────────────────────────────────────────────────

export type FilterAccordionOption = {
  id: string;
  label: string;
  hint?: string;
  swatch?: string;
};

export type FilterAccordionGroup = {
  id: string;
  label: string;
  options: FilterAccordionOption[];
};

export function FilterAccordion(props: {
  groups: FilterAccordionGroup[];
  selected: Record<string, string[]>;
  onChange: React.Dispatch<React.SetStateAction<Record<string, string[]>>>;
  search: string;
  onSearchChange: (s: string) => void;
  defaultExpanded?: Record<string, boolean>;
}) {
  const P = usePalette();
  const [expanded, setExpanded] = useState<Record<string, boolean>>(props.defaultExpanded ?? {});
  const [groupSearch, setGroupSearch] = useState<Record<string, string>>({});

  const toggle = (gid: string, optId: string) => {
    props.onChange((f) => {
      const cur = f[gid] || [];
      const next = cur.includes(optId) ? cur.filter((x) => x !== optId) : [...cur, optId];
      const copy = { ...f };
      if (next.length === 0) delete copy[gid]; else copy[gid] = next;
      return copy;
    });
  };
  const clearGroup = (gid: string) => props.onChange((f) => { const c = { ...f }; delete c[gid]; return c; });

  const q = props.search.toLowerCase();

  return (
    <>
      <div style={{ padding: "0 10px 8px", position: "relative" }}>
        <input
          value={props.search}
          onChange={(e) => props.onSearchChange(e.target.value)}
          placeholder="Search filters…"
          style={{
            width: "100%", padding: "6px 10px 6px 26px", fontSize: 11,
            background: P.elev, border: `1px solid ${P.border}`, color: P.text,
            borderRadius: 5, outline: "none", fontFamily: P.sans,
          }}
        />
        <span style={{ position: "absolute", left: 19, top: 6, fontSize: 11, color: P.faint, pointerEvents: "none" }}>⌕</span>
      </div>
      <div style={{ flex: 1, overflowY: "auto", marginRight: -4, paddingRight: 4 }}>
        {props.groups.map((grp) => {
          const selectedIds = props.selected[grp.id] || [];
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
                  fontSize: 12, fontFamily: P.sans, borderRadius: 5, gap: 8,
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
                  <span style={{ fontSize: 10, padding: "1px 6px", borderRadius: 8, background: P.accent, color: "#fff", fontFamily: P.mono, fontWeight: 600 }}>
                    {selectedIds.length}
                  </span>
                ) : (
                  <span style={{ fontSize: 10, color: P.faint, fontFamily: P.mono }}>{grp.options.length}</span>
                )}
              </button>
              {isOpen ? (
                <div style={{
                  padding: "2px 6px 8px 22px",
                  maxHeight: visible.length > 7 ? 200 : "auto",
                  overflowY: visible.length > 7 ? "auto" : "visible",
                }}>
                  {hasGroupSearch ? (
                    <div style={{ position: "relative", marginBottom: 4, marginTop: 2 }}>
                      <input
                        value={groupSearch[grp.id] || ""}
                        onChange={(e) => setGroupSearch((s) => ({ ...s, [grp.id]: e.target.value }))}
                        placeholder={`Search ${grp.label.toLowerCase()}…`}
                        style={{
                          width: "100%", padding: "4px 22px 4px 22px", fontSize: 11,
                          background: P.elev, border: `1px solid ${P.border}`, color: P.text,
                          borderRadius: 4, outline: "none", fontFamily: P.sans,
                        }}
                      />
                      <span style={{ position: "absolute", left: 7, top: 4, fontSize: 10, color: P.faint, pointerEvents: "none" }}>⌕</span>
                    </div>
                  ) : null}
                  {selectedIds.length > 0 ? (
                    <button onClick={() => clearGroup(grp.id)} style={{
                      fontSize: 10, color: P.dim, background: "none", border: "none",
                      cursor: "pointer", padding: "2px 4px", marginBottom: 2, fontFamily: P.sans,
                    }}>Clear {grp.label.toLowerCase()}</button>
                  ) : null}
                  {visible.length === 0 ? (
                    <div style={{ fontSize: 11, color: P.faint, padding: "6px 4px", fontStyle: "italic" }}>No matches</div>
                  ) : null}
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
                          {checked ? <span style={{ color: "#fff", fontSize: 9, lineHeight: 1, fontWeight: 700 }}>✓</span> : null}
                        </span>
                        <input type="checkbox" checked={checked} onChange={() => toggle(grp.id, opt.id)} style={{ display: "none" }} />
                        {opt.swatch ? <span style={{ width: 7, height: 7, borderRadius: 2, background: opt.swatch, flexShrink: 0 }} /> : null}
                        <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {opt.label}
                        </span>
                        {opt.hint != null ? (
                          <span style={{ fontFamily: P.mono, fontSize: 10, color: P.faint, flexShrink: 0 }}>{opt.hint}</span>
                        ) : null}
                      </label>
                    );
                  })}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </>
  );
}

// ── Sidebar ────────────────────────────────────────────────────────────────

export type SidebarNavItem = {
  id: string;
  label: string;
  icon?: React.ReactNode;
  badge?: React.ReactNode;
};

export function Sidebar(props: {
  logo?: { title: string; subtitle?: string; mark?: React.ReactNode };
  navItems: SidebarNavItem[];
  activeView: string;
  onViewChange: (id: string) => void;
  dateRangeSlot?: React.ReactNode;
  filterGroups?: FilterAccordionGroup[];
  filters?: Record<string, string[]>;
  onFiltersChange?: React.Dispatch<React.SetStateAction<Record<string, string[]>>>;
  humanizeFilter?: (groupId: string, value: string) => string;
  theme?: SaasPaletteTheme;
  onThemeChange?: (t: SaasPaletteTheme) => void;
  footer?: React.ReactNode;
  width?: number;
}) {
  const P = usePalette();
  const [filterSearch, setFilterSearch] = useState("");
  const width = props.width ?? 240;
  const activeFilterCount = Object.values(props.filters ?? {}).reduce((a, b) => a + b.length, 0);
  const humanize = props.humanizeFilter ?? ((_g, v) => v);

  return (
    <div style={{
      width, background: P.sidebar, borderRight: `1px solid ${P.border}`,
      padding: "20px 10px", display: "flex", flexDirection: "column", gap: 2,
      position: "sticky", top: 0, height: "100vh", zIndex: 50, flexShrink: 0,
    }}>
      {props.logo ? (
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 8px 20px" }}>
          {props.logo.mark ?? (
            <div style={{
              width: 28, height: 28, borderRadius: 7,
              background: `linear-gradient(135deg, ${P.accent}, ${P.grad2})`,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 13, fontWeight: 700, color: "#fff",
            }}>◆</div>
          )}
          <div>
            <div style={{ fontSize: 13, fontWeight: 600 }}>{props.logo.title}</div>
            {props.logo.subtitle ? <div style={{ fontSize: 11, color: P.dim }}>{props.logo.subtitle}</div> : null}
          </div>
        </div>
      ) : null}

      <SidebarSectionLabel>Views</SidebarSectionLabel>
      {props.navItems.map((n) => {
        const active = props.activeView === n.id;
        return (
          <button key={n.id} onClick={() => props.onViewChange(n.id)} style={{
            display: "flex", alignItems: "center", gap: 10, textAlign: "left",
            padding: "8px 10px", borderRadius: 6,
            background: active ? P.accentSoft : "transparent",
            color: active ? P.accent : P.sub,
            border: "none", cursor: "pointer", fontSize: 13,
            fontWeight: active ? 500 : 400, fontFamily: P.sans,
          }}
            onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = P.elev; }}
            onMouseLeave={(e) => { if (!active) e.currentTarget.style.background = "transparent"; }}
          >
            {n.icon ? <span style={{ width: 14, textAlign: "center", opacity: 0.8 }}>{n.icon}</span> : null}
            <span style={{ flex: 1 }}>{n.label}</span>
            {n.badge != null ? (
              <span style={{
                fontSize: 10, padding: "1px 6px", borderRadius: 8,
                background: active ? P.accent + "30" : P.elev,
                color: active ? P.accent : P.dim,
                fontFamily: P.mono, fontWeight: 500,
              }}>{n.badge}</span>
            ) : null}
          </button>
        );
      })}

      {props.dateRangeSlot ? (
        <div style={{ padding: "14px 10px 4px" }}>
          <SidebarSectionLabel>Date range</SidebarSectionLabel>
          {props.dateRangeSlot}
        </div>
      ) : null}

      {props.filterGroups && props.filters && props.onFiltersChange ? (
        <>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "20px 10px 6px" }}>
            <div style={{ fontSize: 10, color: P.faint, letterSpacing: "0.08em", textTransform: "uppercase", fontWeight: 600 }}>
              Filters {activeFilterCount > 0 ? <span style={{ color: P.accent, marginLeft: 4 }}>· {activeFilterCount}</span> : null}
            </div>
            {activeFilterCount > 0 ? (
              <button onClick={() => props.onFiltersChange!({})} style={{
                fontSize: 10, color: P.dim, background: "none", border: "none", cursor: "pointer",
                fontFamily: P.sans, padding: "2px 4px",
              }}>Clear</button>
            ) : null}
          </div>
          {activeFilterCount > 0 ? (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 4, padding: "0 10px 8px" }}>
              {Object.entries(props.filters).map(([gid, opts]) => {
                const grp = props.filterGroups!.find((g) => g.id === gid);
                if (!grp || !opts?.length) return null;
                const single = opts.length === 1;
                const label = single ? humanize(gid, opts[0]) : `${grp.label} · ${opts.length}`;
                return (
                  <button key={gid}
                    onClick={() => props.onFiltersChange!((f) => { const c = { ...f }; delete c[gid]; return c; })}
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
          ) : null}

          <FilterAccordion
            groups={props.filterGroups}
            selected={props.filters}
            onChange={props.onFiltersChange}
            search={filterSearch}
            onSearchChange={setFilterSearch}
          />
        </>
      ) : null}

      <div style={{ marginTop: "auto", paddingTop: 14, borderTop: `1px solid ${P.border}`, display: "flex", flexDirection: "column", gap: 10 }}>
        {props.theme && props.onThemeChange ? (
          <div style={{ display: "flex", gap: 3, background: P.elev, borderRadius: 6, padding: 3 }}>
            {(["dark", "light"] as const).map((tk) => (
              <button key={tk}
                onClick={() => props.onThemeChange!(tk)}
                style={{
                  flex: 1, padding: "5px 8px", fontSize: 11, border: "none", borderRadius: 4, cursor: "pointer",
                  background: props.theme === tk ? P.card : "transparent",
                  color: props.theme === tk ? P.text : P.sub,
                  fontFamily: P.sans, textTransform: "capitalize",
                  fontWeight: props.theme === tk ? 500 : 400,
                }}
              >{tk === "dark" ? "◐ Dark" : "◑ Light"}</button>
            ))}
          </div>
        ) : null}
        {props.footer ? (
          <div style={{ fontSize: 10, color: P.dim, fontFamily: P.mono, lineHeight: 1.5, padding: "0 4px" }}>
            {props.footer}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function SidebarSectionLabel({ children }: { children: React.ReactNode }) {
  const P = usePalette();
  return (
    <div style={{
      fontSize: 10, color: P.faint, letterSpacing: "0.08em",
      textTransform: "uppercase", padding: "8px 10px 6px", fontWeight: 600,
    }}>
      {children}
    </div>
  );
}

// ── ShellLayout ────────────────────────────────────────────────────────────

export function ShellLayout(props: {
  palette: SaasPalette;
  sidebar: React.ReactNode;
  title?: string;
  breadcrumb?: string[];
  headerRight?: React.ReactNode;
  children: React.ReactNode;
  mainPadding?: string;
}) {
  const P = props.palette;
  useSaasKeyframes();
  return (
    <PaletteProvider value={P}>
      <div style={{
        background: P.bg, color: P.text, fontFamily: P.sans, fontSize: 14,
        minHeight: "100vh", display: "flex",
      }}>
        {props.sidebar}
        <div style={{ flex: 1, minWidth: 0, padding: props.mainPadding ?? "28px 36px 48px" }}>
          {(props.title || props.breadcrumb || props.headerRight) ? (
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 24 }}>
              <div>
                {props.breadcrumb ? <div style={{ marginBottom: 6 }}><Breadcrumb trail={props.breadcrumb} /></div> : null}
                {props.title ? (
                  <h1 style={{ fontSize: 28, fontWeight: 600, letterSpacing: "-0.02em", margin: 0 }}>{props.title}</h1>
                ) : null}
              </div>
              {props.headerRight ? <div style={{ display: "flex", gap: 8, alignItems: "center" }}>{props.headerRight}</div> : null}
            </div>
          ) : null}
          {props.children}
        </div>
      </div>
    </PaletteProvider>
  );
}

// ── Drill drawer + provider ────────────────────────────────────────────────

export type DrillEntity = {
  kind: "kpi" | "row" | "chart";
  id: string;
  title: string;
  value?: React.ReactNode;
  subvalue?: string;
  breadcrumb?: string;
  sql?: string;
  stats?: Array<[string, string]>;
  breakdown?: Array<{ label: string; value: number }>;
  narrative?: React.ReactNode;
  extra?: React.ReactNode;
};

// Optional AI follow-up chat rendered at the bottom of the drill drawer.
// Apps wire `onAsk` to whatever backend they prefer — typically Definite's
// fi-fast endpoint via the callFiFast() helper below. Returning a string
// appends it as an "agent" message; throwing shows the error as a message.
export type DrillAiChatConfig = {
  onAsk: (userMessage: string, entity: DrillEntity) => Promise<string>;
  placeholder?: string;
  disclaimer?: string;
};

// ── Fi Inspect ────────────────────────────────────────────────────────────
// Lets a parent host (wax) toggle "inspect mode," in which the user picks a
// component inside the data app to scope a follow-on Fi question or edit.
//
// Components wrap themselves in <FiInspectable fiId="..." datum={row}> to
// register with a runtime registry. Parent enters inspect mode by sending
// the bridge's `definite:inspect-mode-enter` message; the overlay highlights
// registered nodes on hover and posts the picked target back as
// `definite:inspect-target`. Selection is ephemeral — there is no
// persistence. The host attaches the payload to its next Fi prompt.

export type FiInspectTargetPayload = {
  fiId: string;
  datasetKey?: string | null;
  description?: string | null;
  datum?: unknown;
  rect: { x: number; y: number; width: number; height: number };
  textPreview?: string | null;
  sourceLoc?: string | null;
};

type FiInspectableEntry = {
  id: string;
  fiId: string;
  datasetKey: string | null;
  description: string | null;
  sourceLoc: string | null;
  getDatum: () => unknown;
  node: HTMLElement;
};

type FiInspectRegistry = {
  register: (entry: FiInspectableEntry) => () => void;
  // Returns the deepest registered inspectable at viewport (clientX, clientY).
  pick: (clientX: number, clientY: number) => FiInspectableEntry | null;
};

const FiInspectContext = React.createContext<FiInspectRegistry | null>(null);

export function FiInspectProvider(props: { children: React.ReactNode }) {
  const entriesRef = React.useRef<Map<string, FiInspectableEntry>>(new Map());

  const registry = useMemo<FiInspectRegistry>(() => ({
    register(entry) {
      entriesRef.current.set(entry.id, entry);
      return () => {
        entriesRef.current.delete(entry.id);
      };
    },
    pick(clientX, clientY) {
      // Walk the DOM stack at the cursor; the topmost element belonging to
      // a registered inspectable wins. We iterate elementsFromPoint() so a
      // chart card containing a smaller "datapoint" inspectable will yield
      // the datapoint, not the card.
      const stack = typeof document.elementsFromPoint === "function"
        ? document.elementsFromPoint(clientX, clientY)
        : [];
      const reverseLookup = new Map<HTMLElement, FiInspectableEntry>();
      for (const entry of entriesRef.current.values()) {
        reverseLookup.set(entry.node, entry);
      }
      for (const el of stack) {
        if (!(el instanceof HTMLElement)) continue;
        // Walk up from this element to the document — first ancestor that
        // is a registered inspectable wins. This way an unregistered child
        // element still resolves to the nearest registered ancestor.
        let cur: HTMLElement | null = el;
        while (cur) {
          const hit = reverseLookup.get(cur);
          if (hit) return hit;
          cur = cur.parentElement;
        }
      }
      return null;
    },
  }), []);

  return (
    <FiInspectContext.Provider value={registry}>
      {props.children}
      <InspectOverlay registry={registry} />
    </FiInspectContext.Provider>
  );
}

export function useFiInspect(): FiInspectRegistry | null {
  return React.useContext(FiInspectContext);
}

export function FiInspectable(props: {
  fiId: string;
  datum?: unknown;
  datasetKey?: string;
  description?: string;
  sourceLoc?: string;
  className?: string;
  style?: React.CSSProperties;
  children: React.ReactNode;
  /** Render as an inline span instead of a block div. */
  inline?: boolean;
}) {
  const registry = React.useContext(FiInspectContext);
  const nodeRef = React.useRef<HTMLElement | null>(null);
  // Snapshot datum in a ref so the overlay reads the latest value at click
  // time without re-registering on every render.
  const datumRef = React.useRef(props.datum);
  React.useEffect(() => { datumRef.current = props.datum; }, [props.datum]);

  React.useEffect(() => {
    if (!registry || !nodeRef.current) return;
    const id = `${props.fiId}::${Math.random().toString(36).slice(2, 10)}`;
    const unregister = registry.register({
      id,
      fiId: props.fiId,
      datasetKey: props.datasetKey ?? null,
      description: props.description ?? null,
      sourceLoc: props.sourceLoc ?? null,
      getDatum: () => datumRef.current,
      node: nodeRef.current,
    });
    return unregister;
  }, [registry, props.fiId, props.datasetKey, props.description, props.sourceLoc]);

  const setRef = React.useCallback((el: HTMLElement | null) => {
    nodeRef.current = el;
  }, []);

  const dataAttrs = {
    "data-fi-id": props.fiId,
    "data-fi-dataset": props.datasetKey ?? undefined,
  };
  if (props.inline) {
    return (
      <span ref={setRef} className={props.className} style={props.style} {...dataAttrs}>
        {props.children}
      </span>
    );
  }
  return (
    <div ref={setRef} className={props.className} style={props.style} {...dataAttrs}>
      {props.children}
    </div>
  );
}

function InspectOverlay({ registry }: { registry: FiInspectRegistry }) {
  const [active, setActive] = useState(false);
  const [hover, setHover] = useState<{ x: number; y: number; w: number; h: number; label: string } | null>(null);
  const activeRef = React.useRef(active);
  React.useEffect(() => { activeRef.current = active; }, [active]);

  React.useEffect(() => {
    function onEnter() { setActive(true); }
    function onExit() { setActive(false); setHover(null); }
    window.addEventListener("definite:inspect-mode-enter", onEnter);
    window.addEventListener("definite:inspect-mode-exit", onExit);
    return () => {
      window.removeEventListener("definite:inspect-mode-enter", onEnter);
      window.removeEventListener("definite:inspect-mode-exit", onExit);
    };
  }, []);

  // Keep the cursor class in sync with React's inspect state so a click-to-pick
  // (which exits locally) doesn't leave a stale crosshair if the parent never
  // echoes back an exit message.
  React.useEffect(() => {
    const root = document.documentElement;
    if (active) root.classList.add("definite-inspecting");
    else root.classList.remove("definite-inspecting");
    return () => { root.classList.remove("definite-inspecting"); };
  }, [active]);

  React.useEffect(() => {
    if (!active) return;

    function onMove(e: MouseEvent) {
      const hit = registry.pick(e.clientX, e.clientY);
      if (!hit) {
        setHover(null);
        return;
      }
      const r = hit.node.getBoundingClientRect();
      setHover({
        x: r.x, y: r.y, w: r.width, h: r.height,
        label: hit.fiId + (hit.datasetKey ? ` · ${hit.datasetKey}` : ""),
      });
    }

    function onClick(e: MouseEvent) {
      const hit = registry.pick(e.clientX, e.clientY);
      if (!hit) return;
      e.preventDefault();
      e.stopPropagation();
      const r = hit.node.getBoundingClientRect();
      const textPreview = (hit.node.textContent ?? "").trim().slice(0, 200);
      const payload: FiInspectTargetPayload = {
        fiId: hit.fiId,
        datasetKey: hit.datasetKey,
        description: hit.description,
        sourceLoc: hit.sourceLoc,
        datum: hit.getDatum(),
        rect: { x: r.x, y: r.y, width: r.width, height: r.height },
        textPreview: textPreview.length ? textPreview : null,
      };
      try {
        window.parent?.postMessage({ type: "definite:inspect-target", payload }, "*");
      }
      catch {
        // Non-iframe context (preview / standalone) — surface via custom event.
        window.dispatchEvent(new CustomEvent("definite:inspect-target", { detail: payload }));
      }
      // Auto-exit after a pick. Parent can re-enter for another selection.
      setActive(false);
      setHover(null);
    }

    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        setActive(false);
        setHover(null);
        try {
          window.parent?.postMessage({ type: "definite:inspect-cancelled" }, "*");
        }
        catch {/* ignore */}
      }
    }

    document.addEventListener("mousemove", onMove, true);
    document.addEventListener("click", onClick, true);
    window.addEventListener("keydown", onKey, true);
    return () => {
      document.removeEventListener("mousemove", onMove, true);
      document.removeEventListener("click", onClick, true);
      window.removeEventListener("keydown", onKey, true);
    };
  }, [active, registry]);

  if (!active) return null;

  return (
    <>
      {/* Crosshair cursor + non-blocking dimmer. We don't intercept clicks
          via a top-level layer because that breaks elementsFromPoint hits;
          instead the document-level capture-phase handlers above take over. */}
      <style>{`html.definite-inspecting, html.definite-inspecting * { cursor: crosshair !important; }`}</style>
      <div
        style={{
          position: "fixed",
          inset: 0,
          pointerEvents: "none",
          zIndex: 2147483646,
          background: "rgba(8, 12, 24, 0.04)",
        }}
      />
      {hover && (
        <div
          style={{
            position: "fixed",
            left: hover.x,
            top: hover.y,
            width: hover.w,
            height: hover.h,
            pointerEvents: "none",
            border: "2px solid #6366f1",
            borderRadius: 6,
            background: "rgba(99, 102, 241, 0.12)",
            boxShadow: "0 0 0 1px rgba(255,255,255,0.6) inset",
            zIndex: 2147483647,
            transition: "left 60ms linear, top 60ms linear, width 60ms linear, height 60ms linear",
          }}
        >
          <div
            style={{
              position: "absolute",
              left: 0,
              top: -22,
              padding: "2px 6px",
              fontSize: 11,
              fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
              color: "#fff",
              background: "#6366f1",
              borderRadius: 4,
              whiteSpace: "nowrap",
              maxWidth: "60vw",
              overflow: "hidden",
              textOverflow: "ellipsis",
            }}
          >
            {hover.label}
          </div>
        </div>
      )}
    </>
  );
}

type DrillContextValue = {
  open: (e: DrillEntity) => void;
  close: () => void;
};

const DrillContext = React.createContext<DrillContextValue | null>(null);

export function DrillProvider(props: {
  children: React.ReactNode;
  aiChat?: DrillAiChatConfig;
}) {
  const [entity, setEntity] = useState<DrillEntity | null>(null);
  const ctx = useMemo<DrillContextValue>(
    () => ({ open: (e) => setEntity(e), close: () => setEntity(null) }),
    [],
  );
  return (
    <DrillContext.Provider value={ctx}>
      <FiInspectProvider>
        {props.children}
        <DrillDrawer entity={entity} onClose={ctx.close} aiChat={props.aiChat} />
      </FiInspectProvider>
    </DrillContext.Provider>
  );
}

// ── fi-fast helper ─────────────────────────────────────────────────────────
// One-shot wrapper around Definite's /v4/fi-fast endpoint. The response
// shape is Gemini-style: { content: { role, parts: [{ text }] }, usage, ... }.
// We extract parts[0].text. Callers handle auth — pass an `authToken` if the
// host needs Bearer auth, or rely on same-origin cookies (credentials: "include").

export type FiFastOptions = {
  endpoint?: string;         // default: "/v4/fi-fast"
  prompt: string;
  system?: string;
  model?: string;            // default: "gemini-3-flash-preview"
  temperature?: number;      // default: 0.1
  maxOutputTokens?: number;  // default: 4096
  authToken?: string;        // if set, sent as Bearer
  credentials?: RequestCredentials;
  signal?: AbortSignal;
};

export async function callFiFast(opts: FiFastOptions): Promise<string> {
  const res = await fetch(opts.endpoint ?? "/v4/fi-fast", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(opts.authToken ? { Authorization: `Bearer ${opts.authToken}` } : {}),
    },
    credentials: opts.credentials ?? (opts.authToken ? "same-origin" : "include"),
    signal: opts.signal,
    body: JSON.stringify({
      prompt: opts.prompt,
      system: opts.system,
      model: opts.model ?? "gemini-3-flash-preview",
      temperature: opts.temperature ?? 0.1,
      max_output_tokens: opts.maxOutputTokens ?? 4096,
    }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`fi-fast ${res.status}: ${body.slice(0, 200)}`);
  }
  const json = await res.json().catch(() => null) as {
    content?: { parts?: Array<{ text?: string }> };
  } | null;
  return json?.content?.parts?.[0]?.text ?? "";
}

// Build a prompt that grounds fi-fast in the drill entity's context. Apps
// can use this as-is, wrap it, or skip it entirely and build their own.
export function buildDrillPrompt(userMessage: string, entity: DrillEntity): string {
  const ctx = {
    entity: entity.title,
    kind: entity.kind,
    value: entity.value,
    subvalue: entity.subvalue,
    breadcrumb: entity.breadcrumb,
    stats: entity.stats,
    breakdown: entity.breakdown?.slice(0, 12),
  };
  return `You are a senior data analyst. The user is inspecting a dashboard widget and clicked to drill in.

Widget context:
${JSON.stringify(ctx, null, 2)}

User question: ${userMessage}

Answer in 2-4 sentences. Ground your answer in the numbers above. Be specific and direct. No preamble.`;
}

export function useDrill(): DrillContextValue {
  const v = React.useContext(DrillContext);
  if (!v) throw new Error("useDrill() requires a <DrillProvider> ancestor.");
  return v;
}

function DrillDrawer({ entity, onClose, aiChat }: {
  entity: DrillEntity | null;
  onClose: () => void;
  aiChat?: DrillAiChatConfig;
}) {
  const P = usePalette();
  useSaasKeyframes();
  type ChatMsg = { role: "user" | "agent" | "error"; text: string };
  const [messages, setMessages] = useState<ChatMsg[]>([]);
  const [draft, setDraft] = useState("");
  const [thinking, setThinking] = useState(false);

  // Reset chat whenever the drill target changes; a new entity → fresh thread.
  useEffect(() => { setMessages([]); setDraft(""); setThinking(false); }, [entity?.id]);

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

  const ask = async () => {
    if (!aiChat || !draft.trim() || thinking) return;
    const q = draft.trim();
    setDraft("");
    setMessages((m) => [...m, { role: "user", text: q }]);
    setThinking(true);
    try {
      const reply = await aiChat.onAsk(q, entity);
      setMessages((m) => [...m, { role: "agent", text: reply || "(no response)" }]);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setMessages((m) => [...m, { role: "error", text: msg }]);
    } finally {
      setThinking(false);
    }
  };
  return (
    <>
      <div onClick={onClose} style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)",
        zIndex: 1500, backdropFilter: "blur(4px)", animation: "saasFade 0.18s ease-out",
      }} />
      <div style={{
        position: "fixed", top: 0, right: 0, bottom: 0, width: 520, maxWidth: "92vw",
        background: P.sidebar, zIndex: 1501, color: P.text,
        boxShadow: "-24px 0 60px rgba(0,0,0,0.35)",
        display: "flex", flexDirection: "column", fontFamily: P.sans,
        borderLeft: `1px solid ${P.border}`, animation: "saasSlide 0.22s ease-out",
      }}>
        <div style={{ padding: "20px 24px 18px", borderBottom: `1px solid ${P.border}` }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, fontSize: 11, color: P.dim }}>
                <span style={{ fontFamily: P.mono, letterSpacing: "0.1em", textTransform: "uppercase", color: P.accent }}>
                  {entity.kind === "kpi" ? "KPI" : entity.kind === "chart" ? "Chart" : "Row"}
                </span>
                <span>·</span>
                <span>{entity.breadcrumb || "Portfolio"}</span>
              </div>
              <div style={{ fontSize: 18, fontWeight: 600, letterSpacing: "-0.01em", lineHeight: 1.3 }}>{entity.title}</div>
              {entity.value != null ? (
                <div style={{ fontSize: 32, fontWeight: 600, letterSpacing: "-0.02em", marginTop: 10, color: P.accent }}>
                  {entity.value}
                </div>
              ) : null}
              {entity.subvalue ? (
                <div style={{ fontSize: 12, color: P.sub, marginTop: 4, fontFamily: P.mono }}>{entity.subvalue}</div>
              ) : null}
            </div>
            <button onClick={onClose} style={{
              background: P.elev, border: `1px solid ${P.border}`, borderRadius: 6,
              padding: "4px 10px", color: P.sub, cursor: "pointer", fontSize: 12, fontFamily: P.sans,
            }}>ESC</button>
          </div>
        </div>
        <div style={{ flex: 1, overflowY: "auto", padding: "18px 24px" }}>
          {entity.narrative ? (
            <div style={{ fontSize: 13, color: P.sub, lineHeight: 1.55, marginBottom: 20 }}>{entity.narrative}</div>
          ) : null}
          {entity.stats && entity.stats.length > 0 ? (
            <div style={{ marginBottom: 22 }}>
              <DrillSectionLabel>Computed</DrillSectionLabel>
              {entity.stats.map(([k, v]) => (
                <div key={k} style={{
                  display: "flex", justifyContent: "space-between", alignItems: "baseline",
                  padding: "7px 0", borderBottom: `1px solid ${P.border}`, fontSize: 12,
                }}>
                  <span style={{ color: P.sub }}>{k}</span>
                  <span style={{ fontFamily: P.mono, color: P.text }}>{v}</span>
                </div>
              ))}
            </div>
          ) : null}
          {entity.breakdown && entity.breakdown.length > 0 ? (
            <div style={{ marginBottom: 22 }}>
              <DrillSectionLabel>Breakdown</DrillSectionLabel>
              {entity.breakdown.map((b) => (
                <div key={b.label} style={{ marginBottom: 8 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 3 }}>
                    <span style={{ color: P.sub }}>{b.label}</span>
                    <span style={{ fontFamily: P.mono, color: P.text }}>{typeof b.value === "number" ? b.value.toLocaleString() : b.value}</span>
                  </div>
                  <div style={{ height: 4, background: P.elev, borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ height: "100%", width: `${(Math.abs(b.value) / maxBar) * 100}%`, background: P.accent, borderRadius: 2 }} />
                  </div>
                </div>
              ))}
            </div>
          ) : null}
          {entity.sql ? (
            <div style={{ marginBottom: aiChat ? 22 : 0 }}>
              <DrillSectionLabel>SQL</DrillSectionLabel>
              <pre style={{
                margin: 0, padding: 12, background: P.elev, border: `1px solid ${P.border}`,
                borderRadius: 6, fontSize: 11, fontFamily: P.mono, color: P.sub,
                whiteSpace: "pre-wrap", lineHeight: 1.5,
              }}>{entity.sql}</pre>
            </div>
          ) : null}
          {entity.extra ?? null}
          {aiChat && messages.length > 0 ? (
            <div style={{ marginBottom: 12 }}>
              <DrillSectionLabel>Follow-up</DrillSectionLabel>
              {messages.map((m, i) => (
                <div key={i} style={{
                  fontSize: 12, lineHeight: 1.55, marginBottom: 10,
                  padding: "8px 10px", borderRadius: 6,
                  background: m.role === "user" ? P.elev : m.role === "error" ? P.badSoft : "transparent",
                  color: m.role === "error" ? P.bad : m.role === "user" ? P.text : P.sub,
                  border: m.role === "agent" ? `1px solid ${P.border}` : "none",
                  whiteSpace: "pre-wrap",
                }}>
                  <div style={{
                    fontSize: 10, fontFamily: P.mono, textTransform: "uppercase",
                    letterSpacing: "0.08em", color: P.faint, marginBottom: 4,
                  }}>
                    {m.role === "user" ? "You" : m.role === "error" ? "Error" : "Agent"}
                  </div>
                  {m.text}
                </div>
              ))}
              {thinking ? (
                <div style={{ fontSize: 11, color: P.dim, fontFamily: P.mono, padding: "4px 10px" }}>
                  <span style={{ animation: "saasPulse 0.9s ease-in-out infinite" }}>thinking…</span>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
        {aiChat ? (
          <div style={{ borderTop: `1px solid ${P.border}`, padding: "12px 18px", background: P.sidebar }}>
            <div style={{ display: "flex", gap: 8 }}>
              <input
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); ask(); } }}
                placeholder={aiChat.placeholder ?? "Ask a follow-up…"}
                disabled={thinking}
                style={{
                  flex: 1, padding: "8px 12px", fontSize: 13,
                  background: P.elev, border: `1px solid ${P.border}`,
                  color: P.text, borderRadius: 6, outline: "none", fontFamily: P.sans,
                }}
                onFocus={(e) => (e.currentTarget.style.borderColor = P.accent)}
                onBlur={(e) => (e.currentTarget.style.borderColor = P.border)}
              />
              <button
                onClick={ask}
                disabled={thinking || !draft.trim()}
                style={{
                  padding: "8px 14px", fontSize: 13, fontWeight: 500,
                  background: thinking || !draft.trim() ? P.elev : P.accent,
                  color: thinking || !draft.trim() ? P.dim : "#fff",
                  border: `1px solid ${thinking || !draft.trim() ? P.border : P.accent}`,
                  borderRadius: 6, cursor: thinking || !draft.trim() ? "not-allowed" : "pointer",
                  fontFamily: P.sans,
                }}
              >Ask</button>
            </div>
            {aiChat.disclaimer ? (
              <div style={{ fontSize: 10, color: P.faint, marginTop: 6, fontFamily: P.mono }}>{aiChat.disclaimer}</div>
            ) : null}
          </div>
        ) : null}
      </div>
    </>
  );
}

function DrillSectionLabel({ children }: { children: React.ReactNode }) {
  const P = usePalette();
  return (
    <div style={{
      fontSize: 10, color: P.faint, letterSpacing: "0.08em",
      textTransform: "uppercase", marginBottom: 8, fontWeight: 600,
    }}>{children}</div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
// SaasDataTable — virtualized, filterable, sortable table
// ═══════════════════════════════════════════════════════════════════════════
// Pure client-side table: feed it a rows array (from useSqlQuery or similar)
// and it handles virtualization, cycle sort, per-column filter popovers,
// exact-match search, column resize, and an aggregates footer. Built for
// up-to-~100K rows; for larger sets, pre-filter via useSqlQuery first.

export type SaasDataTableColumnKind =
  | "string"
  | "enum"
  | "number"
  | "money"
  | "rate"
  | "date"
  | "bool";

export type SaasDataTableColumn<T = Record<string, unknown>> = {
  key: string;
  label: string;
  width: number;
  align?: "left" | "center" | "right";
  mono?: boolean;
  kind: SaasDataTableColumnKind;
  // Enum → display label map (e.g. { current: "Current", late_30: "30 days late" }).
  // Used for filter checkbox labels and default cell text.
  enumLabels?: Record<string, string>;
  // Override the cell element entirely (wins over `format`).
  render?: (value: unknown, row: T) => React.ReactNode;
  // Override text formatting (loses to `render` but used elsewhere, e.g. search).
  format?: (value: unknown) => string;
};

export type SaasDataTableSort = { key: string; dir: "asc" | "desc" };

type ColFilter =
  | { kind: "enum"; values: string[] }
  | { kind: "range"; min?: number; max?: number };

// Default cell text by column kind. Callers override per-column with `format` or `render`.
function defaultFormat(col: SaasDataTableColumn, v: unknown): string {
  if (v == null || v === "") return "—";
  if (col.format) return col.format(v);
  const n = Number(v);
  if (col.kind === "money") {
    if (!Number.isFinite(n)) return String(v);
    if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
    if (Math.abs(n) >= 1e3) return `$${Math.round(n / 1e3)}K`;
    return `$${Math.round(n).toLocaleString()}`;
  }
  if (col.kind === "rate") {
    if (!Number.isFinite(n)) return String(v);
    // Heuristic: values ≤ 1 are decimal fractions (0.35 → 35%), otherwise already pct.
    return Math.abs(n) <= 1 ? `${Math.round(n * 100)}%` : `${n.toFixed(1)}%`;
  }
  if (col.kind === "number") {
    if (!Number.isFinite(n)) return String(v);
    return n.toLocaleString();
  }
  if (col.kind === "bool") return v ? "✓" : "—";
  if (col.kind === "enum") return col.enumLabels?.[String(v)] ?? String(v);
  return String(v);
}

// Gather unique non-null values for a column — powers the enum filter popover.
function uniqueValues<T>(rows: T[], key: string): string[] {
  const set = new Set<string>();
  for (const r of rows) {
    const v = (r as Record<string, unknown>)[key];
    if (v == null || v === "") continue;
    set.add(String(v));
  }
  return [...set].sort();
}

export function SaasDataTable<T extends Record<string, unknown>>(props: {
  columns: SaasDataTableColumn<T>[];
  rows: T[];
  rowKey?: (row: T, index: number) => string | number;
  defaultSort?: SaasDataTableSort;
  onRowClick?: (row: T) => void;
  searchPlaceholder?: string;
  // Aggregates shown in the sticky footer. Receives the *filtered+sorted*
  // rows, returns a map of label → display value. Keep it small (3-6 keys).
  aggregates?: (rows: T[]) => Record<string, React.ReactNode>;
  rowHeight?: number;
  // Table minimum height in pixels. Defaults to a flex fill; set this when
  // the table is inside a container without explicit height.
  height?: number | string;
  // Optional stable column-width persistence key (localStorage).
  widthStorageKey?: string;
  // Rows per page. Pagination auto-kicks in when the filtered+sorted result
  // set exceeds this. Default: 2000. Pass Infinity to disable paging and
  // virtualize the whole set (not recommended above ~50K rows).
  pageSize?: number;
}) {
  const P = usePalette();
  const ROW_H = props.rowHeight ?? 34;

  // Column widths — resizable via right-edge drag handle on headers.
  const [widths, setWidths] = useState<Record<string, number>>(() => {
    if (props.widthStorageKey && typeof window !== "undefined") {
      try {
        const saved = window.localStorage.getItem(props.widthStorageKey);
        if (saved) return JSON.parse(saved);
      } catch { /* ignore */ }
    }
    return Object.fromEntries(props.columns.map((c) => [c.key, c.width]));
  });
  useEffect(() => {
    if (!props.widthStorageKey) return;
    try { window.localStorage.setItem(props.widthStorageKey, JSON.stringify(widths)); } catch { /* ignore */ }
  }, [widths, props.widthStorageKey]);
  const getW = (k: string) => widths[k] ?? props.columns.find((c) => c.key === k)?.width ?? 100;
  const totalW = props.columns.reduce((s, c) => s + getW(c.key), 0);

  // Sort — cycles asc → desc → reset
  const [sort, setSort] = useState<SaasDataTableSort>(
    props.defaultSort ?? { key: props.columns[0]?.key ?? "", dir: "desc" },
  );
  const cycleSort = (key: string) => setSort((s) => {
    if (s.key !== key) return { key, dir: "asc" };
    if (s.dir === "asc") return { key, dir: "desc" };
    return props.defaultSort ?? { key: props.columns[0]?.key ?? "", dir: "desc" };
  });

  // Per-column filters
  const [colFilters, setColFilters] = useState<Record<string, ColFilter>>({});
  const setColFilter = (key: string, val: ColFilter | null) =>
    setColFilters((f) => {
      const n = { ...f };
      if (val == null) delete n[key]; else n[key] = val;
      return n;
    });

  // Search (exact-match across all columns)
  const [search, setSearch] = useState("");
  const q = search.trim().toLowerCase();

  // Apply column filters
  const filtered = useMemo(() => {
    const keys = Object.keys(colFilters);
    if (keys.length === 0) return props.rows;
    return props.rows.filter((r) => {
      for (const k of keys) {
        const f = colFilters[k];
        const v = (r as Record<string, unknown>)[k];
        if (f.kind === "range") {
          const n = Number(v);
          if (f.min != null && (!Number.isFinite(n) || n < f.min)) return false;
          if (f.max != null && (!Number.isFinite(n) || n > f.max)) return false;
        } else {
          if (v == null) return false;
          if (!f.values.includes(String(v))) return false;
        }
      }
      return true;
    });
  }, [props.rows, colFilters]);

  // Apply search
  const searched = useMemo(() => {
    if (!q) return filtered;
    return filtered.filter((r) => {
      for (const c of props.columns) {
        const v = (r as Record<string, unknown>)[c.key];
        if (v == null) continue;
        const display = defaultFormat(c, v).toLowerCase();
        if (display.includes(q)) return true;
        if (String(v).toLowerCase().includes(q)) return true;
      }
      return false;
    });
  }, [filtered, q, props.columns]);

  // Apply sort
  const sorted = useMemo(() => {
    const arr = searched.slice();
    const { key, dir } = sort;
    const m = dir === "asc" ? 1 : -1;
    arr.sort((a, b) => {
      const av = (a as Record<string, unknown>)[key];
      const bv = (b as Record<string, unknown>)[key];
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * m;
      const na = Number(av), nb = Number(bv);
      if (Number.isFinite(na) && Number.isFinite(nb)) return (na - nb) * m;
      return String(av).localeCompare(String(bv)) * m;
    });
    return arr;
  }, [searched, sort]);

  // Paging — auto-kicks in once the filtered+sorted result set exceeds pageSize
  // so we don't end up scrolling through tens of thousands of virtualized rows.
  // Aggregates are computed over the full filtered set (`sorted`), not the page.
  const pageSize = props.pageSize ?? 2000;
  const pagingActive = sorted.length > pageSize && Number.isFinite(pageSize);
  const pageCount = pagingActive ? Math.max(1, Math.ceil(sorted.length / pageSize)) : 1;
  const [page, setPage] = useState(0);
  // Reset to first page whenever the underlying result set changes.
  useEffect(() => { setPage(0); }, [colFilters, search, sort, pageSize]);
  // Clamp if the result set shrinks below the current page.
  useEffect(() => { if (page >= pageCount) setPage(0); }, [pageCount, page]);
  const pageStart = pagingActive ? page * pageSize : 0;
  const pageEnd = pagingActive ? Math.min(sorted.length, pageStart + pageSize) : sorted.length;
  const paged = useMemo(
    () => (pagingActive ? sorted.slice(pageStart, pageEnd) : sorted),
    [sorted, pageStart, pageEnd, pagingActive],
  );

  // Virtualization — operates over the current page.
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportH, setViewportH] = useState(600);
  useEffect(() => {
    const el = scrollerRef.current;
    if (!el) return;
    const onScroll = () => setScrollTop(el.scrollTop);
    const ro = new ResizeObserver(() => setViewportH(el.clientHeight));
    el.addEventListener("scroll", onScroll, { passive: true });
    ro.observe(el);
    setViewportH(el.clientHeight);
    return () => { el.removeEventListener("scroll", onScroll); ro.disconnect(); };
  }, []);

  // Reset scroll on filter/search/sort/page change so users see the top of the current page.
  useEffect(() => {
    if (scrollerRef.current) scrollerRef.current.scrollTop = 0;
    setScrollTop(0);
  }, [colFilters, search, sort, page]);

  const overscan = 10;
  const totalH = paged.length * ROW_H;
  const startIdx = Math.max(0, Math.floor(scrollTop / ROW_H) - overscan);
  const endIdx = Math.min(paged.length, Math.ceil((scrollTop + viewportH) / ROW_H) + overscan);
  const visible = paged.slice(startIdx, endIdx);

  const aggregates = props.aggregates?.(sorted);

  const containerStyle: React.CSSProperties = {
    background: P.card,
    border: `1px solid ${P.border}`,
    borderRadius: 10,
    overflow: "hidden",
    display: "flex",
    flexDirection: "column",
    height: props.height ?? "100%",
    minHeight: 300,
  };

  return (
    <div style={containerStyle}>
      {/* Toolbar: search + active-filter summary */}
      <div style={{
        display: "flex", alignItems: "center", gap: 12,
        padding: "10px 14px", borderBottom: `1px solid ${P.border}`,
      }}>
        <div style={{ position: "relative", flex: 1, maxWidth: 360 }}>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder={props.searchPlaceholder ?? "Search…"}
            style={{
              width: "100%", padding: "6px 10px 6px 26px", fontSize: 12,
              background: P.elev, border: `1px solid ${P.border}`, color: P.text,
              borderRadius: 5, outline: "none", fontFamily: P.sans,
            }}
          />
          <span style={{ position: "absolute", left: 9, top: 6, fontSize: 11, color: P.faint, pointerEvents: "none" }}>⌕</span>
          {search ? (
            <button
              onClick={() => setSearch("")}
              style={{
                position: "absolute", right: 4, top: 3, border: "none", background: "transparent",
                color: P.faint, cursor: "pointer", fontSize: 13, padding: "2px 6px", lineHeight: 1,
              }}
            >×</button>
          ) : null}
        </div>
        <div style={{ fontSize: 11, color: P.dim, fontFamily: P.mono }}>
          {sorted.length.toLocaleString()} of {props.rows.length.toLocaleString()} rows
          {Object.keys(colFilters).length > 0 ? (
            <>
              {" · "}
              <button
                onClick={() => setColFilters({})}
                style={{
                  fontSize: 11, color: P.accent, background: "none", border: "none",
                  cursor: "pointer", padding: 0, fontFamily: P.mono,
                }}
              >clear column filters</button>
            </>
          ) : null}
        </div>
      </div>

      {/* Header row (sticky) */}
      <div style={{
        display: "flex", background: P.elev, borderBottom: `1px solid ${P.border}`,
        position: "sticky", top: 0, zIndex: 2, flexShrink: 0,
      }}>
        {props.columns.map((c) => {
          const w = getW(c.key);
          const sorting = sort.key === c.key ? sort.dir : null;
          const filterActive = colFilters[c.key] != null;
          return (
            <div key={c.key} style={{
              width: w, minWidth: w, maxWidth: w,
              padding: "8px 10px",
              borderRight: `1px solid ${P.border}`,
              position: "relative",
              display: "flex", alignItems: "center",
              justifyContent: c.align === "right" ? "flex-end" : c.align === "center" ? "center" : "flex-start",
              gap: 4,
              fontSize: 11, fontWeight: 500, color: P.dim,
              userSelect: "none",
            }}>
              <span
                onClick={() => cycleSort(c.key)}
                style={{
                  cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 4,
                  color: sorting ? P.accent : P.dim,
                }}
                title="Click to sort"
              >
                {c.label}
                <SaasSortIcon dir={sorting} P={P} />
              </span>
              <SaasColFilterButton
                column={c}
                rows={props.rows}
                value={colFilters[c.key] ?? null}
                onChange={(next) => setColFilter(c.key, next)}
                P={P}
              />
              {/* Resize handle — drag to resize, double-click to reset to default */}
              <div
                onMouseDown={(e) => {
                  const startX = e.clientX;
                  const startW = getW(c.key);
                  const onMove = (ev: MouseEvent) => {
                    const next = Math.max(40, startW + (ev.clientX - startX));
                    setWidths((prev) => ({ ...prev, [c.key]: next }));
                  };
                  const onUp = () => {
                    window.removeEventListener("mousemove", onMove);
                    window.removeEventListener("mouseup", onUp);
                    document.body.style.cursor = "";
                  };
                  window.addEventListener("mousemove", onMove);
                  window.addEventListener("mouseup", onUp);
                  document.body.style.cursor = "col-resize";
                  e.preventDefault();
                }}
                onDoubleClick={() => setWidths((prev) => ({ ...prev, [c.key]: c.width }))}
                title="Drag to resize · double-click to reset"
                style={{
                  position: "absolute", right: -3, top: 0, bottom: 0, width: 7,
                  cursor: "col-resize", zIndex: 3,
                }}
              />
              {filterActive ? (
                <span
                  style={{
                    position: "absolute", top: 2, right: 2,
                    width: 5, height: 5, borderRadius: "50%", background: P.accent,
                  }}
                />
              ) : null}
            </div>
          );
        })}
      </div>

      {/* Virtualized body */}
      <div
        ref={scrollerRef}
        style={{ flex: 1, overflow: "auto", position: "relative", minHeight: 200 }}
      >
        {sorted.length === 0 ? (
          <div style={{
            padding: "40px 14px", textAlign: "center", color: P.dim,
            fontSize: 12, fontFamily: P.sans,
          }}>
            No rows match the current filters.
          </div>
        ) : (
          <div style={{ height: totalH, position: "relative", minWidth: totalW }}>
            {visible.map((row, i) => {
              const idx = startIdx + i;
              const key = props.rowKey ? props.rowKey(row, idx) : idx;
              return (
                <div
                  key={key}
                  onClick={() => props.onRowClick?.(row)}
                  style={{
                    position: "absolute", top: idx * ROW_H, left: 0,
                    display: "flex", alignItems: "center",
                    height: ROW_H, width: "100%", minWidth: totalW,
                    borderBottom: `1px solid ${P.border}`,
                    cursor: props.onRowClick ? "pointer" : "default",
                    background: idx % 2 === 0 ? "transparent" : P.bg,
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = P.elev)}
                  onMouseLeave={(e) => (e.currentTarget.style.background = idx % 2 === 0 ? "transparent" : P.bg)}
                >
                  {props.columns.map((c) => {
                    const w = getW(c.key);
                    const v = (row as Record<string, unknown>)[c.key];
                    const content = c.render ? c.render(v, row) : defaultFormat(c, v);
                    return (
                      <div key={c.key} style={{
                        width: w, minWidth: w, maxWidth: w,
                        padding: "0 10px",
                        textAlign: c.align ?? "left",
                        fontFamily: c.mono ? P.mono : P.sans,
                        fontSize: 12,
                        color: P.text,
                        overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                      }}>
                        {content}
                      </div>
                    );
                  })}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Pagination bar — only when the filtered set exceeds pageSize */}
      {pagingActive ? (
        <div style={{
          display: "flex", alignItems: "center", justifyContent: "space-between",
          padding: "6px 14px", borderTop: `1px solid ${P.border}`,
          background: P.elev, fontSize: 11, color: P.sub,
          flexShrink: 0,
        }}>
          <span style={{ fontFamily: P.mono, color: P.dim }}>
            Rows {(pageStart + 1).toLocaleString()}–{pageEnd.toLocaleString()} of {sorted.length.toLocaleString()}
          </span>
          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <SaasPageBtn onClick={() => setPage(0)}                                 disabled={page === 0}              P={P}>« First</SaasPageBtn>
            <SaasPageBtn onClick={() => setPage((p) => Math.max(0, p - 1))}         disabled={page === 0}              P={P}>‹ Prev</SaasPageBtn>
            <span style={{ fontFamily: P.mono, fontSize: 11, color: P.text, padding: "0 8px" }}>
              Page {page + 1} of {pageCount}
            </span>
            <SaasPageBtn onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))} disabled={page >= pageCount - 1} P={P}>Next ›</SaasPageBtn>
            <SaasPageBtn onClick={() => setPage(pageCount - 1)}                      disabled={page >= pageCount - 1} P={P}>Last »</SaasPageBtn>
          </div>
        </div>
      ) : null}

      {/* Aggregates footer (sticky at bottom) */}
      {aggregates && Object.keys(aggregates).length > 0 ? (
        <div style={{
          display: "flex", alignItems: "center", gap: 24,
          padding: "8px 14px", borderTop: `1px solid ${P.border}`,
          background: P.elev, fontSize: 11, color: P.sub,
          flexShrink: 0, overflowX: "auto",
        }}>
          {Object.entries(aggregates).map(([k, v]) => (
            <div key={k} style={{ display: "flex", alignItems: "baseline", gap: 6, whiteSpace: "nowrap" }}>
              <span style={{ color: P.dim, fontFamily: P.mono, fontSize: 10, textTransform: "uppercase", letterSpacing: "0.06em" }}>{k}</span>
              <span style={{ color: P.text, fontFamily: P.mono, fontSize: 12, fontWeight: 500 }}>{v}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function SaasPageBtn(props: { children: React.ReactNode; onClick: () => void; disabled: boolean; P: SaasPalette }) {
  const { P } = props;
  return (
    <button
      onClick={props.onClick}
      disabled={props.disabled}
      style={{
        padding: "3px 8px", fontSize: 11, fontFamily: P.mono,
        background: "transparent", border: `1px solid ${P.border}`,
        color: props.disabled ? P.faint : P.sub,
        borderRadius: 4,
        cursor: props.disabled ? "not-allowed" : "pointer",
      }}
      onMouseEnter={(e) => { if (!props.disabled) { e.currentTarget.style.color = P.text; e.currentTarget.style.borderColor = P.rule; } }}
      onMouseLeave={(e) => { e.currentTarget.style.color = props.disabled ? P.faint : P.sub; e.currentTarget.style.borderColor = P.border; }}
    >
      {props.children}
    </button>
  );
}

function SaasSortIcon({ dir, P }: { dir: "asc" | "desc" | null; P: SaasPalette }) {
  return (
    <svg width={9} height={11} viewBox="0 0 10 14" style={{ flexShrink: 0 }}>
      <polygon points="5,1 9,5 1,5" fill={dir === "asc" ? P.accent : P.faint} opacity={dir === "asc" ? 1 : 0.5} />
      <polygon points="5,13 1,9 9,9" fill={dir === "desc" ? P.accent : P.faint} opacity={dir === "desc" ? 1 : 0.5} />
    </svg>
  );
}

function SaasColFilterButton<T extends Record<string, unknown>>({
  column, rows, value, onChange, P,
}: {
  column: SaasDataTableColumn<T>;
  rows: T[];
  value: ColFilter | null;
  onChange: (next: ColFilter | null) => void;
  P: SaasPalette;
}) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const popRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (popRef.current && !popRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  const isNumeric = column.kind === "money" || column.kind === "number" || column.kind === "rate";
  const active = value != null;

  const triggerStyle: React.CSSProperties = {
    padding: "2px 4px", border: "none",
    background: active ? P.accentSoft : "transparent",
    borderRadius: 3, cursor: "pointer",
    display: "inline-flex", alignItems: "center", justifyContent: "center",
    marginLeft: 4,
  };
  const popStyle: React.CSSProperties = {
    position: "absolute", top: "calc(100% + 4px)", left: 0, zIndex: 100,
    background: P.card, border: `1px solid ${P.border}`, borderRadius: 6,
    padding: 8, minWidth: 220, boxShadow: "0 12px 28px rgba(0,0,0,0.45)",
  };
  const linkStyle: React.CSSProperties = {
    fontSize: 11, color: P.dim, background: "none", border: "none",
    cursor: "pointer", fontFamily: P.sans, padding: 0,
  };

  if (isNumeric) {
    const v = value?.kind === "range" ? value : { kind: "range" as const };
    return (
      <div ref={popRef} style={{ position: "relative" }}>
        <button onClick={() => setOpen((o) => !o)} style={triggerStyle} title="Filter">
          <SaasFilterIcon active={active} P={P} />
        </button>
        {open ? (
          <div style={popStyle}>
            <div style={{ fontSize: 11, color: P.dim, marginBottom: 6, fontFamily: P.sans }}>{column.label} range</div>
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <input
                type="number"
                placeholder="min"
                value={v.min ?? ""}
                onChange={(e) => onChange({ kind: "range", min: e.target.value === "" ? undefined : Number(e.target.value), max: v.max })}
                style={numInp(P)}
              />
              <span style={{ color: P.dim, fontSize: 11 }}>—</span>
              <input
                type="number"
                placeholder="max"
                value={v.max ?? ""}
                onChange={(e) => onChange({ kind: "range", min: v.min, max: e.target.value === "" ? undefined : Number(e.target.value) })}
                style={numInp(P)}
              />
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8 }}>
              <button onClick={() => { onChange(null); setOpen(false); }} style={linkStyle}>Clear</button>
              <button
                onClick={() => setOpen(false)}
                style={{
                  fontSize: 11, padding: "4px 10px", background: P.accent, color: "#fff",
                  border: "none", borderRadius: 4, cursor: "pointer", fontFamily: P.sans, fontWeight: 500,
                }}
              >Done</button>
            </div>
          </div>
        ) : null}
      </div>
    );
  }

  // Enum / string multi-select
  const opts = useMemo(() => uniqueValues(rows, column.key), [rows, column.key]);
  const qLower = q.toLowerCase();
  const filteredOpts = qLower ? opts.filter((o) => o.toLowerCase().includes(qLower)) : opts;
  const selected = new Set(value?.kind === "enum" ? value.values : []);
  const toggle = (v: string) => {
    const next = new Set(selected);
    if (next.has(v)) next.delete(v); else next.add(v);
    onChange(next.size === 0 ? null : { kind: "enum", values: [...next] });
  };

  return (
    <div ref={popRef} style={{ position: "relative" }}>
      <button onClick={() => setOpen((o) => !o)} style={triggerStyle} title="Filter">
        <SaasFilterIcon active={active} P={P} />
      </button>
      {open ? (
        <div style={popStyle}>
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder={`Filter ${column.label.toLowerCase()}…`}
            style={{
              width: "100%", padding: "4px 8px", marginBottom: 6,
              background: P.elev, border: `1px solid ${P.border}`, borderRadius: 4,
              color: P.text, fontSize: 12, outline: "none", fontFamily: P.sans,
            }}
          />
          <div style={{ maxHeight: 220, overflowY: "auto", margin: "0 -2px" }}>
            {filteredOpts.map((opt) => {
              const checked = selected.has(opt);
              const display = column.enumLabels?.[opt] ?? opt;
              return (
                <label key={opt} style={{
                  display: "flex", alignItems: "center", gap: 7,
                  padding: "4px 6px", borderRadius: 4, cursor: "pointer", fontSize: 12,
                  color: checked ? P.text : P.sub,
                  background: checked ? P.accentSoft : "transparent",
                }}>
                  <span style={{
                    width: 12, height: 12, borderRadius: 3,
                    border: `1px solid ${checked ? P.accent : P.border}`,
                    background: checked ? P.accent : "transparent",
                    display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
                  }}>
                    {checked ? <span style={{ color: "#fff", fontSize: 9, fontWeight: 700, lineHeight: 1 }}>✓</span> : null}
                  </span>
                  <input type="checkbox" checked={checked} onChange={() => toggle(opt)} style={{ display: "none" }} />
                  <span style={{ flex: 1 }}>{display}</span>
                </label>
              );
            })}
            {filteredOpts.length === 0 ? (
              <div style={{ fontSize: 11, color: P.faint, padding: "6px 4px", fontStyle: "italic" }}>No matches</div>
            ) : null}
          </div>
          <div style={{
            display: "flex", justifyContent: "space-between",
            marginTop: 6, paddingTop: 6, borderTop: `1px solid ${P.border}`,
          }}>
            <button onClick={() => { onChange(null); setOpen(false); }} style={linkStyle}>Clear</button>
            <span style={{ fontSize: 10, color: P.dim, fontFamily: P.mono }}>{selected.size} selected</span>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SaasFilterIcon({ active, P }: { active: boolean; P: SaasPalette }) {
  return (
    <svg width={11} height={11} viewBox="0 0 24 24" fill="none" stroke={active ? P.accent : P.faint} strokeWidth={2.4} strokeLinecap="round" strokeLinejoin="round">
      <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />
    </svg>
  );
}

function numInp(P: SaasPalette): React.CSSProperties {
  return {
    padding: "4px 8px", background: P.elev, border: `1px solid ${P.border}`,
    borderRadius: 4, color: P.text, fontSize: 12, outline: "none",
    fontFamily: P.mono, width: 80,
  };
}

declare global {
  interface Window {
    Definite?: DefiniteBridge;
  }
}
