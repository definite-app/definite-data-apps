import React, { useEffect, useMemo, useRef, useState } from "react";

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

function getBridge(): DefiniteBridge {
  if (window.Definite) {
    return window.Definite;
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
  emitRuntimeError({
    ...context,
    message: context.message ?? (error instanceof Error ? error.message : String(error)),
    details: context.details ?? null,
  });
}

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

async function queryRows(conn: any, sql: string): Promise<Array<Record<string, unknown>>> {
  const result = await conn.query(sql);
  return normalizeRows(
    result.toArray().map((row: { toJSON?: () => Record<string, unknown> }) => row.toJSON?.() ?? row),
  );
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
    return () => {
      cancelled = true;
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
    return () => {
      cancelled = true;
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

export function LoadingState(props: { message?: string }) {
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
}) {
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
          {props.rows.map((row, index) => (
            <tr
              key={index}
              style={{
                background: "var(--bg-card)",
                borderBottom: index < props.rows.length - 1 ? "1px solid var(--border)" : "none",
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
      instance.setOption(JSON.parse(serializedOption), true);
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
      instanceRef.current.setOption(JSON.parse(serializedOption), true);
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
  if (unit === "days") x.setDate(x.getDate() + count);
  else if (unit === "weeks") x.setDate(x.getDate() + count * 7);
  else if (unit === "months") x.setMonth(x.getMonth() + count);
  else if (unit === "quarters") x.setMonth(x.getMonth() + count * 3);
  else if (unit === "years") x.setFullYear(x.getFullYear() + count);
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
      return { from: r.from, to: r.to, label };
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
  { key: "all", label: "All time", compute: () => ({ from: "", to: "", label: "All time" }) },
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
}) {
  const presets = props.presets ?? DEFAULT_DATE_RANGE_PRESETS;
  const [open, setOpen] = useState(false);
  const [mainTab, setMainTab] = useState<"relative" | "custom">("relative");
  const [draftMode, setDraftMode] = useState<"previous" | "next">("previous");
  const [draftN, setDraftN] = useState(12);
  const [draftUnit, setDraftUnit] = useState<DateUnit>("months");
  const [draftIncludeCurrent, setDraftIncludeCurrent] = useState(true);
  const [draftCustomFrom, setDraftCustomFrom] = useState(props.value.from || "");
  const [draftCustomTo, setDraftCustomTo] = useState(props.value.to || "");

  useEffect(() => {
    if (open) {
      setDraftCustomFrom(props.value.from || "");
      setDraftCustomTo(props.value.to || "");
    }
  }, [open, props.value.from, props.value.to]);

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
      });
    } else {
      if (!draftCustomFrom && !draftCustomTo) return;
      const label = draftCustomFrom && draftCustomTo
        ? `${formatHumanYmd(draftCustomFrom)} → ${formatHumanYmd(draftCustomTo)}`
        : draftCustomFrom
          ? `After ${formatHumanYmd(draftCustomFrom)}`
          : `Before ${formatHumanYmd(draftCustomTo)}`;
      props.onChange({ from: draftCustomFrom, to: draftCustomTo, label });
    }
    setOpen(false);
  };

  const applyPreset = (p: DateRangePreset) => {
    props.onChange(p.compute());
    if (p.mode && p.mode !== "current") setDraftMode(p.mode);
    if (typeof p.n === "number") setDraftN(p.n);
    if (p.unit) setDraftUnit(p.unit);
    if (typeof p.includeCurrent === "boolean") setDraftIncludeCurrent(p.includeCurrent);
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
            style={{
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
            }}
          >
            <div style={{ background: "var(--bg-elevated)", padding: 8, borderRight: "1px solid var(--border)" }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", padding: "6px 10px 10px", letterSpacing: 0.5 }}>
                Presets
              </div>
              {presets.map((p) => {
                const active = props.value.label === p.label;
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
                        onChange={(e) => setDraftN(Math.max(1, parseInt(e.target.value || "1", 10)))}
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
                          colorScheme: "dark",
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
                          colorScheme: "dark",
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

declare global {
  interface Window {
    Definite?: DefiniteBridge;
  }
}
