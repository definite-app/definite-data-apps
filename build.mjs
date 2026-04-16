import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";

const buildMjsDir = path.dirname(fileURLToPath(import.meta.url));
const toolkitNodeModules = path.join(buildMjsDir, "node_modules");
const canonicalRuntimePath = path.join(buildMjsDir, "runtime", "definite-runtime.tsx");

const definiteRuntimeAliasPlugin = {
  name: "definite-runtime-alias",
  setup(build) {
    build.onResolve({ filter: /^@definite\/runtime$/ }, () => ({
      path: canonicalRuntimePath,
    }));
  },
};

function usage() {
  console.error("Usage: node build.mjs <app-dir> [--preview-data /path/to/preview-data.json]");
  process.exit(1);
}

function escapeInlineScript(value) {
  return value.replace(/<\/script/gi, "<\\/script");
}

async function collectSourceFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "dist" || entry.name === "node_modules" || entry.name.startsWith(".")) {
        continue;
      }
      files.push(...await collectSourceFiles(fullPath));
      continue;
    }

    if (!/\.(jsx?|tsx?)$/.test(entry.name)) {
      continue;
    }

    files.push(fullPath);
  }

  return files;
}

function getLineNumber(source, index) {
  return source.slice(0, index).split("\n").length;
}

function parseLiteralString(argumentSource) {
  const trimmed = argumentSource.trim();
  const match = /^(["'`])(?<value>[\s\S]*?)\1$/u.exec(trimmed);
  if (!match?.groups) {
    return null;
  }
  return match.groups.value;
}

function findHookCalls(source) {
  const calls = [];
  const pattern = /\b(useDataset|useJsonResource)\s*\(([\s\S]*?)\)/g;

  for (const match of source.matchAll(pattern)) {
    const fullMatch = match[0];
    const hookName = match[1];
    const args = match[2] ?? "";

    // Skip the hook function definitions in the runtime file.
    const definitionPrefix = source.slice(Math.max(0, match.index - 24), match.index);
    if (/\b(?:export\s+)?function\s*$/.test(definitionPrefix)) {
      continue;
    }

    const firstArg = args.split(",")[0] ?? "";
    calls.push({
      hookName,
      raw: fullMatch,
      firstArg,
      index: match.index ?? 0,
    });
  }

  return calls;
}

const REQUIRED_FILTERS_MARKER_RE = /\{\{\s*required_filters\s*\}\}/;

function validateManifest(manifest, manifestPath) {
  if (manifest.version !== 2) {
    throw new Error(`Expected ${manifestPath} to contain {"version": 2}`);
  }

  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
    throw new Error(`Expected ${manifestPath} to contain a JSON object`);
  }

  if (typeof manifest.entry !== "string" || manifest.entry.length === 0) {
    throw new Error(`Expected ${manifestPath} to define a non-empty "entry"`);
  }

  if (!manifest.resources || typeof manifest.resources !== "object" || Array.isArray(manifest.resources)) {
    throw new Error(`Expected ${manifestPath} to define a "resources" object`);
  }

  const validKinds = new Set(["dataset", "json"]);
  for (const [key, resource] of Object.entries(manifest.resources)) {
    if (!key || typeof key !== "string") {
      throw new Error(`Expected all resource keys in ${manifestPath} to be non-empty strings`);
    }
    if (!resource || typeof resource !== "object" || Array.isArray(resource)) {
      throw new Error(`Resource ${key} in ${manifestPath} must be an object`);
    }
    if (!validKinds.has(resource.kind)) {
      throw new Error(`Resource ${key} in ${manifestPath} has unsupported kind "${resource.kind}"`);
    }

    const bindings = resource.requiredFilters;
    if (bindings === undefined || bindings === null) {
      continue;
    }
    if (typeof bindings !== "object" || Array.isArray(bindings)) {
      throw new Error(`Resource ${key} in ${manifestPath}: requiredFilters must be an object`);
    }
    for (const [member, binding] of Object.entries(bindings)) {
      if (!binding || typeof binding !== "object" || Array.isArray(binding)) {
        throw new Error(
          `Resource ${key} in ${manifestPath}: requiredFilters[${member}] must be an object with a "column" field`,
        );
      }
      if (typeof binding.column !== "string" || binding.column.length === 0) {
        throw new Error(
          `Resource ${key} in ${manifestPath}: requiredFilters[${member}].column must be a non-empty string`,
        );
      }
    }

    // SQL resources with requiredFilters MUST contain the {{ required_filters }} marker.
    // Cube resources don't need the marker — Cube filter members are dimension names and
    // get merged server-side by the query handler.
    const source = resource.source;
    if (source && typeof source === "object" && source.type === "sql") {
      if (typeof source.sql === "string" && !REQUIRED_FILTERS_MARKER_RE.test(source.sql)) {
        throw new Error(
          `Resource ${key} in ${manifestPath} declares requiredFilters but its SQL is missing the ` +
            `{{ required_filters }} marker. Add it to a WHERE/AND clause where the tenant filter should land.`,
        );
      }
    }
  }
}

async function validateResourceHookUsage(appDir, manifest) {
  const srcDir = path.join(appDir, "src");
  const sourceFiles = await collectSourceFiles(srcDir);
  const errors = [];
  const resources = manifest.resources ?? {};

  for (const filePath of sourceFiles) {
    const source = await readFile(filePath, "utf8");
    const calls = findHookCalls(source);

    for (const call of calls) {
      const line = getLineNumber(source, call.index);
      const literalKey = parseLiteralString(call.firstArg);
      const relativePath = path.relative(appDir, filePath);
      const expectedKind = call.hookName === "useDataset" ? "dataset" : "json";

      if (!literalKey) {
        errors.push(
          `${relativePath}:${line} ${call.hookName}() must use a literal manifest key string as its first argument.`,
        );
        continue;
      }

      const resource = resources[literalKey];
      if (!resource) {
        errors.push(
          `${relativePath}:${line} ${call.hookName}("${literalKey}") does not match any key in app.json resources.`,
        );
        continue;
      }

      if (resource.kind !== expectedKind) {
        errors.push(
          `${relativePath}:${line} ${call.hookName}("${literalKey}") expects a ${expectedKind} resource, but app.json declares "${literalKey}" as ${resource.kind}.`,
        );
      }
    }
  }

  if (errors.length > 0) {
    throw new Error(
      [
        "Invalid data-apps-v2 app. Resource hooks must use literal keys that exist in app.json.",
        ...errors,
      ].join("\n"),
    );
  }
}

const args = process.argv.slice(2);
const appDir = args[0] ? path.resolve(args[0]) : "";
if (!appDir) {
  usage();
}

let previewDataPath = null;
for (let index = 1; index < args.length; index += 1) {
  if (args[index] === "--preview-data") {
    previewDataPath = args[index + 1] ? path.resolve(args[index + 1]) : null;
    index += 1;
  }
}

const manifestPath = path.join(appDir, "app.json");
const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
validateManifest(manifest, manifestPath);
await validateResourceHookUsage(appDir, manifest);
const previewData = previewDataPath ? JSON.parse(await readFile(previewDataPath, "utf8")) : null;
if (previewData?.datasets) {
  const previewBaseDir = path.dirname(previewDataPath);
  for (const [key, value] of Object.entries(previewData.datasets)) {
    if (value && typeof value === "object" && !Array.isArray(value) && typeof value.file === "string") {
      const filePath = path.resolve(previewBaseDir, value.file);
      let bytes;
      try {
        bytes = await readFile(filePath);
      }
      catch (error) {
        throw new Error(
          `Preview dataset "${key}" references file "${value.file}" (resolved: ${filePath}) which could not be read: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
      const format = value.format ?? (filePath.endsWith(".duckdb") ? "duckdb" : "parquet");
      previewData.datasets[key] = {
        format,
        base64: bytes.toString("base64"),
      };
    }
  }
}

const entry = path.join(appDir, manifest.entry ?? "src/main.tsx");
const outDir = path.join(appDir, "dist");
await mkdir(outDir, { recursive: true });

const result = await build({
  absWorkingDir: appDir,
  entryPoints: [entry],
  bundle: true,
  write: false,
  format: "esm",
  platform: "browser",
  target: ["chrome123", "safari17", "firefox124"],
  jsx: "automatic",
  legalComments: "none",
  sourcemap: "inline",
  // nodePaths lets esbuild resolve react/react-dom from the toolkit's own
  // node_modules even when appDir lives outside this repo (e.g., a user
  // scaffolds an app anywhere on disk and runs `node <toolkit>/build.mjs <app>`).
  nodePaths: [toolkitNodeModules],
  plugins: [definiteRuntimeAliasPlugin],
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
});

const jsFile = result.outputFiles.find((file) => file.path.endsWith(".js")) ?? result.outputFiles[0];
if (!jsFile) {
  throw new Error("esbuild did not emit a JavaScript bundle");
}

const title = typeof manifest.name === "string" && manifest.name.length > 0
  ? manifest.name
  : "Definite Data App";
const importMap = {
  imports: {
    "apache-arrow": "https://storage.googleapis.com/definite-public/libs/apache-arrow@17.0.0/apache-arrow.esm.js",
  },
};

const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="definite-app-version" content="2" />
  <link rel="icon" href="data:," />
  <title>${title}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.6.0/dist/echarts.min.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
  <link rel="preload" href="https://cdn.jsdelivr.net/npm/@perspective-dev/server@4.3.0/dist/wasm/perspective-server.wasm" as="fetch" type="application/wasm" crossorigin="anonymous" />
  <link rel="preload" href="https://cdn.jsdelivr.net/npm/@perspective-dev/viewer@4.3.0/dist/wasm/perspective-viewer.wasm" as="fetch" type="application/wasm" crossorigin="anonymous" />
  <link rel="stylesheet" crossorigin="anonymous" href="https://cdn.jsdelivr.net/npm/@perspective-dev/viewer@4.3.0/dist/css/themes.css" />
  <script type="importmap">${escapeInlineScript(JSON.stringify(importMap))}</script>
  <style>
    :root {
      color-scheme: dark;
      --bg-primary: #09090b;
      --bg-card: #0f0f12;
      --bg-elevated: #16161a;
      --bg-hover: #1c1c22;
      --border: #1e1e26;
      --border-hover: #2e2e38;
      --text-primary: #ececef;
      --text-secondary: #9898a0;
      --text-muted: #5c5c66;
      --accent: #0A99FF;
      --accent-muted: rgba(10, 153, 255, 0.15);
      --accent-subtle: rgba(10, 153, 255, 0.06);
      --accent-strong: #38bdf8;
      --ring: rgba(255,255,255,0.06);
      --shadow-card: 0 1px 2px rgba(0,0,0,0.4), 0 0 0 1px var(--border);
      --shadow-card-hover: 0 4px 16px rgba(0,0,0,0.5), 0 0 0 1px var(--border-hover);
    }
    html.light {
      color-scheme: light;
      --bg-primary: #fafafa;
      --bg-card: #ffffff;
      --bg-elevated: #f4f4f5;
      --bg-hover: #ebebed;
      --border: #e4e4e7;
      --border-hover: #c8c8ce;
      --text-primary: #09090b;
      --text-secondary: #52525b;
      --text-muted: #a1a1aa;
      --accent: #0A84D0;
      --accent-muted: rgba(10, 132, 208, 0.12);
      --accent-subtle: rgba(10, 132, 208, 0.04);
      --accent-strong: #0284c7;
      --ring: rgba(0,0,0,0.05);
      --shadow-card: 0 1px 2px rgba(0,0,0,0.05), 0 0 0 1px var(--border);
      --shadow-card-hover: 0 4px 12px rgba(0,0,0,0.08), 0 0 0 1px var(--border-hover);
    }
    * {
      box-sizing: border-box;
      font-family: "Inter", system-ui, sans-serif;
    }
    h1, h2, h3 {
      font-family: "DM Sans", system-ui, sans-serif;
    }
    html, body, #root {
      min-height: 100%;
      margin: 0;
      background: var(--bg-primary);
      color: var(--text-primary);
    }
    #root { position: relative; z-index: 1; }
    body::before {
      content: "";
      position: fixed;
      top: -200px;
      left: 50%;
      transform: translateX(-50%);
      width: 900px;
      height: 500px;
      background: radial-gradient(ellipse at center, var(--accent-subtle) 0%, transparent 70%);
      pointer-events: none;
      z-index: 0;
    }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--border-hover); }
    @keyframes fade-up {
      from { opacity: 0; transform: translateY(8px); }
      to { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulse-dot {
      0%, 100% { opacity: 0.3; }
      50% { opacity: 1; }
    }
    @keyframes shimmer {
      0% { background-position: -200% 0; }
      100% { background-position: 200% 0; }
    }
    perspective-viewer, perspective-viewer * {
      transition: none !important;
    }
    perspective-viewer {
      width: 100%;
      height: 100%;
      --plugin--font-family: "Inter", system-ui, sans-serif;
    }
  </style>
  <script id="definite-app-manifest" type="application/json">${escapeInlineScript(JSON.stringify(manifest))}</script>
  ${previewData ? `<script id="definite-app-preview-data" type="application/json">${escapeInlineScript(JSON.stringify(previewData))}</script>` : ""}
</head>
<body>
  <div id="root"></div>
  <script type="module">${escapeInlineScript(jsFile.text)}</script>
</body>
</html>
`;

await writeFile(path.join(outDir, "index.html"), html, "utf8");
console.log(`Built ${path.join(outDir, "index.html")}`);

// =============================================================================
// Embedded variant: strip query declarations from the manifest and inject a
// placeholder script tag that the Definite backend swaps for the real token
// at serve time when the app is fetched via the embed route.
// =============================================================================

function buildEmbeddedManifest(src) {
  const strippedResources = {};
  for (const [key, resource] of Object.entries(src.resources ?? {})) {
    if (!resource || typeof resource !== "object") {
      strippedResources[key] = resource;
      continue;
    }
    const source = resource.source ?? null;
    if (source && typeof source === "object" && (source.type === "sql" || source.type === "cube")) {
      // Replace the source with just its type + embedded:true so the runtime
      // knows which backend branch to call, but doesn't see the raw SQL or
      // cube query JSON.
      strippedResources[key] = {
        ...resource,
        source: { type: source.type, embedded: true },
      };
    } else {
      strippedResources[key] = resource;
    }
  }
  return { ...src, resources: strippedResources };
}

const embeddedManifest = buildEmbeddedManifest(manifest);
const EMBED_TOKEN_PLACEHOLDER =
  '<script id="__definite_embed_token">window.__DEFINITE_EMBED=null;</script>';

const embeddedHtml = html
  .replace(
    `<script id="definite-app-manifest" type="application/json">${escapeInlineScript(JSON.stringify(manifest))}</script>`,
    `<script id="definite-app-manifest" type="application/json">${escapeInlineScript(JSON.stringify(embeddedManifest))}</script>\n  ${EMBED_TOKEN_PLACEHOLDER}`,
  );

if (!embeddedHtml.includes(EMBED_TOKEN_PLACEHOLDER)) {
  throw new Error(
    "build.mjs failed to emit the __definite_embed_token placeholder in the embedded HTML. " +
      "The placeholder script tag format must match what the Definite embed route expects.",
  );
}

await writeFile(path.join(outDir, "index.embedded.html"), embeddedHtml, "utf8");
console.log(`Built ${path.join(outDir, "index.embedded.html")}`);
