#!/usr/bin/env node
// Headless smoke check for a built data-app.
//
// Usage: node scripts/headless-smoke.mjs <path-to-built-index.html> [--require-text "<text>"]
//
// What it does:
//   1. Serves the directory containing index.html on a localhost port.
//   2. Launches Playwright Chromium and navigates to the page.
//   3. Listens for uncaught exceptions, page crashes, and console errors.
//   4. Waits for the React root (#root) to render at least one child element,
//      i.e. the runtime mounted without throwing on import or first render.
//   5. Optionally waits for a specific text snippet (--require-text), useful
//      when an example has real preview data and the shell paints fully.
//   6. Exits non-zero if any pageerror/crash fired, or if the wait timed out.
//
// Designed to run identically locally and in CI. The static server is
// stdlib-only (node:http + node:fs); only chromium is an external dep.

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const argv = process.argv.slice(2);
if (argv.length < 1) {
  console.error("Usage: node scripts/headless-smoke.mjs <path-to-index.html> [--require-text <text>]");
  process.exit(2);
}

const htmlPath = path.resolve(argv[0]);
let requireText = null;
for (let i = 1; i < argv.length; i++) {
  if (argv[i] === "--require-text" && argv[i + 1]) {
    requireText = argv[i + 1];
    i++;
  }
}

const serveDir = path.dirname(htmlPath);
const htmlFile = path.basename(htmlPath);

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js":   "text/javascript; charset=utf-8",
  ".mjs":  "text/javascript; charset=utf-8",
  ".css":  "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".parquet": "application/octet-stream",
  ".map":  "application/json; charset=utf-8",
};

function startServer() {
  return new Promise((resolve, reject) => {
    const server = createServer(async (req, res) => {
      try {
        const urlPath = decodeURIComponent((req.url ?? "/").split("?")[0]);
        const rel = urlPath === "/" ? htmlFile : urlPath.replace(/^\/+/, "");
        const full = path.join(serveDir, rel);
        // Prevent path traversal outside serveDir.
        if (!full.startsWith(serveDir)) {
          res.statusCode = 403;
          res.end("forbidden");
          return;
        }
        const s = await stat(full).catch(() => null);
        if (!s || !s.isFile()) {
          res.statusCode = 404;
          res.end("not found");
          return;
        }
        const buf = await readFile(full);
        res.setHeader("Content-Type", MIME[path.extname(full)] ?? "application/octet-stream");
        res.setHeader("Cache-Control", "no-store");
        // Permit cross-origin loaders the runtime might use.
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.end(buf);
      } catch (err) {
        res.statusCode = 500;
        res.end(String(err));
      }
    });
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (typeof addr !== "object" || !addr) {
        reject(new Error("failed to determine server address"));
        return;
      }
      resolve({ server, port: addr.port });
    });
  });
}

async function main() {
  const { server, port } = await startServer();
  const url = `http://127.0.0.1:${port}/${htmlFile}`;
  console.log(`[smoke] serving ${serveDir}`);
  console.log(`[smoke] navigating to ${url}`);

  // Lazy-import Playwright so the script can at least print usage without
  // requiring the dep to be installed.
  const { chromium } = await import("playwright");
  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();

  const pageErrors = [];
  const consoleErrors = [];
  page.on("pageerror", (err) => {
    pageErrors.push(err.message ?? String(err));
  });
  page.on("crash", () => {
    pageErrors.push("page crashed");
  });
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      consoleErrors.push(msg.text());
    }
  });

  let timedOut = false;
  let textMissing = false;
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30_000 });
    // Wait for React to mount _something_ inside #root. If the runtime throws
    // on import or the first render crashes, #root stays empty and this times
    // out.
    await page.waitForFunction(
      () => {
        const root = document.querySelector("#root");
        return !!root && root.childElementCount > 0;
      },
      { timeout: 30_000 },
    );
    if (requireText) {
      // Poll for the text to appear in a visible element. DuckDB-WASM init
      // + first useDataset can take several seconds.
      try {
        await page.waitForFunction(
          (needle) => {
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            let node;
            while ((node = walker.nextNode())) {
              const text = node.nodeValue ?? "";
              if (!text.includes(needle)) continue;
              let el = node.parentElement;
              while (el) {
                if (el.offsetParent !== null) return true;
                el = el.parentElement;
              }
            }
            return false;
          },
          requireText,
          { timeout: 30_000 },
        );
      } catch {
        textMissing = true;
      }
    } else {
      // Settle for non-text-required runs so deferred runtime errors get
      // captured by pageerror.
      await page.waitForTimeout(2000);
    }
  } catch (err) {
    timedOut = true;
    pageErrors.push(`navigation/wait failed: ${err?.message ?? err}`);
  } finally {
    await browser.close();
    server.close();
  }

  const failed = pageErrors.length > 0 || timedOut || textMissing;
  if (consoleErrors.length > 0) {
    console.log("[smoke] console.error messages (informational, non-fatal):");
    for (const e of consoleErrors) console.log(`  - ${e}`);
  }
  if (pageErrors.length > 0) {
    console.log("[smoke] page errors:");
    for (const e of pageErrors) console.log(`  - ${e}`);
  }
  if (textMissing) {
    console.log(`[smoke] required text not found on page: ${JSON.stringify(requireText)}`);
  }
  if (failed) {
    console.log("[smoke] FAIL");
    process.exit(1);
  }
  console.log("[smoke] PASS");
}

main().catch((err) => {
  console.error("[smoke] unexpected error:", err);
  process.exit(1);
});
