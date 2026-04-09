import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const repoRoot = path.dirname(fileURLToPath(import.meta.url));
const buildScript = path.join(repoRoot, "build.mjs");

const appName = process.argv[2];
if (!appName) {
  console.error("Usage: node preview.mjs <app-name>");
  console.error("  e.g. node preview.mjs revenue-explorer");
  process.exit(1);
}

// Look for the app in examples/ first, then current directory
const appDir = path.resolve(
  path.join(repoRoot, "examples", appName, "app.json")
    ? path.join(repoRoot, "examples", appName)
    : appName
);
const previewDataPath = path.join(appDir, "preview-data.json");

const child = spawn(
  process.execPath,
  [buildScript, appDir, "--preview-data", previewDataPath],
  { stdio: "inherit" },
);

child.on("exit", (code) => {
  if (code !== 0) {
    process.exit(code ?? 1);
  }
  console.log(`Preview built at ${path.join(appDir, "dist", "index.html")}`);
});
