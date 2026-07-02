import { existsSync, readdirSync, rmSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeRoot = resolve(projectRoot, "src-tauri", "resources", "workflow-runtime");
const modulesRoot = resolve(runtimeRoot, "node_modules");
const cloakCacheRoot = resolve(runtimeRoot, ".cloakbrowser");
const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";
const environment = {
  ...process.env,
  PUPPETEER_SKIP_DOWNLOAD: "true",
  CLOAKBROWSER_CACHE_DIR: cloakCacheRoot,
};

rmSync(modulesRoot, { recursive: true, force: true });
rmSync(resolve(runtimeRoot, "package-lock.json"), { force: true });

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: projectRoot,
    env: environment,
    stdio: "inherit",
    shell: process.platform === "win32" && command.toLowerCase().endsWith(".cmd"),
  });

  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with exit code ${result.status}: ${result.error?.message || "unknown process error"}`);
  }
}

function findFile(root, filename) {
  if (!existsSync(root)) {
    return null;
  }

  for (const entry of readdirSync(root)) {
    const candidate = join(root, entry);
    const stats = statSync(candidate);

    if (stats.isDirectory()) {
      const nested = findFile(candidate, filename);

      if (nested) {
        return nested;
      }
    } else if (entry.toLowerCase() === filename.toLowerCase()) {
      return candidate;
    }
  }

  return null;
}

run(npmCommand, ["install", "--prefix", runtimeRoot, "--omit=dev"]);

for (const packageName of ["puppeteer", "puppeteer-core", "puppeteer-extra", "puppeteer-extra-plugin-stealth", "cloakbrowser"]) {
  if (!existsSync(resolve(modulesRoot, packageName, "package.json"))) {
    throw new Error(`Required portable workflow package is missing: ${packageName}`);
  }
}

const browserName = process.platform === "win32" ? "chrome.exe" : "chrome";
let cloakBrowserBinary = findFile(cloakCacheRoot, browserName);

if (!cloakBrowserBinary) {
  run(process.execPath, [resolve(modulesRoot, "cloakbrowser", "dist", "cli.js"), "install"]);
  cloakBrowserBinary = findFile(cloakCacheRoot, browserName);
}

if (!cloakBrowserBinary) {
  throw new Error(`CloakBrowser binary was not installed below ${cloakCacheRoot}`);
}

console.log(`Portable CloakBrowser ready: ${cloakBrowserBinary}`);
