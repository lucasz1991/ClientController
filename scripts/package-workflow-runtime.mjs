import { createRequire } from "node:module";
import { existsSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeRoot = resolve(projectRoot, "src-tauri", "resources", "workflow-runtime");
const nodeModules = resolve(runtimeRoot, "node_modules");
const archivePath = resolve(runtimeRoot, "node_modules.tar.gz");

if (!existsSync(nodeModules)) {
  throw new Error(`Workflow runtime dependencies not installed: ${nodeModules}`);
}

const require = createRequire(resolve(runtimeRoot, "package.json"));
const tar = require("tar");
rmSync(archivePath, { force: true });
await tar.c({ gzip: true, file: archivePath, cwd: runtimeRoot }, ["node_modules"]);
rmSync(nodeModules, { recursive: true, force: true });

console.log(`Workflow dependencies packed to ${archivePath}`);
