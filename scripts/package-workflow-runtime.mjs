import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeRoot = resolve(projectRoot, "src-tauri", "resources", "workflow-runtime");
const nodeModules = resolve(runtimeRoot, "node_modules");
const archivePath = resolve(runtimeRoot, "node_modules.tar.gz");
const manifestPath = resolve(runtimeRoot, "workflow-runtime-manifest.json");

if (!existsSync(nodeModules)) {
  throw new Error(`Workflow runtime dependencies not installed: ${nodeModules}`);
}

const require = createRequire(resolve(runtimeRoot, "package.json"));
const tar = require("tar");
rmSync(archivePath, { force: true });
await tar.c({ gzip: true, file: archivePath, cwd: runtimeRoot }, ["node_modules"]);
rmSync(nodeModules, { recursive: true, force: true });

if (existsSync(manifestPath)) {
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  const dependencyArchive = {
    path: "node_modules.tar.gz",
    bytes: statSync(archivePath).size,
    sha256: createHash("sha256").update(readFileSync(archivePath)).digest("hex"),
  };
  const nextManifest = {
    ...manifest,
    packagedAt: new Date().toISOString(),
    dependencyArchive,
  };
  nextManifest.manifestHash = createHash("sha256")
    .update(JSON.stringify({
      files: nextManifest.files,
      dependencyArchive,
    }))
    .digest("hex");
  writeFileSync(manifestPath, JSON.stringify(nextManifest, null, 2));
}

console.log(`Workflow dependencies packed to ${archivePath}`);
