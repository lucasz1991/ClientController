import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const factoryRoot = resolve(projectRoot, "..", "AiUserFactory");
const targetRoot = resolve(projectRoot, "src-tauri", "resources", "workflow-runtime");

if (!existsSync(resolve(factoryRoot, "node", "workflows", "run_step.cjs"))) {
  throw new Error(`AiUserFactory workflow runtime not found: ${factoryRoot}`);
}

for (const relative of ["node/workflows", "resources/node/register/lib"]) {
  const target = resolve(targetRoot, relative);
  rmSync(target, { recursive: true, force: true });
  mkdirSync(dirname(target), { recursive: true });
  cpSync(resolve(factoryRoot, relative), target, { recursive: true });
}

function fileHash(filePath) {
  return createHash("sha256").update(readFileSync(filePath)).digest("hex");
}

function listRuntimeFiles(root, prefix = "") {
  const directory = resolve(root, prefix);
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const nextPrefix = prefix ? `${prefix}/${entry.name}` : entry.name;
    const absolute = resolve(root, nextPrefix);

    if (entry.isDirectory()) {
      return listRuntimeFiles(root, nextPrefix);
    }

    if (!entry.isFile() || nextPrefix.includes("/node_modules/") || nextPrefix.endsWith("node_modules.tar.gz")) {
      return [];
    }

    return [{
      path: nextPrefix,
      bytes: statSync(absolute).size,
      sha256: fileHash(absolute),
    }];
  });
}

cpSync(
  resolve(projectRoot, "src-tauri", "workflow-runtime-package.json"),
  resolve(targetRoot, "package.json"),
);

const bundledNodeName = process.platform === "win32" ? "node.exe" : "node";
mkdirSync(resolve(targetRoot, "bin"), { recursive: true });
cpSync(process.execPath, resolve(targetRoot, "bin", bundledNodeName));

const manifestFiles = listRuntimeFiles(targetRoot)
  .filter((file) => file.path !== "workflow-runtime-manifest.json")
  .sort((a, b) => a.path.localeCompare(b.path));
const manifestHash = createHash("sha256")
  .update(JSON.stringify(manifestFiles.map(({ path, sha256 }) => [path, sha256])))
  .digest("hex");
writeFileSync(
  resolve(targetRoot, "workflow-runtime-manifest.json"),
  JSON.stringify({
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    sourceRoot: relative(projectRoot, factoryRoot) || ".",
    runtimeRoot: relative(projectRoot, targetRoot),
    nodeBinary: `bin/${bundledNodeName}`,
    manifestHash,
    files: manifestFiles,
  }, null, 2),
);

console.log(`Workflow runtime synchronized to ${targetRoot}`);
