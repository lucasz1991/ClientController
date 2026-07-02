import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
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

cpSync(
  resolve(projectRoot, "src-tauri", "workflow-runtime-package.json"),
  resolve(targetRoot, "package.json"),
);

const bundledNodeName = process.platform === "win32" ? "node.exe" : "node";
mkdirSync(resolve(targetRoot, "bin"), { recursive: true });
cpSync(process.execPath, resolve(targetRoot, "bin", bundledNodeName));

console.log(`Workflow runtime synchronized to ${targetRoot}`);
