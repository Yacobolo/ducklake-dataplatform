import { cpSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, "..", "..");

const sourceDir = join(rootDir, "web", "tokens");
const targetDir = join(rootDir, "internal", "ui", "assets", "static", "vendor", "primer-primitives", "dist", "css");

const includeDirs = [
  ["base", "size"],
  ["base", "motion"],
  ["base", "typography"],
  ["functional", "size"],
  ["functional", "typography"],
  ["functional", "themes"],
];

for (const parts of includeDirs) {
  const from = join(sourceDir, ...parts);
  const to = join(targetDir, ...parts);
  mkdirSync(dirname(to), { recursive: true });
  cpSync(from, to, { recursive: true });
}

console.log("Synced local tokens into internal/ui/assets/static/vendor/primer-primitives.");
