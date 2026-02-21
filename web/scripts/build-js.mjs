import { build } from "esbuild";
import { createHash } from "node:crypto";
import { mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, extname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, "..", "..");
const webDir = join(rootDir, "web");

const entryFiles = [
  join(webDir, "src", "sql-editor.ts"),
  join(webDir, "src", "notebook.ts"),
];

const outputDir = join(rootDir, "internal", "ui", "assets", "static", "js");
const modeArg = process.argv.find((arg) => arg.startsWith("--mode="));
const mode = modeArg ? modeArg.split("=")[1] : "prod";

if (mode !== "dev" && mode !== "prod") {
  console.error(`Invalid mode '${mode}'. Use --mode=dev or --mode=prod.`);
  process.exit(1);
}

mkdirSync(outputDir, { recursive: true });

const result = await build({
  absWorkingDir: webDir,
  entryPoints: entryFiles,
  outdir: outputDir,
  bundle: true,
  format: "iife",
  target: "es2020",
  minify: mode === "prod",
  sourcemap: mode === "dev",
  write: false,
  logLevel: "info",
  loader: {
    ".ts": "ts",
  },
});

const jsOutputs = result.outputFiles.filter((file) => file.path.endsWith(".js"));
if (jsOutputs.length === 0) {
  console.error("esbuild did not generate JavaScript output.");
  process.exit(1);
}

const manifest = {};
for (const output of jsOutputs) {
  const sourceName = basename(output.path);
  const baseName = sourceName.slice(0, -extname(sourceName).length);
  const appJSPath = join(outputDir, sourceName);
  writeFileSync(appJSPath, output.text);

  if (mode === "prod") {
    const hash = createHash("sha256").update(output.text).digest("hex").slice(0, 10);
    const hashedName = `${baseName}.${hash}.js`;
    const hashedPath = join(outputDir, hashedName);

    for (const name of readdirSync(outputDir)) {
      if (name.startsWith(`${baseName}.`) && name.endsWith(".js") && name !== hashedName && name !== sourceName) {
        rmSync(join(outputDir, name));
      }
    }

    writeFileSync(hashedPath, output.text);
    manifest[sourceName] = hashedName;
    console.log(`Built JS: ${hashedName}`);
  } else {
    console.log(`Built JS: ${sourceName}`);
  }
}

if (mode === "prod") {
  writeFileSync(join(outputDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
}
