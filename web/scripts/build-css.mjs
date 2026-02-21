import { build } from "esbuild";
import { createHash } from "node:crypto";
import { mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, "..", "..");

const entryFile = join(rootDir, "web", "styles", "app.css");
const outputDir = join(rootDir, "internal", "ui", "assets", "static", "css");
const modeArg = process.argv.find((arg) => arg.startsWith("--mode="));
const mode = modeArg ? modeArg.split("=")[1] : "prod";

if (mode !== "dev" && mode !== "prod") {
  console.error(`Invalid mode '${mode}'. Use --mode=dev or --mode=prod.`);
  process.exit(1);
}

mkdirSync(outputDir, { recursive: true });

const result = await build({
  entryPoints: [entryFile],
  outdir: outputDir,
  bundle: true,
  minify: mode === "prod",
  sourcemap: mode === "dev",
  write: false,
  logLevel: "info",
  loader: {
    ".css": "css",
  },
});

const cssOutput = result.outputFiles.find((file) => file.path.endsWith(".css")) ?? result.outputFiles[0];
if (!cssOutput) {
  console.error("esbuild did not generate CSS output.");
  process.exit(1);
}

const cssText = cssOutput.text;
const appCSSPath = join(outputDir, "app.css");
writeFileSync(appCSSPath, cssText);

if (mode === "prod") {
  const hash = createHash("sha256").update(cssText).digest("hex").slice(0, 10);
  const hashedName = `app.${hash}.css`;
  const hashedPath = join(outputDir, hashedName);

  for (const name of readdirSync(outputDir)) {
    if (name.startsWith("app.") && name.endsWith(".css") && name !== hashedName && name !== "app.css") {
      rmSync(join(outputDir, name));
    }
  }

  writeFileSync(hashedPath, cssText);
  writeFileSync(join(outputDir, "manifest.json"), `${JSON.stringify({ "app.css": hashedName }, null, 2)}\n`);
  console.log(`Built CSS: ${hashedName}`);
} else {
  console.log("Built CSS: app.css");
}
