import { transform } from "esbuild";
import { createHash } from "node:crypto";
import { mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import chokidar from "chokidar";
import postcss from "postcss";
import postcssImport from "postcss-import";
import tailwindcss from "@tailwindcss/postcss";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const rootDir = join(__dirname, "..", "..");

const entryFile = join(rootDir, "web", "styles", "app.css");
const outputDir = join(rootDir, "internal", "ui", "assets", "static", "css");
const modeArg = process.argv.find((arg) => arg.startsWith("--mode="));
const mode = modeArg ? modeArg.split("=")[1] : "prod";
const shouldWatch = process.argv.includes("--watch");

if (mode !== "dev" && mode !== "prod") {
  console.error(`Invalid mode '${mode}'. Use --mode=dev or --mode=prod.`);
  process.exit(1);
}

mkdirSync(outputDir, { recursive: true });

const processor = postcss([
  postcssImport(),
  tailwindcss(),
]);

async function buildCSS() {
  const result = await processor.process(`@import "${entryFile}";`, {
    from: entryFile,
    to: join(outputDir, "app.css"),
    map: mode === "dev" ? { inline: false, annotation: true } : false,
  });

  const transformed = await transform(result.css, {
    loader: "css",
    minify: mode === "prod",
    sourcemap: false,
  });

  const cssText = transformed.code;
  const appCSSPath = join(outputDir, "app.css");
  writeFileSync(appCSSPath, cssText);

  if (mode === "dev") {
    const map = result.map?.toString();
    if (map) {
      writeFileSync(join(outputDir, "app.css.map"), map);
    }
    console.log("Built CSS: app.css");
    return;
  }

  const hash = createHash("sha256").update(cssText).digest("hex").slice(0, 10);
  const hashedName = `app.${hash}.css`;
  const hashedPath = join(outputDir, hashedName);

  for (const name of readdirSync(outputDir)) {
    if (name.startsWith("app.") && name.endsWith(".css") && name !== hashedName && name !== "app.css") {
      rmSync(join(outputDir, name));
    }
  }

  rmSync(join(outputDir, "app.css.map"), { force: true });
  writeFileSync(hashedPath, cssText);
  writeFileSync(join(outputDir, "manifest.json"), `${JSON.stringify({ "app.css": hashedName }, null, 2)}\n`);
  console.log(`Built CSS: ${hashedName}`);
}

await buildCSS();

if (shouldWatch) {
  const watchPaths = [
    join(rootDir, "web", "styles"),
    join(rootDir, "web", "tokens"),
    join(rootDir, "web", "src"),
    join(rootDir, "internal", "ui"),
  ];

  let running = false;
  let pending = false;

  async function rebuild() {
    if (running) {
      pending = true;
      return;
    }
    running = true;
    try {
      await buildCSS();
    } catch (error) {
      console.error(error);
    } finally {
      running = false;
      if (pending) {
        pending = false;
        await rebuild();
      }
    }
  }

  const watcher = chokidar.watch(watchPaths, {
    ignoreInitial: true,
  });

  watcher.on("all", async (_event, changedPath) => {
    console.log(`CSS change detected: ${changedPath}`);
    await rebuild();
  });

  console.log("Watching CSS sources...");
}
