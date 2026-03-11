import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import MiniSearch from "minisearch";

const distDir = path.resolve("docs/.vitepress/dist/assets/chunks");
const entries = await readdir(distDir);
const indexFile = entries.find((entry) => entry.startsWith("@localSearchIndexroot.") && entry.endsWith(".js"));

if (!indexFile) {
  throw new Error("could not find built VitePress local search index");
}

const indexModule = await import(pathToFileURL(path.join(distDir, indexFile)).href);
const miniSearch = MiniSearch.loadJSON(indexModule.default, {
  fields: ["title", "titles", "text"],
  storeFields: ["title", "titles"],
  searchOptions: {
    fuzzy: 0.2,
    prefix: true,
    boost: { title: 4, text: 2, titles: 1 },
  },
});

const queries = [
  { query: "authenticate with api key", expectedPrefix: "/how-to/authentication" },
  { query: "run first secure query", expectedPrefix: "/start-here/quickstart" },
  { query: "plan and apply declarative config", expectedPrefix: "/how-to/declarative-workflows" },
];

const failures = [];
for (const { query, expectedPrefix } of queries) {
  const results = miniSearch.search(query).slice(0, 5);
  const found = results.some((result) => String(result.id).startsWith(expectedPrefix));
  if (!found) {
    failures.push({
      query,
      expectedPrefix,
      actual: results.map((result) => result.id),
    });
  }
}

if (failures.length > 0) {
  const details = failures
    .map(({ query, expectedPrefix, actual }) => `${query}: expected ${expectedPrefix}, saw ${actual.join(", ")}`)
    .join("\n");
  throw new Error(`local search retrieval check failed\n${details}`);
}

const output = queries
  .map(({ query, expectedPrefix }) => `ok: ${query} -> ${expectedPrefix}`)
  .join("\n");
process.stdout.write(`${output}\n`);
