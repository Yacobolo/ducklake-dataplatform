import { RuntimeManifestClient, type RuntimeManifestRequest } from "./manifest-client";
import { getDuckDBWasmRuntime, getDuckDBWasmSupport } from "./duckdb-wasm";
import type { BrowserRuntimeSupport, ManifestResponse, ManifestBrowserRuntime } from "./types";

const DEFAULT_SCHEMA = "main";
const MAX_LOCAL_RELATIONS = 4;
const MAX_LOCAL_SQL_BYTES = 128 * 1024;
const SUPPORTED_BROWSER_RUNTIME_VERSION = "duckdb-wasm/v1alpha1";
const SUPPORTED_AUTH_MODE = "WEB_SESSION";
const SUPPORTED_FILE_URL_TYPE = "HTTPS_PRESIGNED_URL";

export interface LocalQueryExecutionRequest {
  sql: string;
  catalog?: string;
  schema?: string;
}

export interface LocalQueryExecutionResult {
  columns: string[];
  rows: unknown[][];
  rowCount: number;
}

export interface LocalQueryRelation {
  schema: string;
  table: string;
}

export interface LocalQueryGuardrailPreview {
  relations: LocalQueryRelation[];
  limit: number | null;
  primaryRelation: LocalQueryRelation;
}

export interface LocalQueryPreflight {
  support: BrowserRuntimeSupport;
  preview: LocalQueryGuardrailPreview;
  manifests: ManifestResponse[];
  guidanceMaxRows: number;
}

export class LocalQueryUnsupportedError extends Error {}
export class LocalQueryCancelledError extends Error {}

export async function executeLocalQuery(
  manifestClient: RuntimeManifestClient,
  request: LocalQueryExecutionRequest,
): Promise<{ result: LocalQueryExecutionResult; support: BrowserRuntimeSupport; manifests: ManifestResponse[] }> {
  const preflight = await preflightLocalQuery(manifestClient, request);
  const runtime = await getDuckDBWasmRuntime();
  await runtime.resetSession();

  for (const manifest of preflight.manifests) {
    const fileNames = await registerManifestFiles(runtime, manifest);
    await runtime.exec(buildSecureViewSQL(manifest, fileNames));
  }

  const table = await runtime.query(request.sql);
  const result = toExecutionResult(table);
  return {
    result,
    support: preflight.support,
    manifests: preflight.manifests,
  };
}

export async function preflightLocalQuery(
  manifestClient: RuntimeManifestClient,
  request: LocalQueryExecutionRequest,
): Promise<LocalQueryPreflight> {
  const support = await getDuckDBWasmSupport();
  if (!support.supported) {
    throw new LocalQueryUnsupportedError(support.message);
  }

  const preview = previewLocalQuery(request.sql, request.schema);
  const manifests = await fetchManifestsForRelations(manifestClient, request.catalog, preview.relations);
  const guidanceMaxRows = validateQueryGuardrails(request.sql, manifests);

  return {
    support,
    preview,
    manifests,
    guidanceMaxRows,
  };
}

export function previewLocalQuery(sqlText: string, defaultSchema?: string): LocalQueryGuardrailPreview {
  validateQueryShape(sqlText);

  const relations = parseBaseRelations(sqlText, defaultSchema);
  if (relations.length === 0) {
    throw new LocalQueryUnsupportedError("Browser-local execution requires at least one concrete table reference.");
  }
  if (relations.length > MAX_LOCAL_RELATIONS) {
    throw new LocalQueryUnsupportedError(
      `Browser-local execution currently supports up to ${MAX_LOCAL_RELATIONS} base tables per query.`,
    );
  }

  return {
    relations,
    limit: parseQueryLimit(sqlText),
    primaryRelation: relations[0],
  };
}

function validateQueryShape(sqlText: string): void {
  const normalized = sqlText.trim();
  if (!/^(select|with)\b/i.test(normalized)) {
    throw new LocalQueryUnsupportedError("Browser-local execution currently supports SELECT queries only.");
  }
  if (normalized.length > MAX_LOCAL_SQL_BYTES) {
    throw new LocalQueryUnsupportedError("Browser-local execution currently limits SQL text size for browser stability.");
  }
  if (/\b(insert|update|delete|merge|copy|alter|drop|create|attach|call)\b/i.test(normalized)) {
    throw new LocalQueryUnsupportedError("Browser-local execution only supports read-only SQL.");
  }
}

async function fetchManifestsForRelations(
  manifestClient: RuntimeManifestClient,
  catalog: string | undefined,
  relations: LocalQueryRelation[],
): Promise<ManifestResponse[]> {
  const manifests: ManifestResponse[] = [];
  for (const relation of relations) {
    const request: RuntimeManifestRequest = {
      catalog,
      schema: relation.schema,
      table: relation.table,
    };
    const manifest = await manifestClient.fetchManifest(request);
    validateManifestRuntime(manifest);
    manifests.push(manifest);
  }
  return manifests;
}

function validateManifestRuntime(manifest: ManifestResponse): void {
  const runtime = manifest.browser_runtime;
  if (!runtime) {
    throw new LocalQueryUnsupportedError("Manifest does not include browser runtime metadata.");
  }
  if (!runtime.supported) {
    throw new LocalQueryUnsupportedError(runtime.status_reason || "Browser-local runtime is not enabled for this manifest.");
  }
  if (runtime.required_runtime_version !== SUPPORTED_BROWSER_RUNTIME_VERSION) {
    throw new LocalQueryUnsupportedError(
      `Browser runtime version mismatch. Server requires ${runtime.required_runtime_version} but the UI supports ${SUPPORTED_BROWSER_RUNTIME_VERSION}.`,
    );
  }
  if (!runtime.required_auth_modes.includes(SUPPORTED_AUTH_MODE)) {
    throw new LocalQueryUnsupportedError("Browser-local execution requires a web-session manifest auth mode.");
  }
  if (!runtime.supported_file_url_types.includes(SUPPORTED_FILE_URL_TYPE)) {
    throw new LocalQueryUnsupportedError("Browser-local execution requires HTTPS presigned file URLs.");
  }
  if (!manifest.files.length) {
    throw new LocalQueryUnsupportedError(`Manifest for ${manifest.schema}.${manifest.table} does not include any files.`);
  }
  const expiresAt = Date.parse(manifest.expires_at);
  if (Number.isNaN(expiresAt) || expiresAt <= Date.now()) {
    throw new LocalQueryUnsupportedError(`Manifest for ${manifest.schema}.${manifest.table} is already expired.`);
  }
  for (const fileURL of manifest.files) {
    validateManifestFileURL(fileURL, runtime);
  }
}

function validateManifestFileURL(fileURL: string, runtime: ManifestBrowserRuntime): void {
  let url: URL;
  try {
    url = new URL(fileURL);
  } catch {
    throw new LocalQueryUnsupportedError("Manifest returned an invalid file URL for browser-local execution.");
  }
  if (url.protocol !== "https:") {
    throw new LocalQueryUnsupportedError("Browser-local execution currently requires HTTPS presigned file URLs.");
  }
  if (runtime.requires_cors && !url.hostname) {
    throw new LocalQueryUnsupportedError("Browser-local execution requires CORS-capable presigned file URLs.");
  }
}

function validateQueryGuardrails(sqlText: string, manifests: ManifestResponse[]): number {
  const guidanceMaxRows = manifests.reduce((min, manifest) => {
    const runtime = manifest.browser_runtime;
    if (!runtime) {
      return min;
    }
    return Math.min(min, runtime.recommended_max_rows);
  }, Number.POSITIVE_INFINITY);

  const limit = parseQueryLimit(sqlText);
  if (limit === null) {
    throw new LocalQueryUnsupportedError(
      `Browser-local execution requires an explicit LIMIT. Keep local queries at or below ${guidanceMaxRows} rows, or switch to Shared Endpoint.`,
    );
  }
  if (Number.isNaN(limit) || limit < 1) {
    throw new LocalQueryUnsupportedError("Browser-local execution requires a positive LIMIT value.");
  }
  if (limit > guidanceMaxRows) {
    throw new LocalQueryUnsupportedError(
      `Requested LIMIT ${limit} exceeds the browser-local guidance of ${guidanceMaxRows} rows. Switch to Shared Endpoint or lower the LIMIT.`,
    );
  }
  return guidanceMaxRows;
}

function parseBaseRelations(sqlText: string, defaultSchema?: string): LocalQueryRelation[] {
  const cteNames = parseCTENames(sqlText);
  const matches = [...sqlText.matchAll(/\b(from|join)\s+([a-zA-Z0-9_."]+)/gi)];
  const relations: LocalQueryRelation[] = [];
  const seen = new Set<string>();

  for (const match of matches) {
    const identifier = match[2].replaceAll("\"", "").trim();
    if (!identifier) {
      continue;
    }

    const lastPart = identifier.split(".").filter(Boolean).at(-1) ?? identifier;
    if (cteNames.has(lastPart.toLowerCase())) {
      continue;
    }

    const parts = identifier.split(".").filter(Boolean);
    let relation: LocalQueryRelation;
    if (parts.length === 1) {
      relation = { schema: defaultSchema || DEFAULT_SCHEMA, table: parts[0] };
    } else if (parts.length === 2) {
      relation = { schema: parts[0], table: parts[1] };
    } else {
      throw new LocalQueryUnsupportedError(
        "Browser-local execution currently supports table or schema.table references only.",
      );
    }

    const key = `${relation.schema}.${relation.table}`.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      relations.push(relation);
    }
  }

  return relations;
}

function parseCTENames(sqlText: string): Set<string> {
  const names = new Set<string>();
  const withClause = /^\s*with\s+([\s\S]+?)\bselect\b/i.exec(sqlText);
  if (!withClause) {
    return names;
  }
  const ctePattern = /(?:^|,)\s*("?[\w]+"?)\s+as\s*\(/gi;
  for (const match of withClause[1].matchAll(ctePattern)) {
    const name = match[1].replaceAll("\"", "").trim().toLowerCase();
    if (name) {
      names.add(name);
    }
  }
  return names;
}

function parseQueryLimit(sqlText: string): number | null {
  const limitMatch = /\blimit\s+(\d+)\b/i.exec(sqlText);
  if (!limitMatch) {
    return null;
  }
  return Number.parseInt(limitMatch[1], 10);
}

function buildSecureViewSQL(manifest: ManifestResponse, fileNames: string[]): string {
  const schemaIdentifier = quoteIdentifier(manifest.schema);
  const tableIdentifier = quoteIdentifier(manifest.table);
  const selectExpressions = manifest.columns.map((column) => {
    const mask = manifest.column_masks?.[column.name];
    if (mask) {
      return `${mask} AS ${quoteIdentifier(column.name)}`;
    }
    return `${quoteIdentifier(column.name)} AS ${quoteIdentifier(column.name)}`;
  });
  const fromPaths = fileNames.map((path) => `'${escapeString(path)}'`).join(", ");
  const whereClause = manifest.row_filters.length > 0 ? ` WHERE ${manifest.row_filters.map((filter) => `(${filter})`).join(" AND ")}` : "";

  return [
    `CREATE SCHEMA IF NOT EXISTS ${schemaIdentifier};`,
    `CREATE OR REPLACE VIEW ${schemaIdentifier}.${tableIdentifier} AS`,
    `SELECT ${selectExpressions.join(", ")}`,
    `FROM read_parquet([${fromPaths}])${whereClause};`,
  ].join("\n");
}

function buildVirtualParquetPath(schema: string, table: string, index: number): string {
  return `/quack-runtime/${schema}/${table}/part-${index}.parquet`;
}

async function registerManifestFiles(
  runtime: { registerFileURL(path: string, sourceURL: string): Promise<void> },
  manifest: ManifestResponse,
): Promise<string[]> {
  const names: string[] = [];
  for (let index = 0; index < manifest.files.length; index += 1) {
    const path = buildVirtualParquetPath(manifest.schema, manifest.table, index);
    await runtime.registerFileURL(path, manifest.files[index]);
    names.push(path);
  }
  return names;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

function escapeString(value: string): string {
  return value.replaceAll("'", "''");
}

function normalizeArrowValue(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  return value;
}

function toExecutionResult(table: {
  schema: { fields: Array<{ name: string }> };
  toArray(): unknown[];
}): LocalQueryExecutionResult {
  const columns = table.schema.fields.map((field) => field.name);
  const rows = table.toArray().map((row) => columns.map((column) => normalizeArrowValue((row as Record<string, unknown>)[column])));
  return {
    columns,
    rows,
    rowCount: rows.length,
  };
}
