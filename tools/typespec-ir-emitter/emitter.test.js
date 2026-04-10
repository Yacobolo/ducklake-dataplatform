import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
const specDir = path.join(repoRoot, "api", "spec");

test("emits exact response status codes and operation extensions", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "typespec-ir-emitter-"));
  const configPath = path.join(tempDir, "tspconfig.yaml");

  await writeFile(
    configPath,
    [
      "emit:",
      '  - "@duck/typespec-ir-emitter"',
      "options:",
      '  "@duck/typespec-ir-emitter":',
      `    emitter-output-dir: "${tempDir.replace(/\\/g, "/")}"`,
      '    output-file: "json-ir.json"',
      "",
    ].join("\n"),
    "utf8",
  );

  try {
    await execFileAsync("./node_modules/.bin/tsp", ["compile", "./main.tsp", "--config", configPath], {
      cwd: specDir,
    });

    const ir = JSON.parse(await readFile(path.join(tempDir, "json-ir.json"), "utf8"));
    const byOperationID = new Map(ir.endpoints.map((endpoint) => [endpoint.operation_id, endpoint]));

    assert.deepEqual(ir.info, {
      title: "Duck Data Platform API",
      version: "0.1.0",
      description:
        "Duck Data Platform exposes a secure SQL query layer over DuckDB together with metadata management, RBAC, row-level security, column masking, lineage, orchestration, and semantic modeling APIs backed by SQLite metadata.",
    });
    assert.deepEqual(ir.servers, [
      {
        url: "https://localhost:8443/v1",
        description: "HTTPS base URL for local and proxied deployments",
      },
    ]);
    assert.equal(ir.tags.find((tag) => tag.name === "Health")?.description, "Operational readiness and service health endpoints.");
    assert.equal(ir.tags.find((tag) => tag.name === "Catalogs")?.description, "Catalog registrations, runtime catalogs, search, schema objects, manifests, and ingestion management APIs.");

    assert.deepEqual(
      byOperationID.get("createUploadUrl").responses.map((response) => response.status_code),
      [200, 400, 401, 403, 429, 500],
    );
    assert.deepEqual(byOperationID.get("createUploadUrl").tags, ["Catalogs"]);
    assert.equal(byOperationID.get("createUploadUrl").summary, "Create upload URL");
    assert.equal(byOperationID.get("createUploadUrl").responses[0].extensions["x-apigen-response-shape"].body_type, "UploadUrlResponse");
    assert.deepEqual(
      byOperationID.get("getStorageCredential").responses.map((response) => response.status_code),
      [200, 400, 401, 403, 404, 429, 500],
    );
    assert.deepEqual(byOperationID.get("getHealth").tags, ["Health"]);
    assert.equal(byOperationID.get("getHealth").summary, "Get health");
    assert.equal(byOperationID.get("getHealth").extensions["x-authz"].mode, "public");
    assert.equal(byOperationID.get("getHealth").extensions.security, undefined);
    assert.equal(byOperationID.get("createSchema").extensions["x-authz"].mode, "privilege");
    assert.equal(byOperationID.get("getCatalog").extensions["x-cli-command"], "catalog info");
    assert.equal(byOperationID.get("updatePrincipal").extensions["x-cli-command"], "security principals set-admin");
    assert.equal(byOperationID.get("createSemanticModelRelationship").extensions["x-cli-command"], "semantic semantic-relationships create");
    assert.equal(byOperationID.get("listSemanticModelRelationships").extensions["x-cli-command"], "semantic semantic-relationships list");
    assert.equal(
      byOperationID.get("listSemanticModelRelationships").responses[0].extensions["x-apigen-response-shape"].body_type,
      "SemanticRelationshipList",
    );
    assert.equal(byOperationID.has("bootstrapComplete"), false);
    assert.deepEqual(ir.schemas.Error.required, ["code", "message"]);
    assert.equal(ir.schemas.Error.properties.message.schema.type, "string");
    assert.deepEqual(ir.schemas.PaginatedGroupMembers.properties.data.schema, {
      type: "array",
      items: { ref: "GroupMember" },
    });
    assert.deepEqual(ir.schemas.ComputeEndpointStatus.enum, ["ACTIVE", "INACTIVE", "STARTING", "STOPPING", "ERROR"]);
    assert.equal(ir.schemas.LocalLoginRequest.type, "object");
    assert.equal(ir.schemas.BootstrapTokenRequest.type, "object");
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});
