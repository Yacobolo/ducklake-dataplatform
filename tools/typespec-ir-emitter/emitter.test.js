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

    assert.deepEqual(
      byOperationID.get("createUploadUrl").responses.map((response) => response.status_code),
      [200, 401, 403, 429, 500],
    );
    assert.equal(byOperationID.get("createUploadUrl").responses[0].extensions["x-apigen-response-shape"].body_type, "UploadUrlResponse");
    assert.deepEqual(
      byOperationID.get("getStorageCredential").responses.map((response) => response.status_code),
      [200, 401, 403, 404, 429, 500],
    );
    assert.equal(byOperationID.get("createSchema").extensions["x-authz"].mode, "privilege");
    assert.equal(byOperationID.has("bootstrapComplete"), false);
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
