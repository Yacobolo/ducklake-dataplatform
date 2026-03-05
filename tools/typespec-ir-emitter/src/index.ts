import type { EmitContext, Model, Namespace, Operation, Type } from "@typespec/compiler";
import { getDoc, resolvePath } from "@typespec/compiler";
import { getOperationVerb, getRoutePath } from "@typespec/http";

type EmitterOptions = {
  "output-file"?: string;
};

type IRSchemaRef = {
  ref?: string;
  type?: string;
  format?: string;
};

type IRSchema = {
  type: string;
  properties?: Record<string, { description?: string; schema: IRSchemaRef }>;
  required?: string[];
  items?: IRSchemaRef;
};

type IREndpoint = {
  method: string;
  path: string;
  operation_id: string;
  summary?: string;
  description?: string;
  tags: string[];
  parameters?: Array<{
    name: string;
    in: "path" | "query";
    required?: boolean;
    description?: string;
    schema: IRSchemaRef;
  }>;
  request_body?: {
    required?: boolean;
    description?: string;
    schema: IRSchemaRef;
  };
  responses: Array<{
    status_code: number;
    description: string;
    schema?: IRSchemaRef;
  }>;
  extensions?: Record<string, unknown>;
};

type IRDocument = {
  schema_version: "v1";
  info: {
    title: string;
    version: string;
    description: string;
  };
  servers: Array<{ url: string; description: string }>;
  tags: Array<{ name: string; description: string }>;
  schemas: Record<string, IRSchema>;
  endpoints: IREndpoint[];
};

export async function $onEmit(context: EmitContext<EmitterOptions>): Promise<void> {
  const outputFile = context.options["output-file"] ?? "json-ir.json";
  const outputPath = resolvePath(context.emitterOutputDir, outputFile);

  const schemas: Record<string, IRSchema> = {
    Error: {
      type: "object",
      properties: {
        code: { description: "HTTP status code", schema: { type: "integer", format: "int32" } },
        message: { description: "Error message", schema: { type: "string" } },
      },
      required: ["code", "message"],
    },
  };

  const endpoints = collectEndpoints(context, schemas);
  const ir: IRDocument = {
    schema_version: "v1",
    info: {
      title: "Duck Data Platform API",
      version: "0.1.0",
      description: "TypeSpec-emitted JSON IR for Go code generation.",
    },
    servers: [{ url: "https://localhost:8080", description: "Local development" }],
    tags: [
      { name: "system", description: "System endpoints" },
      { name: "query", description: "Query execution endpoints" },
      { name: "security", description: "Security and access-control endpoints" },
      { name: "catalogs", description: "Catalog and metadata endpoints" },
      { name: "manifest", description: "Manifest and data-access endpoints" },
      { name: "observability", description: "Audit and query observability endpoints" },
      { name: "storage", description: "Storage credentials and external locations" },
      { name: "compute", description: "Compute endpoint management" },
      { name: "lineage", description: "Lineage graph and impact analysis" },
      { name: "notebooks", description: "Notebook authoring and execution" },
      { name: "pipelines", description: "Pipeline orchestration endpoints" },
      { name: "models", description: "Model lifecycle and testing" },
      { name: "macros", description: "Macro management and revisions" },
      { name: "semantic", description: "Semantic models and metrics" },
      { name: "governance", description: "Tags and classifications" },
      { name: "api", description: "General API endpoints" },
    ],
    schemas,
    endpoints,
  };

  await context.program.host.writeFile(outputPath, `${JSON.stringify(ir, null, 2)}\n`);
}

function collectEndpoints(
  context: EmitContext<EmitterOptions>,
  schemas: Record<string, IRSchema>,
): IREndpoint[] {
  const endpoints: IREndpoint[] = [];
  visitNamespace(context, context.program.getGlobalNamespaceType(), schemas, endpoints);
  endpoints.sort((a, b) => {
    if (a.path !== b.path) {
      return a.path.localeCompare(b.path);
    }
    if (a.method !== b.method) {
      return httpMethodRank(a.method) - httpMethodRank(b.method);
    }
    return a.operation_id.localeCompare(b.operation_id);
  });
  return endpoints;
}

function visitNamespace(
  context: EmitContext<EmitterOptions>,
  namespace: Namespace,
  schemas: Record<string, IRSchema>,
  endpoints: IREndpoint[],
): void {
  for (const operation of namespace.operations.values()) {
    appendOperation(context, operation, schemas, endpoints);
  }
  for (const childNamespace of namespace.namespaces.values()) {
    visitNamespace(context, childNamespace, schemas, endpoints);
  }
}

function appendOperation(
  context: EmitContext<EmitterOptions>,
  operation: Operation,
  schemas: Record<string, IRSchema>,
  endpoints: IREndpoint[],
): void {
  const verb = getOperationVerb(context.program, operation);
  if (!verb) {
    return;
  }

  const routePath = getRoutePath(context.program, operation)?.path;
  if (!routePath) {
    return;
  }

  const responseSchema = isNoContentReturn(operation.returnType) ? undefined : toSchemaRef(operation.returnType, schemas);
  const bodySchema = getBodySchema(context, operation, schemas);
  const parameters = getParameters(context, operation, routePath, schemas);
  const authzMetadata = operationAuthz(operation.name);
  const isAuthenticated = authzMetadata.mode !== "public";
  const operationDoc = cleanText(getDoc(context.program, operation));
  const cliCommand = cliCommandForOperation(operation.name);

  const endpoint: IREndpoint = {
    method: verb.toLowerCase(),
    path: routePath,
    operation_id: operation.name,
    summary: humanizeOperationName(operation.name),
    ...(operationDoc ? { description: operationDoc } : {}),
    tags: tagsForRoute(routePath),
    responses: buildResponses(verb.toLowerCase(), routePath, operation.name, responseSchema),
  };

  if (parameters.length > 0) {
    endpoint.parameters = parameters;
  }

  if (bodySchema) {
    endpoint.request_body = {
      required: bodySchema.required,
      description: bodySchema.description,
      schema: bodySchema.schema,
    };
  }

  const extensions: Record<string, unknown> = {};
  if (cliCommand !== "") {
    extensions["x-cli-command"] = cliCommand;
  }
  if (isAuthenticated) {
    extensions.security = [{ ApiKeyAuth: [] }, { BearerAuth: [] }];
    extensions["x-authz"] = authzMetadata;
  }
  if (Object.keys(extensions).length > 0) {
    endpoint.extensions = extensions;
  }

  endpoints.push(endpoint);
}

function getBodySchema(
  context: EmitContext<EmitterOptions>,
  operation: Operation,
  schemas: Record<string, IRSchema>,
): { schema: IRSchemaRef; required: boolean; description: string } | undefined {
  const bodyParam = operation.parameters.properties.get("body");
  if (!bodyParam) {
    return undefined;
  }
  return {
    schema: toSchemaRef(bodyParam.type, schemas),
    required: !bodyParam.optional,
    description: cleanText(getDoc(context.program, bodyParam)) || "Request payload",
  };
}

function getParameters(
  context: EmitContext<EmitterOptions>,
  operation: Operation,
  routePath: string,
  schemas: Record<string, IRSchema>,
): NonNullable<IREndpoint["parameters"]> {
  const parameters: NonNullable<IREndpoint["parameters"]> = [];
  for (const [name, prop] of operation.parameters.properties) {
    if (name === "body") {
      continue;
    }

    const inPath = routePath.includes(`{${name}}`);
    parameters.push({
      name,
      in: inPath ? "path" : "query",
      required: inPath || !prop.optional,
      description: cleanText(getDoc(context.program, prop)),
      schema: toSchemaRef(prop.type, schemas),
    });
  }
  parameters.sort((a, b) => a.name.localeCompare(b.name));
  return parameters;
}

function httpMethodRank(method: string): number {
  switch (method.toLowerCase()) {
    case "get":
      return 0;
    case "post":
      return 1;
    case "put":
      return 2;
    case "patch":
      return 3;
    case "delete":
      return 4;
    default:
      return 100;
  }
}

type AuthzMode = "public" | "authenticated" | "admin_only" | "privilege";

type AuthzCheck = {
  securable_type: string;
  privilege: string;
  securable_id_source: string;
};

type AuthzMetadata = {
  mode: AuthzMode;
  checks?: AuthzCheck[];
};

function operationAuthz(operationName: string): AuthzMetadata {
  if (operationName === "getHealth") {
    return { mode: "public" };
  }
  if (operationName === "createGrant" || operationName === "deleteGrant") {
    return { mode: "admin_only" };
  }
  const privilegeChecks: Record<string, AuthzCheck> = {
    createSchema: {
      securable_type: "catalog",
      privilege: "CREATE_SCHEMA",
      securable_id_source: "catalog_name_param",
    },
    updateSchema: {
      securable_type: "schema",
      privilege: "CREATE_SCHEMA",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteSchema: {
      securable_type: "schema",
      privilege: "CREATE_SCHEMA",
      securable_id_source: "runtime_resolved_object_id",
    },
    createTable: {
      securable_type: "schema",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    updateTable: {
      securable_type: "table",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteTable: {
      securable_type: "table",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    updateColumn: {
      securable_type: "table",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    createView: {
      securable_type: "schema",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    updateView: {
      securable_type: "schema",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteView: {
      securable_type: "schema",
      privilege: "CREATE_TABLE",
      securable_id_source: "runtime_resolved_object_id",
    },
    createVolume: {
      securable_type: "catalog",
      privilege: "CREATE_VOLUME",
      securable_id_source: "catalog_sentinel",
    },
    updateVolume: {
      securable_type: "volume",
      privilege: "CREATE_VOLUME",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteVolume: {
      securable_type: "volume",
      privilege: "CREATE_VOLUME",
      securable_id_source: "runtime_resolved_object_id",
    },
    createStorageCredential: {
      securable_type: "catalog",
      privilege: "CREATE_STORAGE_CREDENTIAL",
      securable_id_source: "catalog_sentinel",
    },
    updateStorageCredential: {
      securable_type: "storage_credential",
      privilege: "CREATE_STORAGE_CREDENTIAL",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteStorageCredential: {
      securable_type: "storage_credential",
      privilege: "CREATE_STORAGE_CREDENTIAL",
      securable_id_source: "runtime_resolved_object_id",
    },
    createExternalLocation: {
      securable_type: "catalog",
      privilege: "CREATE_EXTERNAL_LOCATION",
      securable_id_source: "catalog_sentinel",
    },
    updateExternalLocation: {
      securable_type: "external_location",
      privilege: "CREATE_EXTERNAL_LOCATION",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteExternalLocation: {
      securable_type: "external_location",
      privilege: "CREATE_EXTERNAL_LOCATION",
      securable_id_source: "runtime_resolved_object_id",
    },
    createComputeEndpoint: {
      securable_type: "catalog",
      privilege: "MANAGE_COMPUTE",
      securable_id_source: "catalog_sentinel",
    },
    updateComputeEndpoint: {
      securable_type: "compute_endpoint",
      privilege: "MANAGE_COMPUTE",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteComputeEndpoint: {
      securable_type: "compute_endpoint",
      privilege: "MANAGE_COMPUTE",
      securable_id_source: "runtime_resolved_object_id",
    },
    createComputeAssignment: {
      securable_type: "compute_endpoint",
      privilege: "MANAGE_COMPUTE",
      securable_id_source: "runtime_resolved_object_id",
    },
    deleteComputeAssignment: {
      securable_type: "catalog",
      privilege: "MANAGE_COMPUTE",
      securable_id_source: "catalog_sentinel",
    },
  };
  const check = privilegeChecks[operationName];
  if (check) {
    return { mode: "privilege", checks: [check] };
  }
  return { mode: "authenticated" };
}

function tagsForRoute(routePath: string): string[] {
  if (routePath === "/healthz") {
    return ["system"];
  }
  if (routePath === "/manifest") {
    return ["manifest"];
  }
  if (
    routePath === "/audit-logs" ||
    routePath === "/query-history" ||
    routePath.includes("/metastore/summary")
  ) {
    return ["observability"];
  }
  if (routePath.startsWith("/catalogs")) {
    return ["catalogs"];
  }
  if (routePath.startsWith("/storage-credentials") || routePath.startsWith("/external-locations")) {
    return ["storage"];
  }
  if (routePath.startsWith("/compute-endpoints")) {
    return ["compute"];
  }
  if (routePath.startsWith("/lineage")) {
    return ["lineage"];
  }
  if (routePath.startsWith("/notebooks")) {
    return ["notebooks"];
  }
  if (routePath.startsWith("/pipelines") || routePath.startsWith("/sources")) {
    return ["pipelines"];
  }
  if (routePath.startsWith("/models") || routePath.startsWith("/model-runs")) {
    return ["models"];
  }
  if (routePath.startsWith("/macros")) {
    return ["macros"];
  }
  if (
    routePath.startsWith("/semantic-models") ||
    routePath.startsWith("/metrics") ||
    routePath.startsWith("/metric-queries") ||
    routePath.startsWith("/semantic-relationships")
  ) {
    return ["semantic"];
  }
  if (routePath.startsWith("/tags") || routePath.startsWith("/tag-assignments") || routePath.startsWith("/classifications")) {
    return ["governance"];
  }
  if (
    routePath.startsWith("/principals") ||
    routePath.startsWith("/groups") ||
    routePath.startsWith("/grants") ||
    routePath.startsWith("/api-keys") ||
    routePath.startsWith("/tables/") ||
    routePath.startsWith("/row-filters") ||
    routePath.startsWith("/column-masks")
  ) {
    return ["security"];
  }
  if (routePath === "/query" || routePath.startsWith("/queries")) {
    return ["query"];
  }
  return ["api"];
}

function buildResponses(
  method: string,
  routePath: string,
  operationName: string,
  responseSchema: IRSchemaRef | undefined,
): IREndpoint["responses"] {
  const responses: IREndpoint["responses"] = [];
  const successCode = successStatusCode(method, operationName, responseSchema !== undefined);
  const successDescription = successCode === 201 ? "Created" : successCode === 204 ? "No Content" : "OK";

  responses.push({
    status_code: successCode,
    description: successDescription,
    ...(successCode !== 204 && responseSchema ? { schema: responseSchema } : {}),
  });

  if (method === "get" && routePath.includes("{")) {
    responses.push({ status_code: 404, description: "Not Found", schema: { ref: "Error" } });
  }

  responses.push({ status_code: 401, description: "Unauthorized", schema: { ref: "Error" } });
  responses.push({ status_code: 429, description: "Too Many Requests", schema: { ref: "Error" } });
  responses.push({ status_code: 500, description: "Internal Server Error", schema: { ref: "Error" } });

  return responses;
}

function successStatusCode(method: string, operationName: string, hasResponseSchema: boolean): number {
  if (method === "delete") {
    return 204;
  }
  if (method === "put") {
    return 204;
  }
  if (method === "post") {
    if (operationName === "createGroupMember") {
      return 204;
    }
    if (!hasResponseSchema) {
      return 204;
    }
    return 201;
  }
  return 200;
}

function isNoContentReturn(type: Type): boolean {
  if (type.kind === "Void") {
    return true;
  }
  if (type.kind === "Intrinsic" && type.name.toLowerCase() === "void") {
    return true;
  }
  return false;
}

function toSchemaRef(type: Type, schemas: Record<string, IRSchema>): IRSchemaRef {
  switch (type.kind) {
    case "String":
      return { type: "string" };
    case "Number":
      return { type: "number" };
    case "Boolean":
      return { type: "boolean" };
    case "Scalar": {
      const scalarName = type.name.toLowerCase();
      if (scalarName.includes("int32")) {
        return { type: "integer", format: "int32" };
      }
      if (scalarName.includes("int64")) {
        return { type: "integer", format: "int64" };
      }
      if (scalarName.includes("int")) {
        return { type: "integer", format: "int32" };
      }
      if (scalarName.includes("float") || scalarName.includes("decimal") || scalarName.includes("number")) {
        return { type: "number" };
      }
      if (scalarName.includes("boolean")) {
        return { type: "boolean" };
      }
      return { type: "string" };
    }
    case "Model": {
      if (type.name === "Array" && type.indexer?.value) {
        return { type: "array" };
      }
      ensureModelSchema(type, schemas);
      return { ref: type.name };
    }
    case "Tuple": {
      return { type: "array" };
    }
    default:
      return { type: "string" };
  }
}

function ensureModelSchema(model: Model, schemas: Record<string, IRSchema>): void {
  if (!model.name || schemas[model.name]) {
    return;
  }

  const properties: Record<string, { description?: string; schema: IRSchemaRef }> = {};
  const required: string[] = [];

  for (const [name, prop] of model.properties) {
    const ref = toSchemaRef(prop.type, schemas);
    properties[name] = { schema: ref };
    if (!prop.optional) {
      required.push(name);
    }
  }

  schemas[model.name] = {
    type: "object",
    properties,
    required: required.length > 0 ? required : undefined,
  };
}

function humanizeOperationName(operationName: string): string {
  return operationName
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cliCommandForOperation(operationName: string): string {
  return legacyCLICommands[operationName] ?? "";
}

const legacyCLICommands: Record<string, string> = {
  bindColumnMask: "security column-masks bind",
  bindRowFilter: "security row-filters bind",
  cancelModelRun: "models cancel cancel-model-run",
  cancelPipelineRun: "pipelines runs cancel",
  cancelQuery: "query cancel",
  checkMetricFreshness: "semantic freshness check-metric-freshness",
  checkModelFreshness: "models freshness check-model-freshness",
  checkSourceFreshness: "models freshness check-source-freshness",
  cleanupExpiredAPIKeys: "security api-keys cleanup",
  closeNotebookSession: "notebooks sessions close",
  commitTableIngestion: "ingestion commit",
  createAPIKey: "security api-keys create",
  createCell: "notebooks cells create",
  createColumnMask: "security column-masks create",
  createComputeAssignment: "compute assignments create",
  createComputeEndpoint: "compute endpoints create",
  createExternalLocation: "storage locations create",
  createGitRepo: "notebooks git-repos create",
  createGrant: "security grants create",
  createGroup: "security groups create",
  createGroupMember: "security members add",
  createMacro: "models macros create",
  createManifest: "manifest create",
  createModel: "models models create",
  createModelTest: "models tests create",
  createNotebook: "notebooks notebooks create",
  createNotebookSession: "notebooks sessions create",
  createPipeline: "pipelines pipelines create",
  createPipelineJob: "pipelines jobs create",
  createPrincipal: "security principals create",
  createRowFilter: "security row-filters create",
  createSchema: "catalog schemas create",
  createSemanticMetric: "semantic metrics create",
  createSemanticModel: "semantic semantic-models create",
  createSemanticPreAggregation: "semantic pre-aggregations create",
  createSemanticRelationship: "semantic semantic-relationships create",
  createStorageCredential: "storage credentials create",
  createTable: "catalog tables create",
  createTag: "governance tags create",
  createTagAssignment: "governance tag-assignments create",
  createUploadUrl: "ingestion upload-url",
  createView: "catalog views create",
  createVolume: "catalog volumes create",
  deleteAPIKey: "security api-keys delete",
  deleteCatalogRegistration: "catalog delete-registration",
  deleteCell: "notebooks cells delete",
  deleteColumnMask: "security column-masks delete",
  deleteComputeAssignment: "compute assignments delete",
  deleteComputeEndpoint: "compute endpoints delete",
  deleteExternalLocation: "storage locations delete",
  deleteGitRepo: "notebooks git-repos delete",
  deleteGrant: "security grants revoke",
  deleteGroup: "security groups delete",
  deleteGroupMember: "security members remove",
  deleteLineageEdge: "lineage edges delete",
  deleteMacro: "models macros delete",
  deleteModel: "models models delete",
  deleteModelTest: "models tests delete",
  deleteNotebook: "notebooks notebooks delete",
  deletePipeline: "pipelines pipelines delete",
  deletePipelineJob: "pipelines jobs delete",
  deletePrincipal: "security principals delete",
  deleteQuery: "query delete",
  deleteRowFilter: "security row-filters delete",
  deleteSchema: "catalog schemas delete",
  deleteSemanticMetric: "semantic metrics delete",
  deleteSemanticModel: "semantic semantic-models delete",
  deleteSemanticPreAggregation: "semantic pre-aggregations delete",
  deleteSemanticRelationship: "semantic semantic-relationships delete",
  deleteStorageCredential: "storage credentials delete",
  deleteTable: "catalog tables delete",
  deleteTag: "governance tags delete",
  deleteTagAssignment: "governance tag-assignments delete",
  deleteView: "catalog views delete",
  deleteVolume: "catalog volumes delete",
  diffMacroRevisions: "models diff diff-macro-revisions",
  executeCell: "notebooks cells execute",
  executeQuery: "query",
  explainMetricQuery: "semantic explain",
  getCatalog: "catalog get",
  getCatalogRegistration: "catalog get-registration",
  getColumnImpact: "lineage impact get",
  getColumnLineage: "lineage columns get",
  getComputeEndpoint: "compute endpoints get",
  getComputeEndpointHealth: "compute endpoints health",
  getDownstreamLineage: "lineage tables downstream",
  getExternalLocation: "storage locations get",
  getGitRepo: "notebooks git-repos get",
  getGroup: "security groups get",
  getMacro: "models macros get",
  getMacroImpact: "models impact get",
  getMetastoreSummary: "observability metastore summary",
  getModel: "models models get",
  getModelDAG: "models dag get",
  getModelRun: "models model-runs get",
  getNotebook: "notebooks notebooks get",
  getNotebookJob: "notebooks jobs get",
  getPipeline: "pipelines pipelines get",
  getPipelineRun: "pipelines runs get",
  getPrincipal: "security principals get",
  getQuery: "query status",
  getQueryResults: "query results",
  getSchema: "catalog schemas get",
  getSemanticModel: "semantic semantic-models get",
  getStorageCredential: "storage credentials get",
  getTable: "catalog tables get",
  getTableLineage: "lineage tables get",
  getUpstreamLineage: "lineage tables upstream",
  getView: "catalog views get",
  getVolume: "catalog volumes get",
  listAPIKeys: "security api-keys list",
  listAuditLogs: "observability audit-logs list",
  listCatalogs: "catalog list-registrations",
  listClassifications: "governance classifications list",
  listColumnMasks: "security column-masks list",
  listComputeAssignments: "compute assignments list",
  listComputeEndpoints: "compute endpoints list",
  listExternalLocations: "storage locations list",
  listGitRepos: "notebooks git-repos list",
  listGrants: "security grants list",
  listGroupMembers: "security members list",
  listGroups: "security groups list",
  listMacroRevisions: "models revisions list",
  listMacros: "models macros list",
  listModelRunSteps: "models steps list",
  listModelRuns: "models model-runs list",
  listModelTestResults: "models test-results list",
  listModelTests: "models tests list",
  listModels: "models models list",
  listNotebookJobs: "notebooks jobs list",
  listNotebooks: "notebooks notebooks list",
  listPipelineJobRuns: "pipelines runs list-job-runs",
  listPipelineJobs: "pipelines jobs list",
  listPipelineRuns: "pipelines runs list",
  listPipelines: "pipelines pipelines list",
  listPrincipals: "security principals list",
  listQueryHistory: "observability query-history list",
  listRowFilters: "security row-filters list",
  listSchemas: "catalog schemas list",
  listSemanticMetrics: "semantic metrics list",
  listSemanticModels: "semantic semantic-models list",
  listSemanticPreAggregations: "semantic pre-aggregations list",
  listSemanticRelationships: "semantic semantic-relationships list",
  listStorageCredentials: "storage credentials list",
  listTableColumns: "catalog columns list",
  listTables: "catalog tables list",
  listTags: "governance tags list",
  listViews: "catalog views list",
  listVolumes: "catalog volumes list",
  loadTableExternalFiles: "ingestion load",
  profileTable: "catalog tables profile",
  promoteNotebookToModel: "models from-notebook promote-notebook-to-model",
  purgeLineage: "lineage purge",
  registerCatalog: "catalog register",
  reorderCells: "notebooks cells reorder",
  runAllCells: "notebooks sessions run-all",
  runAllCellsAsync: "notebooks sessions run-all-async",
  runMetricQuery: "semantic run",
  searchCatalog: "governance search",
  setDefaultCatalog: "catalog set-default",
  submitQuery: "query submit",
  syncGitRepo: "notebooks git-repos sync",
  triggerModelRun: "models model-runs trigger-model-run",
  triggerPipelineRun: "pipelines runs trigger",
  unbindColumnMask: "security column-masks unbind",
  unbindRowFilter: "security row-filters unbind",
  updateCatalogRegistration: "catalog update-registration",
  updateCell: "notebooks cells update",
  updateColumn: "catalog columns update",
  updateComputeEndpoint: "compute endpoints update",
  updateExternalLocation: "storage locations update",
  updateMacro: "models macros update",
  updateModel: "models models update",
  updateNotebook: "notebooks notebooks update",
  updatePipeline: "pipelines pipelines update",
  updatePrincipalAdmin: "security principals set-admin",
  updateSchema: "catalog schemas update",
  updateSemanticMetric: "semantic metrics update",
  updateSemanticModel: "semantic semantic-models update",
  updateSemanticPreAggregation: "semantic pre-aggregations update",
  updateSemanticRelationship: "semantic semantic-relationships update",
  updateStorageCredential: "storage credentials update",
  updateTable: "catalog tables update",
  updateView: "catalog views update",
  updateVolume: "catalog volumes update",
};

function cleanText(value: string | undefined): string {
  return (value ?? "").trim();
}
