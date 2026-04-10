import type { EmitContext, Enum, Model, ModelProperty, Namespace, Operation, Type } from "@typespec/compiler";
import { getAllTags, getDoc, getService, getSummary, listServices, resolvePath } from "@typespec/compiler";
import { getExtensions, getTagsMetadata, resolveInfo } from "@typespec/openapi";
import { getHttpOperation, getOperationVerb, getRoutePath, getServers } from "@typespec/http";

type EmitterOptions = {
  "output-file"?: string;
};

type IRSchemaRef = {
  ref?: string;
  type?: string;
  format?: string;
  items?: IRSchemaRef;
};

type IRSchema = {
  type: string;
  enum?: string[];
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
    headers?: Array<{
      name: string;
      required?: boolean;
      description?: string;
      schema: IRSchemaRef;
    }>; 
    schema?: IRSchemaRef;
    extensions?: Record<string, unknown>;
  }>;
  extensions?: Record<string, unknown>;
};

type IRResponseShape = {
  kind: "wrapped_json";
  body_type: string;
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

  const serviceNamespace = resolveServiceNamespace(context);
  const schemas: Record<string, IRSchema> = {};
  const endpoints = collectEndpoints(context, schemas);
  const ir: IRDocument = {
    schema_version: "v1",
    info: resolveServiceInfo(context, serviceNamespace),
    servers: resolveServiceServers(context, serviceNamespace),
    tags: resolveServiceTags(context, serviceNamespace, endpoints),
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

  const [httpOperation] = getHttpOperation(context.program, operation);
  const bodySchema = getBodySchema(context, operation, schemas);
  const parameters = getParameters(context, operation, routePath, schemas);
  const responses = buildResponses(context, httpOperation, operation.name, schemas);
  const operationExtensions = getIRCompatibleExtensions(getExtensions(context.program, operation));
  if (operationExtensions["x-apigen-manual"] === true) {
    return;
  }

  const authzMetadata = authzMetadataForOperation(operationExtensions);
  const isAuthenticated = authzMetadata.mode !== "public";
  const operationDoc = cleanText(getDoc(context.program, operation));
  const operationSummary = cleanText(getSummary(context.program, operation)) || humanizeOperationName(operation.name);
  const operationTags = getAllTags(context.program, operation) ?? [];

  const endpoint: IREndpoint = {
    method: verb.toLowerCase(),
    path: routePath,
    operation_id: operation.name,
    summary: operationSummary,
    ...(operationDoc ? { description: operationDoc } : {}),
    tags: operationTags,
    responses,
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
  for (const [key, value] of Object.entries(operationExtensions)) {
    if (key === "x-apigen-manual") {
      continue;
    }
    extensions[key] = value;
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

function resolveServiceNamespace(context: EmitContext<EmitterOptions>): Namespace {
  const services = [...listServices(context.program)];
  if (services.length === 0) {
    return context.program.getGlobalNamespaceType();
  }

  services.sort((left, right) => (left.type.name ?? "").localeCompare(right.type.name ?? ""));
  return services[0].type;
}

function resolveServiceInfo(
  context: EmitContext<EmitterOptions>,
  serviceNamespace: Namespace,
): IRDocument["info"] {
  const service = getService(context.program, serviceNamespace);
  const info = resolveInfo(context.program, serviceNamespace);

  return {
    title: cleanText(info?.title ?? service?.title) || "API",
    version: cleanText(info?.version) || "0.1.0",
    description: cleanText(info?.description ?? getDoc(context.program, serviceNamespace)),
  };
}

function resolveServiceServers(
  context: EmitContext<EmitterOptions>,
  serviceNamespace: Namespace,
): IRDocument["servers"] {
  const servers = getServers(context.program, serviceNamespace) ?? [];
  return servers.map((server) => ({
    url: server.url,
    description: cleanText(server.description),
  }));
}

function resolveServiceTags(
  context: EmitContext<EmitterOptions>,
  serviceNamespace: Namespace,
  endpoints: IREndpoint[],
): IRDocument["tags"] {
  const tagsMetadata = getTagsMetadata(context.program, serviceNamespace) ?? {};
  const tagNames = new Set<string>();

  for (const endpoint of endpoints) {
    for (const tag of endpoint.tags) {
      if (tag !== "") {
        tagNames.add(tag);
      }
    }
  }

  for (const tagName of Object.keys(tagsMetadata)) {
    if (tagName !== "") {
      tagNames.add(tagName);
    }
  }

  return [...tagNames]
    .sort((left, right) => left.localeCompare(right))
    .map((name) => ({
      name,
      description: cleanText(tagsMetadata[name]?.description),
    }));
}

function getIRCompatibleExtensions(extensions: ReadonlyMap<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of extensions) {
    if (!key.startsWith("x-")) {
      continue;
    }
    output[key] = value;
  }
  return output;
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

function authzMetadataForOperation(
  operationExtensions: Record<string, unknown>,
): AuthzMetadata {
  const authzValue = operationExtensions["x-authz"];
  if (isAuthzMetadata(authzValue)) {
    return authzValue;
  }
  return { mode: "authenticated" };
}

function isAuthzMetadata(value: unknown): value is AuthzMetadata {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Record<string, unknown>;
  switch (candidate.mode) {
    case "public":
    case "authenticated":
    case "admin_only":
      return true;
    case "privilege":
      return Array.isArray(candidate.checks);
    default:
      return false;
  }
}

function buildResponses(
  context: EmitContext<EmitterOptions>,
  httpOperation: ReturnType<typeof getHttpOperation>[0],
  operationName: string,
  schemas: Record<string, IRSchema>,
): IREndpoint["responses"] {
  const responses: IREndpoint["responses"] = [];

  for (const response of httpOperation.responses) {
    if (typeof response.statusCodes !== "number") {
      continue;
    }

    const responseContent = selectResponseContent(response.responses);
    const responseShape = responseShapeForOperation(operationName, response.statusCodes);
    const headers = responseContent?.headers
      ? Object.entries(responseContent.headers)
          .map(([name, prop]) => ({
            name,
            required: !prop.optional,
            description: cleanText(getDoc(context.program, prop)),
            schema: toSchemaRef(prop.type, schemas),
          }))
          .sort((a, b) => a.name.localeCompare(b.name))
      : undefined;
    const bodyType = responseContent?.body?.type;

    responses.push({
      status_code: response.statusCodes,
      description: cleanText(response.description) || defaultResponseDescription(response.statusCodes),
      ...(headers && headers.length > 0 ? { headers } : {}),
      ...(bodyType && !isNoContentReturn(bodyType) ? { schema: toSchemaRef(bodyType, schemas) } : {}),
      ...(responseShape ? { extensions: { "x-apigen-response-shape": responseShape } } : {}),
    });
  }

  responses.sort((a, b) => a.status_code - b.status_code);
  return responses;
}

function selectResponseContent(
  contents: Array<{ headers?: Record<string, ModelProperty>; body?: { type: Type } }>,
): { headers?: Record<string, ModelProperty>; body?: { type: Type } } | undefined {
  return contents.find((content) => content.body || (content.headers && Object.keys(content.headers).length > 0)) ?? contents[0];
}

function defaultResponseDescription(statusCode: number): string {
  switch (statusCode) {
    case 200:
      return "OK";
    case 201:
      return "Created";
    case 204:
      return "No Content";
    case 400:
      return "Bad Request";
    case 401:
      return "Unauthorized";
    case 403:
      return "Forbidden";
    case 404:
      return "Not Found";
    case 409:
      return "Conflict";
    case 429:
      return "Too Many Requests";
    case 500:
      return "Internal Server Error";
    case 503:
      return "Service Unavailable";
    default:
      return `HTTP ${statusCode}`;
  }
}

function responseShapeForOperation(operationName: string, statusCode: number): IRResponseShape | undefined {
  return (
    responseShapeManifest[`${operationName}:${statusCode}`] ??
    Object.entries(responseShapeManifest).find(([key]) => key.startsWith(`${operationName}:`))?.[1]
  );
}

const responseShapeManifest: Record<string, IRResponseShape> = {
  "cancelModelRun:201": { kind: "wrapped_json", body_type: "ModelRun" },
  "cancelPipelineRun:201": { kind: "wrapped_json", body_type: "PipelineRun" },
  "cancelQuery:201": { kind: "wrapped_json", body_type: "CancelQueryResponse" },
  "checkModelFreshness:200": { kind: "wrapped_json", body_type: "FreshnessStatus" },
  "checkSourceFreshness:200": { kind: "wrapped_json", body_type: "SourceFreshnessStatus" },
  "cleanupExpiredAPIKeys:201": { kind: "wrapped_json", body_type: "CleanupAPIKeysResponse" },
  "commitTableIngestion:201": { kind: "wrapped_json", body_type: "IngestionResult" },
  "createAPIKey:201": { kind: "wrapped_json", body_type: "CreateAPIKeyResponse" },
  "createCell:201": { kind: "wrapped_json", body_type: "Cell" },
  "createColumnMask:201": { kind: "wrapped_json", body_type: "ColumnMask" },
  "createComputeAssignment:201": { kind: "wrapped_json", body_type: "ComputeAssignment" },
  "createComputeEndpoint:201": { kind: "wrapped_json", body_type: "ComputeEndpoint" },
  "createExternalLocation:201": { kind: "wrapped_json", body_type: "ExternalLocation" },
  "createGitRepo:201": { kind: "wrapped_json", body_type: "GitRepo" },
  "createGrant:201": { kind: "wrapped_json", body_type: "PrivilegeGrant" },
  "createGroup:201": { kind: "wrapped_json", body_type: "Group" },
  "createMacro:201": { kind: "wrapped_json", body_type: "Macro" },
  "createManifest:201": { kind: "wrapped_json", body_type: "ManifestResponse" },
  "createModel:201": { kind: "wrapped_json", body_type: "Model" },
  "createModelTest:201": { kind: "wrapped_json", body_type: "ModelTest" },
  "createNotebook:201": { kind: "wrapped_json", body_type: "Notebook" },
  "createNotebookSession:201": { kind: "wrapped_json", body_type: "NotebookSession" },
  "createPipeline:201": { kind: "wrapped_json", body_type: "Pipeline" },
  "createPipelineJob:201": { kind: "wrapped_json", body_type: "PipelineJob" },
  "createPrincipal:201": { kind: "wrapped_json", body_type: "Principal" },
  "createRowFilter:201": { kind: "wrapped_json", body_type: "RowFilter" },
  "createSchema:201": { kind: "wrapped_json", body_type: "SchemaDetail" },
  "createStorageCredential:201": { kind: "wrapped_json", body_type: "StorageCredential" },
  "createTable:201": { kind: "wrapped_json", body_type: "TableDetail" },
  "createTag:201": { kind: "wrapped_json", body_type: "Tag" },
  "createTagAssignment:201": { kind: "wrapped_json", body_type: "TagAssignment" },
  "createUploadUrl:201": { kind: "wrapped_json", body_type: "UploadUrlResponse" },
  "createView:201": { kind: "wrapped_json", body_type: "ViewDetail" },
  "createVolume:201": { kind: "wrapped_json", body_type: "VolumeDetail" },
  "createSemanticMetric:201": { kind: "wrapped_json", body_type: "SemanticMetric" },
  "createSemanticModel:201": { kind: "wrapped_json", body_type: "SemanticModel" },
  "createSemanticPreAggregation:201": { kind: "wrapped_json", body_type: "SemanticPreAggregation" },
  "createSemanticModelRelationship:201": { kind: "wrapped_json", body_type: "SemanticRelationship" },
  "checkMetricFreshness:200": { kind: "wrapped_json", body_type: "MetricFreshnessStatus" },
  "diffMacroRevisions:200": { kind: "wrapped_json", body_type: "MacroRevisionDiff" },
  "explainMetricQuery:201": { kind: "wrapped_json", body_type: "MetricQueryExplainResponse" },
  "executeCell:201": { kind: "wrapped_json", body_type: "CellExecutionResult" },
  "executeQuery:201": { kind: "wrapped_json", body_type: "QueryResult" },
  "getCatalog:200": { kind: "wrapped_json", body_type: "CatalogRegistration" },
  "getColumnImpact:200": { kind: "wrapped_json", body_type: "PaginatedColumnLineageEdges" },
  "getColumnLineage:200": { kind: "wrapped_json", body_type: "PaginatedColumnLineageEdges" },
  "getComputeEndpoint:200": { kind: "wrapped_json", body_type: "ComputeEndpoint" },
  "getComputeEndpointHealth:200": { kind: "wrapped_json", body_type: "ComputeEndpointHealth" },
  "getDownstreamLineage:200": { kind: "wrapped_json", body_type: "PaginatedLineageEdges" },
  "getExternalLocation:200": { kind: "wrapped_json", body_type: "ExternalLocation" },
  "getGitRepo:200": { kind: "wrapped_json", body_type: "GitRepo" },
  "getGroup:200": { kind: "wrapped_json", body_type: "Group" },
  "getMacro:200": { kind: "wrapped_json", body_type: "Macro" },
  "getMacroImpact:200": { kind: "wrapped_json", body_type: "MacroImpactList" },
  "getMetastoreSummary:200": { kind: "wrapped_json", body_type: "MetastoreSummary" },
  "getModel:200": { kind: "wrapped_json", body_type: "Model" },
  "getModelDAG:200": { kind: "wrapped_json", body_type: "ModelDAG" },
  "getModelRun:200": { kind: "wrapped_json", body_type: "ModelRun" },
  "getNotebook:200": { kind: "wrapped_json", body_type: "NotebookDetail" },
  "getNotebookJob:200": { kind: "wrapped_json", body_type: "NotebookJob" },
  "getPipeline:200": { kind: "wrapped_json", body_type: "Pipeline" },
  "getPipelineRun:200": { kind: "wrapped_json", body_type: "PipelineRun" },
  "getPrincipal:200": { kind: "wrapped_json", body_type: "Principal" },
  "getQuery:200": { kind: "wrapped_json", body_type: "QueryJob" },
  "getQueryResults:200": { kind: "wrapped_json", body_type: "QueryResult" },
  "getSchema:200": { kind: "wrapped_json", body_type: "SchemaDetail" },
  "getSemanticModel:200": { kind: "wrapped_json", body_type: "SemanticModel" },
  "getStorageCredential:200": { kind: "wrapped_json", body_type: "StorageCredential" },
  "getTable:200": { kind: "wrapped_json", body_type: "TableDetail" },
  "getTableLineage:200": { kind: "wrapped_json", body_type: "LineageNode" },
  "getUpstreamLineage:200": { kind: "wrapped_json", body_type: "PaginatedLineageEdges" },
  "getView:200": { kind: "wrapped_json", body_type: "ViewDetail" },
  "getVolume:200": { kind: "wrapped_json", body_type: "VolumeDetail" },
  "listAPIKeys:200": { kind: "wrapped_json", body_type: "PaginatedAPIKeys" },
  "listAuditLogs:200": { kind: "wrapped_json", body_type: "PaginatedAuditLogs" },
  "listCatalogs:200": { kind: "wrapped_json", body_type: "CatalogRegistrationList" },
  "listClassifications:200": { kind: "wrapped_json", body_type: "PaginatedTags" },
  "listComputeAssignments:200": { kind: "wrapped_json", body_type: "PaginatedComputeAssignments" },
  "listComputeEndpoints:200": { kind: "wrapped_json", body_type: "PaginatedComputeEndpoints" },
  "listExternalLocations:200": { kind: "wrapped_json", body_type: "PaginatedExternalLocations" },
  "listGitRepos:200": { kind: "wrapped_json", body_type: "PaginatedGitRepos" },
  "listGrants:200": { kind: "wrapped_json", body_type: "PaginatedGrants" },
  "listGroupMembers:200": { kind: "wrapped_json", body_type: "PaginatedGroupMembers" },
  "listGroups:200": { kind: "wrapped_json", body_type: "PaginatedGroups" },
  "listMacroRevisions:200": { kind: "wrapped_json", body_type: "MacroRevisionList" },
  "listMacros:200": { kind: "wrapped_json", body_type: "PaginatedMacros" },
  "listModelRunSteps:200": { kind: "wrapped_json", body_type: "ModelRunStepList" },
  "listModelRuns:200": { kind: "wrapped_json", body_type: "PaginatedModelRuns" },
  "listModelTestResults:200": { kind: "wrapped_json", body_type: "ModelTestResultList" },
  "listModelTests:200": { kind: "wrapped_json", body_type: "ModelTestList" },
  "listModels:200": { kind: "wrapped_json", body_type: "PaginatedModels" },
  "listNotebookJobs:200": { kind: "wrapped_json", body_type: "PaginatedNotebookJobs" },
  "listNotebooks:200": { kind: "wrapped_json", body_type: "PaginatedNotebooks" },
  "listPipelineJobRuns:200": { kind: "wrapped_json", body_type: "PipelineJobRunList" },
  "listPipelineJobs:200": { kind: "wrapped_json", body_type: "PipelineJobList" },
  "listPipelineRuns:200": { kind: "wrapped_json", body_type: "PaginatedPipelineRuns" },
  "listPipelines:200": { kind: "wrapped_json", body_type: "PaginatedPipelines" },
  "listPrincipals:200": { kind: "wrapped_json", body_type: "PaginatedPrincipals" },
  "listQueryHistory:200": { kind: "wrapped_json", body_type: "PaginatedQueryHistoryEntries" },
  "listRowFilters:200": { kind: "wrapped_json", body_type: "PaginatedRowFilters" },
  "listSchemas:200": { kind: "wrapped_json", body_type: "PaginatedSchemaDetails" },
  "listColumnMasks:200": { kind: "wrapped_json", body_type: "PaginatedColumnMasks" },
  "listSemanticMetrics:200": { kind: "wrapped_json", body_type: "SemanticMetricList" },
  "listSemanticModels:200": { kind: "wrapped_json", body_type: "PaginatedSemanticModels" },
  "listSemanticPreAggregations:200": { kind: "wrapped_json", body_type: "SemanticPreAggregationList" },
  "listSemanticModelRelationships:200": { kind: "wrapped_json", body_type: "SemanticRelationshipList" },
  "listStorageCredentials:200": { kind: "wrapped_json", body_type: "PaginatedStorageCredentials" },
  "listTableColumns:200": { kind: "wrapped_json", body_type: "PaginatedColumnDetails" },
  "listTables:200": { kind: "wrapped_json", body_type: "PaginatedTableDetails" },
  "listTags:200": { kind: "wrapped_json", body_type: "PaginatedTags" },
  "listViews:200": { kind: "wrapped_json", body_type: "PaginatedViewDetails" },
  "listVolumes:200": { kind: "wrapped_json", body_type: "PaginatedVolumes" },
  "loadTableExternalFiles:201": { kind: "wrapped_json", body_type: "IngestionResult" },
  "profileTable:201": { kind: "wrapped_json", body_type: "TableStatistics" },
  "promoteNotebookToModel:201": { kind: "wrapped_json", body_type: "Model" },
  "purgeLineage:201": { kind: "wrapped_json", body_type: "PurgeLineageResponse" },
  "registerCatalog:201": { kind: "wrapped_json", body_type: "CatalogRegistration" },
  "reorderCells:201": { kind: "wrapped_json", body_type: "CellList" },
  "runAllCells:201": { kind: "wrapped_json", body_type: "RunAllResult" },
  "runAllCellsAsync:201": { kind: "wrapped_json", body_type: "NotebookJob" },
  "runMetricQuery:201": { kind: "wrapped_json", body_type: "MetricQueryRunResponse" },
  "searchCatalog:200": { kind: "wrapped_json", body_type: "PaginatedSearchResults" },
  "setDefaultCatalog:201": { kind: "wrapped_json", body_type: "CatalogRegistration" },
  "submitQuery:201": { kind: "wrapped_json", body_type: "SubmitQueryResponse" },
  "syncGitRepo:201": { kind: "wrapped_json", body_type: "GitSyncResult" },
  "triggerModelRun:201": { kind: "wrapped_json", body_type: "ModelRun" },
  "triggerPipelineRun:201": { kind: "wrapped_json", body_type: "PipelineRun" },
  "updateCatalogRegistration:200": { kind: "wrapped_json", body_type: "CatalogRegistration" },
  "updateCell:200": { kind: "wrapped_json", body_type: "Cell" },
  "updateColumn:200": { kind: "wrapped_json", body_type: "ColumnDetail" },
  "updateComputeEndpoint:200": { kind: "wrapped_json", body_type: "ComputeEndpoint" },
  "updateExternalLocation:200": { kind: "wrapped_json", body_type: "ExternalLocation" },
  "updateMacro:200": { kind: "wrapped_json", body_type: "Macro" },
  "updateModel:200": { kind: "wrapped_json", body_type: "Model" },
  "updateNotebook:200": { kind: "wrapped_json", body_type: "Notebook" },
  "updatePipeline:200": { kind: "wrapped_json", body_type: "Pipeline" },
  "updateSchema:200": { kind: "wrapped_json", body_type: "SchemaDetail" },
  "updateSemanticMetric:200": { kind: "wrapped_json", body_type: "SemanticMetric" },
  "updateSemanticModel:200": { kind: "wrapped_json", body_type: "SemanticModel" },
  "updateSemanticPreAggregation:200": { kind: "wrapped_json", body_type: "SemanticPreAggregation" },
  "updateSemanticModelRelationship:200": { kind: "wrapped_json", body_type: "SemanticRelationship" },
  "updateStorageCredential:200": { kind: "wrapped_json", body_type: "StorageCredential" },
  "updateTable:200": { kind: "wrapped_json", body_type: "TableDetail" },
  "updateView:200": { kind: "wrapped_json", body_type: "ViewDetail" },
  "updateVolume:200": { kind: "wrapped_json", body_type: "VolumeDetail" },
};

function isNoContentReturn(type: Type): boolean {
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
    case "Enum":
      ensureEnumSchema(type, schemas);
      return { ref: type.name };
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
        return { type: "array", items: toSchemaRef(type.indexer.value, schemas) };
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

function ensureEnumSchema(enumType: Enum, schemas: Record<string, IRSchema>): void {
  if (!enumType.name || schemas[enumType.name]) {
    return;
  }

  const values: string[] = [];
  for (const member of enumType.members.values()) {
    values.push(String(member.value ?? member.name));
  }

  schemas[enumType.name] = {
    type: "string",
    enum: values,
  };
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

function cleanText(value: string | undefined): string {
  return (value ?? "").trim();
}
