import { getDoc, resolvePath } from "@typespec/compiler";
import { getExtensions } from "@typespec/openapi";
import { getHttpOperation, getOperationVerb, getRoutePath } from "@typespec/http";
export async function $onEmit(context) {
    const outputFile = context.options["output-file"] ?? "json-ir.json";
    const outputPath = resolvePath(context.emitterOutputDir, outputFile);
    const schemas = {
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
    const ir = {
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
function collectEndpoints(context, schemas) {
    const endpoints = [];
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
function visitNamespace(context, namespace, schemas, endpoints) {
    for (const operation of namespace.operations.values()) {
        appendOperation(context, operation, schemas, endpoints);
    }
    for (const childNamespace of namespace.namespaces.values()) {
        visitNamespace(context, childNamespace, schemas, endpoints);
    }
}
function appendOperation(context, operation, schemas, endpoints) {
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
    if (operationExtensions["x-apigen-manual"] === true || isManuallyMountedOperation(operation.name)) {
        return;
    }
    const authzMetadata = authzMetadataForOperation(operationExtensions, operation.name);
    const isAuthenticated = authzMetadata.mode !== "public";
    const operationDoc = cleanText(getDoc(context.program, operation));
    const cliCommand = cliCommandForOperation(operation.name);
    const endpoint = {
        method: verb.toLowerCase(),
        path: routePath,
        operation_id: operation.name,
        summary: humanizeOperationName(operation.name),
        ...(operationDoc ? { description: operationDoc } : {}),
        tags: tagsForRoute(routePath),
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
    const extensions = {};
    for (const [key, value] of Object.entries(operationExtensions)) {
        if (key === "x-apigen-manual") {
            continue;
        }
        extensions[key] = value;
    }
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
function getIRCompatibleExtensions(extensions) {
    const output = {};
    for (const [key, value] of extensions) {
        if (!key.startsWith("x-")) {
            continue;
        }
        output[key] = value;
    }
    return output;
}
function isManuallyMountedOperation(operationName) {
    switch (operationName) {
        case "bootstrapComplete":
        case "localLogin":
        case "createBootstrapToken":
        case "getOIDCProvider":
        case "upsertOIDCProvider":
        case "revokeAllWebSessions":
        case "getWebSessionStats":
            return true;
        default:
            return false;
    }
}
function getBodySchema(context, operation, schemas) {
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
function getParameters(context, operation, routePath, schemas) {
    const parameters = [];
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
function httpMethodRank(method) {
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
function operationAuthz(operationName) {
    if (operationName === "getHealth") {
        return { mode: "public" };
    }
    if (operationName === "createGrant" || operationName === "deleteGrant") {
        return { mode: "admin_only" };
    }
    const privilegeChecks = {
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
function authzMetadataForOperation(operationExtensions, operationName) {
    const authzValue = operationExtensions["x-authz"];
    if (isAuthzMetadata(authzValue)) {
        return authzValue;
    }
    return operationAuthz(operationName);
}
function isAuthzMetadata(value) {
    if (!value || typeof value !== "object") {
        return false;
    }
    const candidate = value;
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
function tagsForRoute(routePath) {
    if (routePath === "/healthz") {
        return ["system"];
    }
    if (routePath === "/manifest") {
        return ["manifest"];
    }
    if (routePath === "/audit-logs" ||
        routePath === "/query-history" ||
        routePath.includes("/metastore/summary")) {
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
    if (routePath.startsWith("/semantic-models") ||
        routePath.startsWith("/metrics") ||
        routePath.startsWith("/metric-queries") ||
        routePath.startsWith("/semantic-relationships")) {
        return ["semantic"];
    }
    if (routePath.startsWith("/tags") || routePath.startsWith("/tag-assignments") || routePath.startsWith("/classifications")) {
        return ["governance"];
    }
    if (routePath.startsWith("/principals") ||
        routePath.startsWith("/groups") ||
        routePath.startsWith("/grants") ||
        routePath.startsWith("/api-keys") ||
        routePath.startsWith("/tables/") ||
        routePath.startsWith("/row-filters") ||
        routePath.startsWith("/column-masks")) {
        return ["security"];
    }
    if (routePath === "/query" || routePath.startsWith("/queries")) {
        return ["query"];
    }
    return ["api"];
}
function buildResponses(context, httpOperation, operationName, schemas) {
    const responses = [];
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
function selectResponseContent(contents) {
    return contents.find((content) => content.body || (content.headers && Object.keys(content.headers).length > 0)) ?? contents[0];
}
function defaultResponseDescription(statusCode) {
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
function responseShapeForOperation(operationName, statusCode) {
    return (responseShapeManifest[`${operationName}:${statusCode}`] ??
        Object.entries(responseShapeManifest).find(([key]) => key.startsWith(`${operationName}:`))?.[1]);
}
const responseShapeManifest = {
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
    "createSemanticRelationship:201": { kind: "wrapped_json", body_type: "SemanticRelationship" },
    "checkMetricFreshness:200": { kind: "wrapped_json", body_type: "MetricFreshnessStatus" },
    "diffMacroRevisions:200": { kind: "wrapped_json", body_type: "MacroRevisionDiff" },
    "explainMetricQuery:201": { kind: "wrapped_json", body_type: "MetricQueryExplainResponse" },
    "executeCell:201": { kind: "wrapped_json", body_type: "CellExecutionResult" },
    "executeQuery:201": { kind: "wrapped_json", body_type: "QueryResult" },
    "getCatalog:200": { kind: "wrapped_json", body_type: "CatalogRegistration" },
    "getCatalogRegistration:200": { kind: "wrapped_json", body_type: "CatalogRegistration" },
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
    "listSemanticRelationships:200": { kind: "wrapped_json", body_type: "PaginatedSemanticRelationships" },
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
    "updateSemanticRelationship:200": { kind: "wrapped_json", body_type: "SemanticRelationship" },
    "updateStorageCredential:200": { kind: "wrapped_json", body_type: "StorageCredential" },
    "updateTable:200": { kind: "wrapped_json", body_type: "TableDetail" },
    "updateView:200": { kind: "wrapped_json", body_type: "ViewDetail" },
    "updateVolume:200": { kind: "wrapped_json", body_type: "VolumeDetail" },
};
function isNoContentReturn(type) {
    if (type.kind === "Intrinsic" && type.name.toLowerCase() === "void") {
        return true;
    }
    return false;
}
function toSchemaRef(type, schemas) {
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
function ensureEnumSchema(enumType, schemas) {
    if (!enumType.name || schemas[enumType.name]) {
        return;
    }
    const values = [];
    for (const member of enumType.members.values()) {
        values.push(String(member.value ?? member.name));
    }
    schemas[enumType.name] = {
        type: "string",
        enum: values,
    };
}
function ensureModelSchema(model, schemas) {
    if (!model.name || schemas[model.name]) {
        return;
    }
    const properties = {};
    const required = [];
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
function humanizeOperationName(operationName) {
    return operationName
        .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
        .replace(/_/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}
function cliCommandForOperation(operationName) {
    return legacyCLICommands[operationName] ?? "";
}
const legacyCLICommands = {
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
    createAsset: "assets create",
    createAssetBackfill: "assets backfills create",
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
    deleteAsset: "assets delete",
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
    getAsset: "assets get",
    getAssetBackfill: "assets backfills get",
    getAssetGraph: "assets graph get",
    getCatalog: "catalog info",
    getCatalogRegistration: "catalog get",
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
    listAssetBackfills: "assets backfills list",
    listAssetCheckResults: "assets check-results list",
    listAssetChecks: "assets checks list",
    listAssetMaterializations: "assets materializations list",
    listAssetPartitions: "assets partitions list",
    listAssetRuns: "assets runs list",
    listAssets: "assets list",
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
    triggerAssetMaterialization: "assets materialize",
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
    updateAsset: "assets update",
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
function cleanText(value) {
    return (value ?? "").trim();
}
