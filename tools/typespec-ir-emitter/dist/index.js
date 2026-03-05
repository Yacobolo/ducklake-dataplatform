import { getDoc, resolvePath } from "@typespec/compiler";
import { getOperationVerb, getRoutePath } from "@typespec/http";

export async function $onEmit(context) {
  const outputFile = context.options["output-file"] ?? "json-ir.json";
  const outputPath = resolvePath(context.emitterOutputDir, outputFile);

  const schemas = {
    Error: {
      type: "object",
      properties: {
        code: { description: "HTTP status code", schema: { type: "integer", format: "int32" } },
        message: { description: "Error message", schema: { type: "string" } }
      },
      required: ["code", "message"]
    }
  };

  const endpoints = collectEndpoints(context, schemas);
  const ir = {
    schema_version: "v1",
    info: {
      title: "Duck Data Platform API",
      version: "0.1.0",
      description: "TypeSpec-emitted JSON IR for Go code generation."
    },
    servers: [{ url: "https://localhost:8080", description: "Local development" }],
    tags: [
      { name: "system", description: "System endpoints" },
      { name: "query", description: "Query execution endpoints" },
      { name: "security", description: "Security and access-control endpoints" },
      { name: "catalogs", description: "Catalog and metadata endpoints" },
      { name: "manifest", description: "Manifest and data-access endpoints" },
      { name: "observability", description: "Audit and query observability endpoints" },
      { name: "api", description: "General API endpoints" }
    ],
    schemas,
    endpoints
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

  const responseSchema = isNoContentReturn(operation.returnType) ? undefined : toSchemaRef(operation.returnType, schemas);
  const bodySchema = getBodySchema(context, operation, schemas);
  const parameters = getParameters(context, operation, routePath, schemas);
  const authzMode = operationAuthzMode(operation.name);
  const isAuthenticated = authzMode !== "public";
  const operationDoc = cleanText(getDoc(context.program, operation));

  const endpoint = {
    method: verb.toLowerCase(),
    path: routePath,
    operation_id: operation.name,
    ...(operationDoc ? { description: operationDoc } : {}),
    tags: tagsForRoute(routePath),
    responses: buildResponses(verb.toLowerCase(), routePath, operation.name, responseSchema)
  };

  if (parameters.length > 0) {
    endpoint.parameters = parameters;
  }

  if (bodySchema) {
    endpoint.request_body = {
      required: bodySchema.required,
      description: bodySchema.description,
      schema: bodySchema.schema
    };
  }

  if (isAuthenticated) {
    endpoint.extensions = {
      security: [{ ApiKeyAuth: [] }, { BearerAuth: [] }],
      "x-authz": { mode: authzMode }
    };
  }

  endpoints.push(endpoint);
}

function getBodySchema(context, operation, schemas) {
  const bodyParam = operation.parameters.properties.get("body");
  if (!bodyParam) {
    return undefined;
  }
  return {
    schema: toSchemaRef(bodyParam.type, schemas),
    required: !bodyParam.optional,
    description: cleanText(getDoc(context.program, bodyParam)) || "Request payload"
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
      schema: toSchemaRef(prop.type, schemas)
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

function operationAuthzMode(operationName) {
  if (operationName === "getHealth") {
    return "public";
  }
  if (operationName === "createGrant" || operationName === "deleteGrant") {
    return "admin_only";
  }
  return "authenticated";
}

function tagsForRoute(routePath) {
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
  if (routePath.startsWith("/v1/query")) {
    return ["query"];
  }
  return ["api"];
}

function buildResponses(method, routePath, operationName, responseSchema) {
  const responses = [];
  const successCode = successStatusCode(method, operationName, responseSchema !== undefined);
  const successDescription = successCode === 201 ? "Created" : successCode === 204 ? "No Content" : "OK";

  responses.push({
    status_code: successCode,
    description: successDescription,
    ...(successCode !== 204 && responseSchema ? { schema: responseSchema } : {})
  });

  if (method === "get" && routePath.includes("{")) {
    responses.push({ status_code: 404, description: "Not Found", schema: { ref: "Error" } });
  }

  responses.push({ status_code: 401, description: "Unauthorized", schema: { ref: "Error" } });
  responses.push({ status_code: 429, description: "Too Many Requests", schema: { ref: "Error" } });
  responses.push({ status_code: 500, description: "Internal Server Error", schema: { ref: "Error" } });

  return responses;
}

function successStatusCode(method, operationName, hasResponseSchema) {
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
    if (operationName.startsWith("create") || operationName.startsWith("register")) {
      return 201;
    }
    return 200;
  }
  return 200;
}

function isNoContentReturn(type) {
  if (type.kind === "Void") {
    return true;
  }
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
    required: required.length > 0 ? required : undefined
  };
}

function cleanText(value) {
  return (value ?? "").trim();
}
