import "@typespec/http";
import "@typespec/openapi";
import "@typespec/openapi3";
import "./models/core.tsp";
import "./models/assets.tsp";
import "./models/ingestion.tsp";
import "./operations/core.tsp";
import "./operations/assets.tsp";
import "./operations/auth.tsp";
import "./operations/extensions.tsp";
import "./extensions/openapi.tsp";

using TypeSpec.Http;
using TypeSpec.OpenAPI;

@service(#{ title: "Duck Data Platform API" })
@doc("Secure SQL query layer over DuckDB with RBAC, row-level security, and column masking, backed by SQLite metadata.")
@server("https://api.example.com/v1", "Primary API server")
@tag("Duck")
@useAuth(BearerAuth | ApiKeyAuth<ApiKeyLocation.header, "X-API-Key">)
namespace Duck;
