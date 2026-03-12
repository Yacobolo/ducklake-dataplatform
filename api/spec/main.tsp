import "@typespec/http";
import "@typespec/openapi";
import "@typespec/openapi3";
import "./models/core.tsp";
import "./models/assets.tsp";
import "./models/ingestion.tsp";
import "./models/products.tsp";
import "./operations/core.tsp";
import "./operations/assets.tsp";
import "./operations/auth.tsp";
import "./operations/extensions.tsp";
import "./operations/products.tsp";
import "./extensions/openapi.tsp";

using TypeSpec.Http;
using TypeSpec.OpenAPI;

@service(#{ title: "Duck Data Platform API" })
@doc("Duck Data Platform exposes a secure SQL query layer over DuckDB together with metadata management, RBAC, row-level security, column masking, lineage, orchestration, and semantic modeling APIs backed by SQLite metadata.")
@info(#{
  version: "0.1.0"
})
@server("https://localhost:8443/v1", "HTTPS base URL for local and proxied deployments")
@tagMetadata("Health", #{ description: "Operational readiness and service health endpoints." })
@tagMetadata("Auth", #{ description: "Authentication bootstrap, login, OIDC configuration, and web session administration." })
@tagMetadata("Identity", #{ description: "Principals, groups, and API key management for authenticated access." })
@tagMetadata("Governance", #{ description: "Privileges, tags, classifications, row filters, and column masking controls." })
@tagMetadata("Catalogs", #{ description: "Catalog, schema, table, view, volume, and ingestion management APIs." })
@tagMetadata("Assets", #{ description: "Data asset definitions, runs, checks, partitions, and materialization workflows." })
@tagMetadata("Products", #{ description: "Product-first control-plane APIs for domains, teams, product contracts, releases, discovery, and portfolio reporting." })
@tagMetadata("Queries", #{ description: "Synchronous and asynchronous SQL query execution, search, and query history endpoints." })
@tagMetadata("Lineage", #{ description: "Table and column lineage inspection together with lineage maintenance operations." })
@tagMetadata("Storage", #{ description: "Storage credentials and external location configuration for object storage access." })
@tagMetadata("Compute", #{ description: "Compute endpoint lifecycle, assignments, and health checks." })
@tagMetadata("Notebooks", #{ description: "Notebook authoring, sessions, cells, and job execution endpoints." })
@tagMetadata("Integrations", #{ description: "Git repository and external integration lifecycle operations." })
@tagMetadata("Pipelines", #{ description: "Pipeline definitions, jobs, runs, and orchestration controls." })
@tagMetadata("Models", #{ description: "Model, macro, and model run management for transformation workflows." })
@tagMetadata("Semantic Layer", #{ description: "Semantic models, metrics, relationships, and metric query execution." })
@useAuth(BearerAuth | ApiKeyAuth<ApiKeyLocation.header, "X-API-Key">)
namespace Duck;
