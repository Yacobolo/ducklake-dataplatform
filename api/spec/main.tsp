import "@typespec/http";
import "@typespec/openapi";
import "@typespec/openapi3";
import "./models/core.tsp";
import "./models/assets.tsp";
import "./models/ingestion.tsp";
import "./models/products.tsp";
import "./operations/assets.tsp";
import "./operations/auth.tsp";
import "./operations/audit.tsp";
import "./operations/catalogs.tsp";
import "./operations/compute.tsp";
import "./operations/dashboards.tsp";
import "./operations/governance.tsp";
import "./operations/health.tsp";
import "./operations/identity.tsp";
import "./operations/integrations.tsp";
import "./operations/lineage.tsp";
import "./operations/models.tsp";
import "./operations/notebooks.tsp";
import "./operations/pipelines.tsp";
import "./operations/products.tsp";
import "./operations/queries.tsp";
import "./operations/resources.tsp";
import "./operations/semantic.tsp";
import "./operations/storage.tsp";
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
@tagMetadata("Catalogs", #{ description: "Catalog registrations, runtime catalogs, search, schema objects, manifests, and ingestion management APIs." })
@tagMetadata("Assets", #{ description: "Data asset definitions, runs, checks, partitions, and materialization workflows." })
@tagMetadata("Products", #{ description: "Product-first control-plane APIs for domains, teams, product contracts, releases, discovery, and portfolio reporting." })
@tagMetadata("Queries", #{ description: "Synchronous and asynchronous SQL query execution and query history endpoints." })
@tagMetadata("Resources", #{ description: "Recent and saved resource APIs for personalized navigation and activity tracking." })
@tagMetadata("Audit", #{ description: "Audit entry inspection for authorization and query activity." })
@tagMetadata("Lineage", #{ description: "Table and column lineage inspection together with lineage maintenance operations." })
@tagMetadata("Storage", #{ description: "Storage credentials and external location configuration for object storage access." })
@tagMetadata("Compute", #{ description: "Compute endpoint lifecycle, assignments, and health checks." })
@tagMetadata("Notebooks", #{ description: "Notebook authoring, sessions, cells, and job execution endpoints." })
@tagMetadata("Dashboards", #{ description: "Dashboard and widget authoring plus rendered dashboard views." })
@tagMetadata("Integrations", #{ description: "Git repository and external integration lifecycle operations." })
@tagMetadata("Pipelines", #{ description: "Pipeline definitions, jobs, runs, and orchestration controls." })
@tagMetadata("Models", #{ description: "Model, macro, and model run management for transformation workflows." })
@tagMetadata("Semantic Layer", #{ description: "Semantic models, metrics, relationships, and metric query execution." })
@useAuth(BearerAuth | ApiKeyAuth<ApiKeyLocation.header, "X-API-Key">)
namespace Duck;
