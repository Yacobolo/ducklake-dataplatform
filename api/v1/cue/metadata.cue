package api

schema_version: "v1"

api: {
  "base_path": "/v1"
}

info: {
  "title": "QuackStack API",
  "version": "0.1.0",
  "description": "QuackStack exposes a secure SQL query layer over DuckDB together with metadata management, RBAC, row-level security, column masking, lineage, orchestration, and semantic modeling APIs backed by SQLite metadata."
}

servers: [
  {
    "url": "https://localhost:8443",
    "description": "HTTPS base URL for local and proxied deployments",
    "variables": {}
  }
]

tags: [
  {
    "name": "Assets",
    "description": "Data asset definitions, runs, checks, partitions, and materialization workflows."
  },
  {
    "name": "Audit",
    "description": "Audit entry inspection for authorization and query activity."
  },
  {
    "name": "Auth",
    "description": "Authentication bootstrap, login, OIDC configuration, and web session administration."
  },
  {
    "name": "Catalogs",
    "description": "Catalog registrations, runtime catalogs, search, schema objects, manifests, and ingestion management APIs."
  },
  {
    "name": "Compute",
    "description": "Compute endpoint lifecycle, assignments, and health checks."
  },
  {
    "name": "Dashboards",
    "description": "Dashboard and widget authoring plus rendered dashboard views."
  },
  {
    "name": "Folders",
    "description": "Folder lifecycle, sharing, and namespace browsing for authored assets."
  },
  {
    "name": "Governance",
    "description": "Privileges, tags, classifications, row filters, and column masking controls."
  },
  {
    "name": "Health",
    "description": "Operational readiness and service health endpoints."
  },
  {
    "name": "Identity",
    "description": "Principals, groups, and API key management for authenticated access."
  },
  {
    "name": "Integrations",
    "description": "Git repository and external integration lifecycle operations."
  },
  {
    "name": "Lineage",
    "description": "Table and column lineage inspection together with lineage maintenance operations."
  },
  {
    "name": "Models",
    "description": "Model, macro, and model run management for transformation workflows."
  },
  {
    "name": "Notebooks",
    "description": "Notebook authoring, shares, sessions, cells, and job execution endpoints."
  },
  {
    "name": "Pipelines",
    "description": "Pipeline definitions, jobs, runs, and orchestration controls."
  },
  {
    "name": "Products",
    "description": "Product-first control-plane APIs for domains, teams, product contracts, releases, discovery, and portfolio reporting."
  },
  {
    "name": "Projects",
    "description": "Project, environment, and build APIs for authoring execution contexts within a workspace."
  },
  {
    "name": "Queries",
    "description": "Synchronous and asynchronous SQL query execution and query history endpoints."
  },
  {
    "name": "Resources",
    "description": "Recent and saved resource APIs for personalized navigation and activity tracking."
  },
  {
    "name": "Semantic Layer",
    "description": "Semantic models, metrics, relationships, and metric query execution."
  },
  {
    "name": "Storage",
    "description": "Storage credentials and external location configuration for object storage access."
  },
  {
    "name": "Workspaces",
    "description": "Top-level authoring ownership boundaries, membership management, and workspace defaults."
  }
]

openapi: {
  version: "3.0.0"
  tag_order: [
    "Semantic Layer",
    "Models",
    "Pipelines",
    "Integrations",
    "Dashboards",
    "Notebooks",
    "Compute",
    "Storage",
    "Lineage",
    "Audit",
    "Resources",
    "Queries",
    "Products",
    "Assets",
    "Catalogs",
    "Governance",
    "Projects",
    "Folders",
    "Workspaces",
    "Identity",
    "Auth",
    "Health",
  ]
  security: [{
    BearerAuth: []
  }, {
    ApiKeyAuth: []
  }]
  security_schemes: {
    BearerAuth: {
      type:   "http"
      scheme: "Bearer"
    }
    ApiKeyAuth: {
      type: "apiKey"
      in:   "header"
      name: "X-API-Key"
    }
  }
}
