package api

import "list"

endpoints: list.Concat([
  endpoints_assets,
  endpoints_audit,
  endpoints_catalogs,
  endpoints_compute,
  endpoints_dashboards,
  endpoints_folders,
  endpoints_generated,
  endpoints_governance,
  endpoints_health,
  endpoints_identity,
  endpoints_integrations,
  endpoints_lineage,
  endpoints_models,
  endpoints_notebooks,
  endpoints_pipelines,
  endpoints_products,
  endpoints_projects,
  endpoints_queries,
  endpoints_resources,
  endpoints_semantic,
  endpoints_storage,
  endpoints_workspaces,
])
