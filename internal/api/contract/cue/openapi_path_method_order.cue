package api

openapi: {
  path_method_order: {
  "/api-key-cleanup-runs": [
    "post"
  ],
  "/api-keys": [
    "get",
    "post"
  ],
  "/api-keys/{api_key_id}": [
    "delete"
  ],
  "/assets": [
    "get",
    "post"
  ],
  "/assets/{asset_key}": [
    "get",
    "patch",
    "delete"
  ],
  "/assets/{asset_key}/backfills": [
    "get",
    "post"
  ],
  "/assets/{asset_key}/backfills/{backfill_id}": [
    "get"
  ],
  "/assets/{asset_key}/checks": [
    "get"
  ],
  "/assets/{asset_key}/checks/results": [
    "get"
  ],
  "/assets/{asset_key}/freshness": [
    "get"
  ],
  "/assets/{asset_key}/freshness-reconciliations": [
    "post"
  ],
  "/assets/{asset_key}/freshness/blockers": [
    "get"
  ],
  "/assets/{asset_key}/freshness/explanation": [
    "get"
  ],
  "/assets/{asset_key}/freshness/requirements": [
    "get"
  ],
  "/assets/{asset_key}/graph": [
    "get"
  ],
  "/assets/{asset_key}/materializations": [
    "post",
    "get"
  ],
  "/assets/{asset_key}/partitions": [
    "get"
  ],
  "/assets/{asset_key}/runs": [
    "get"
  ],
  "/audit-entries": [
    "get"
  ],
  "/auth/bootstrap/complete": [
    "post"
  ],
  "/auth/bootstrap/tokens": [
    "post"
  ],
  "/auth/local/login": [
    "post"
  ],
  "/auth/provider/oidc": [
    "get",
    "put"
  ],
  "/auth/sessions/revocations": [
    "post"
  ],
  "/auth/sessions/stats": [
    "get"
  ],
  "/catalogs": [
    "post",
    "get"
  ],
  "/catalogs/search": [
    "get"
  ],
  "/catalogs/{catalog_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/catalogs/{catalog_name}/default": [
    "put"
  ],
  "/catalogs/{catalog_name}/history": [
    "get"
  ],
  "/catalogs/{catalog_name}/metastore/summary": [
    "get"
  ],
  "/catalogs/{catalog_name}/schemas": [
    "get",
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables": [
    "get",
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns": [
    "get"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns/{column_name}": [
    "patch"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-commits": [
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/ingestion-loads": [
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/manifest": [
    "get"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/profiles": [
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/upload-urls": [
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/views": [
    "get",
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/views/{view_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/volumes": [
    "get",
    "post"
  ],
  "/catalogs/{catalog_name}/schemas/{schema_name}/volumes/{volume_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/catalogs/{catalog_name}/version-summary": [
    "get"
  ],
  "/classifications": [
    "get"
  ],
  "/column-masks": [
    "get",
    "post"
  ],
  "/column-masks/{column_mask_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/column-masks/{column_mask_id}/bindings": [
    "get",
    "post"
  ],
  "/column-masks/{column_mask_id}/bindings/{principal_type}/{principal_id}": [
    "delete"
  ],
  "/compute-endpoints": [
    "get",
    "post"
  ],
  "/compute-endpoints/{endpoint_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/compute-endpoints/{endpoint_name}/assignments": [
    "get",
    "post"
  ],
  "/compute-endpoints/{endpoint_name}/assignments/{assignment_id}": [
    "delete"
  ],
  "/compute-endpoints/{endpoint_name}/health": [
    "get"
  ],
  "/compute-routing-defaults": [
    "get",
    "patch"
  ],
  "/dashboards": [
    "get",
    "post"
  ],
  "/dashboards/{dashboard_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/dashboards/{dashboard_id}/rendered": [
    "get"
  ],
  "/dashboards/{dashboard_id}/widgets": [
    "get",
    "post"
  ],
  "/dashboards/{dashboard_id}/widgets/{widget_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/data-products": [
    "get",
    "post"
  ],
  "/data-products/portfolio": [
    "get"
  ],
  "/data-products/scorecards": [
    "get"
  ],
  "/data-products/{product_slug}": [
    "get",
    "patch",
    "delete"
  ],
  "/data-products/{product_slug}/dependencies": [
    "get",
    "post"
  ],
  "/data-products/{product_slug}/events": [
    "get"
  ],
  "/data-products/{product_slug}/outputs": [
    "get"
  ],
  "/data-products/{product_slug}/semantic-entrypoints": [
    "get"
  ],
  "/data-products/{product_slug}/status": [
    "get"
  ],
  "/data-products/{product_slug}/subscriptions": [
    "get",
    "post"
  ],
  "/data-products/{product_slug}/versions": [
    "get",
    "post"
  ],
  "/data-products/{product_slug}/versions/{version}": [
    "get",
    "delete"
  ],
  "/data-products/{product_slug}/versions/{version}/deprecations": [
    "post"
  ],
  "/data-products/{product_slug}/versions/{version}/publications": [
    "post"
  ],
  "/data-products/{product_slug}/versions/{version}/retirements": [
    "post"
  ],
  "/external-locations": [
    "get",
    "post"
  ],
  "/external-locations/{location_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/folders/contents": [
    "get"
  ],
  "/folders/search": [
    "get"
  ],
  "/folders/{folder_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/folders/{folder_id}/contents": [
    "get"
  ],
  "/folders/{folder_id}/moves": [
    "post"
  ],
  "/folders/{folder_id}/path": [
    "get"
  ],
  "/folders/{folder_id}/search": [
    "get"
  ],
  "/folders/{folder_id}/shares": [
    "get",
    "post"
  ],
  "/folders/{folder_id}/shares/{principal_name}": [
    "delete"
  ],
  "/git-repos": [
    "get",
    "post"
  ],
  "/git-repos/{git_repo_id}": [
    "get",
    "delete"
  ],
  "/git-repos/{git_repo_id}/sync-runs": [
    "post"
  ],
  "/grants": [
    "get",
    "post"
  ],
  "/grants/{grant_id}": [
    "delete"
  ],
  "/groups": [
    "get",
    "post"
  ],
  "/groups/{group_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/groups/{group_id}/members": [
    "get",
    "post"
  ],
  "/groups/{group_id}/members/{member_type}/{member_id}": [
    "delete"
  ],
  "/healthz": [
    "get"
  ],
  "/lineage/columns/{schema_name}/{table_name}": [
    "get"
  ],
  "/lineage/columns/{schema_name}/{table_name}/{column_name}/impacts": [
    "get"
  ],
  "/lineage/edges/{edge_id}": [
    "delete"
  ],
  "/lineage/purges": [
    "post"
  ],
  "/lineage/tables/{schema_name}/{table_name}": [
    "get"
  ],
  "/lineage/tables/{schema_name}/{table_name}/downstream": [
    "get"
  ],
  "/lineage/tables/{schema_name}/{table_name}/upstream": [
    "get"
  ],
  "/macros": [
    "get",
    "post"
  ],
  "/macros/{macro_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/macros/{macro_name}/impacts": [
    "get"
  ],
  "/macros/{macro_name}/revision-diffs": [
    "get"
  ],
  "/macros/{macro_name}/revisions": [
    "get"
  ],
  "/me/recent-resources": [
    "get"
  ],
  "/me/saved-resources": [
    "get",
    "post"
  ],
  "/me/saved-resources/{resource_type}/{resource_key}": [
    "delete"
  ],
  "/model-runs": [
    "post",
    "get"
  ],
  "/model-runs/{run_id}": [
    "get"
  ],
  "/model-runs/{run_id}/cancellations": [
    "post"
  ],
  "/model-runs/{run_id}/steps": [
    "get"
  ],
  "/model-runs/{run_id}/steps/{step_id}/test-results": [
    "get"
  ],
  "/models": [
    "get",
    "post"
  ],
  "/models/dag": [
    "get"
  ],
  "/models/{project_name}/{model_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/models/{project_name}/{model_name}/freshness": [
    "get"
  ],
  "/models/{project_name}/{model_name}/tests": [
    "post",
    "get"
  ],
  "/models/{project_name}/{model_name}/tests/{test_id}": [
    "delete"
  ],
  "/notebooks": [
    "get",
    "post"
  ],
  "/notebooks/{notebook_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/notebooks/{notebook_id}/cells": [
    "post"
  ],
  "/notebooks/{notebook_id}/cells/reorder": [
    "post"
  ],
  "/notebooks/{notebook_id}/cells/{cell_id}": [
    "patch",
    "delete"
  ],
  "/notebooks/{notebook_id}/copies": [
    "post"
  ],
  "/notebooks/{notebook_id}/jobs": [
    "get"
  ],
  "/notebooks/{notebook_id}/jobs/{job_id}": [
    "get"
  ],
  "/notebooks/{notebook_id}/model-promotions": [
    "post",
    "delete"
  ],
  "/notebooks/{notebook_id}/moves": [
    "post"
  ],
  "/notebooks/{notebook_id}/sessions": [
    "post"
  ],
  "/notebooks/{notebook_id}/sessions/{session_id}": [
    "delete"
  ],
  "/notebooks/{notebook_id}/sessions/{session_id}/cell-executions": [
    "post"
  ],
  "/notebooks/{notebook_id}/sessions/{session_id}/cell-executions/{cell_id}": [
    "post"
  ],
  "/notebooks/{notebook_id}/sessions/{session_id}/job-runs": [
    "post"
  ],
  "/notebooks/{notebook_id}/shares": [
    "get",
    "post"
  ],
  "/notebooks/{notebook_id}/shares/{principal_name}": [
    "delete"
  ],
  "/pipelines": [
    "get",
    "post"
  ],
  "/pipelines/runs/{run_id}": [
    "get"
  ],
  "/pipelines/runs/{run_id}/cancellations": [
    "post"
  ],
  "/pipelines/runs/{run_id}/jobs": [
    "get"
  ],
  "/pipelines/{pipeline_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/pipelines/{pipeline_name}/jobs": [
    "get",
    "post"
  ],
  "/pipelines/{pipeline_name}/jobs/{job_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/pipelines/{pipeline_name}/runs": [
    "post",
    "get"
  ],
  "/principals": [
    "get",
    "post"
  ],
  "/principals/{principal_id}": [
    "get",
    "delete",
    "patch"
  ],
  "/product-domains": [
    "get",
    "post"
  ],
  "/product-domains/{domain_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/product-domains/{domain_name}/teams": [
    "get",
    "post"
  ],
  "/product-domains/{domain_name}/teams/{team_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/projects/{project_id}": [
    "get"
  ],
  "/projects/{project_id}/builds": [
    "get",
    "post"
  ],
  "/projects/{project_id}/environments": [
    "get",
    "post"
  ],
  "/queries": [
    "post",
    "get"
  ],
  "/queries/history": [
    "get"
  ],
  "/queries/{query_id}": [
    "get",
    "delete"
  ],
  "/queries/{query_id}/cancellations": [
    "post"
  ],
  "/queries/{query_id}/results": [
    "get"
  ],
  "/query-executions": [
    "post"
  ],
  "/row-filters": [
    "get",
    "post"
  ],
  "/row-filters/{row_filter_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/row-filters/{row_filter_id}/bindings": [
    "get",
    "post"
  ],
  "/row-filters/{row_filter_id}/bindings/{principal_type}/{principal_id}": [
    "delete"
  ],
  "/semantic-metrics/{metric_name}/freshness": [
    "get"
  ],
  "/semantic-models": [
    "get",
    "post"
  ],
  "/semantic-models/{semantic_model_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/semantic-models/{semantic_model_id}/metrics": [
    "get",
    "post"
  ],
  "/semantic-models/{semantic_model_id}/metrics/{metric_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/semantic-models/{semantic_model_id}/pre-aggregations": [
    "get",
    "post"
  ],
  "/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/semantic-models/{semantic_model_id}/query-explanations": [
    "post"
  ],
  "/semantic-models/{semantic_model_id}/query-runs": [
    "post"
  ],
  "/semantic-models/{semantic_model_id}/relationships": [
    "get",
    "post"
  ],
  "/semantic-models/{semantic_model_id}/relationships/{relationship_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/semantic-sources/{source_schema}/{source_table}/freshness": [
    "get"
  ],
  "/storage-credentials": [
    "get",
    "post"
  ],
  "/storage-credentials/{credential_name}": [
    "get",
    "patch",
    "delete"
  ],
  "/tags": [
    "get",
    "post"
  ],
  "/tags/{tag_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/tags/{tag_id}/assignments": [
    "get",
    "post"
  ],
  "/tags/{tag_id}/assignments/{assignment_id}": [
    "delete"
  ],
  "/workspaces": [
    "get",
    "post"
  ],
  "/workspaces/{workspace_id}": [
    "get",
    "patch",
    "delete"
  ],
  "/workspaces/{workspace_id}/folders": [
    "get",
    "post"
  ],
  "/workspaces/{workspace_id}/members": [
    "get",
    "post"
  ],
  "/workspaces/{workspace_id}/members/{principal_name}": [
    "delete"
  ],
  "/workspaces/{workspace_id}/projects": [
    "get",
    "post"
  ]
}
}
