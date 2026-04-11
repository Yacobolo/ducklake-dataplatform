package api

// Authored core schemas.

schemas_core: {
  AuditDecisionStatus: #enumSchema & {
    #values: [
      "ALLOWED",
      "DENIED",
      "ERROR"
    ]
  },
  AuditEntry: #objectSchema & {
    #fields: {
      action: #stringProperty,
      created_at: #createdAtProperty,
      duration_ms: #int64Property,
      error_message: #stringProperty,
      id: #idProperty,
      original_sql: #stringProperty,
      principal_name: #principalNameProperty,
      rewritten_sql: #stringProperty,
      statement_type: #stringProperty,
      status: #refProperty & {#ref: "AuditDecisionStatus"},
      tables_accessed: #stringArrayProperty
    },
    #required: [
      "id"
    ]
  },
  CreateEnvironmentRequest: #objectSchema & {
    #fields: {
      compute_endpoint: #stringProperty,
      defer_to_environment: #stringProperty,
      description: #descriptionProperty,
      kind: #refProperty & {#ref: "EnvironmentKind"},
      name: #nameProperty,
      source_overrides: #refProperty & {#ref: "Record"},
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      variables: #refProperty & {#ref: "Record"}
    },
    #required: [
      "name",
      "target_catalog",
      "target_schema"
    ]
  },
  CreateFolderRequest: #objectSchema & {
    #fields: {
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      name: #nameProperty,
      parent_folder_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  CreateProjectRequest: #objectSchema & {
    #fields: {
      default_branch: #stringProperty,
      description: #descriptionProperty,
      kind: #refProperty & {#ref: "ProjectKind"},
      name: #nameProperty,
      product_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  Error: #objectSchema & {
    #fields: {
      code: #int32Property,
      details: #refProperty & {#ref: "Record"},
      message: #stringProperty
    },
    #required: [
      "code",
      "message"
    ]
  },
  GitSyncResult: #objectSchema & {
    #fields: {
      commit_sha: #stringProperty,
      notebooks_created: #int32Property,
      notebooks_deleted: #int32Property,
      notebooks_updated: #int32Property
    }
  },
  HealthResponse: #objectSchema & {
    #fields: {
      status: #statusProperty
    },
    #required: [
      "status"
    ]
  },
  LineageEdge: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      edge_type: #stringProperty,
      id: #idProperty,
      principal_name: #principalNameProperty,
      source_schema: #stringProperty,
      source_table: #stringProperty,
      target_schema: #stringProperty,
      target_table: #stringProperty
    }
  },
  LineageNode: #objectSchema & {
    #fields: {
      table_name: #stringProperty,
      upstream: #arrayRefProperty & {
        #ref: "LineageEdge"
      },
      downstream: #arrayRefProperty & {
        #ref: "LineageEdge"
      }
    }
  },
  MetastoreSummary: #objectSchema & {
    #fields: {
      catalog_name: #stringProperty,
      data_path: #stringProperty,
      metastore_type: #stringProperty,
      schema_count: #int32Property,
      storage_backend: #stringProperty,
      table_count: #int32Property
    },
    #required: [
      "catalog_name"
    ]
  },
  MetastoreType: #enumSchema & {
    #values: [
      "sqlite",
      "postgres"
    ]
  },
  MoveFolderRequest: #objectSchema & {
    #fields: {
      confirm_context_change: #boolProperty,
      confirm_leave_git: #boolProperty,
      parent_folder_id: #stringProperty
    }
  },
  PurgeLineageRequest: #objectSchema & {
    #fields: {
      older_than_days: #int32Property
    },
    #required: [
      "older_than_days"
    ]
  },
  PurgeLineageResponse: #objectSchema & {
    #fields: {
      deleted_count: #int64Property
    }
  },
  Record: #objectSchema,
  ResolvedDashboardWidget: #objectSchema & {
    #fields: {
      columns: #stringArrayProperty,
      generated_sql: #stringProperty,
      row_count: #int64Property,
      rows: {
        schema: {
          type: "array",
          items: {
            type: "array",
            items: {
              type: "string"
            }
          }
        }
      },
      widget: #refProperty & {#ref: "DashboardWidget"}
    },
    #required: [
      "columns"
    ]
  },
  RunAllResult: #objectSchema & {
    #fields: {
      notebook_id: #stringProperty,
      results: #arrayRefProperty & {#ref: "CellExecutionResult"},
      total_duration_ms: #int64Property
    }
  },
  StorageType: #enumSchema & {
    #values: [
      "S3",
      "AZURE",
      "GCS"
    ]
  },
  TableStatistics: #objectSchema & {
    #fields: {
      column_count: #int32Property,
      last_profiled_at: #stringProperty,
      profiled_by: #stringProperty,
      row_count: #int64Property,
      size_bytes: #int64Property
    }
  },
  TabularColumn: #objectSchema & {
    #fields: {
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  URLStyle: #enumSchema & {
    #values: [
      "path",
      "vhost"
    ]
  },
  UpdateCatalogRegistrationRequest: #objectSchema & {
    #fields: {
      comment: #commentProperty,
      data_path: #stringProperty
    }
  },
  UpdateFolderRequest: #objectSchema & {
    #fields: {
      default_environment_id: #stringProperty,
      default_project_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      name: #nameProperty
    }
  },
  UpdatePrincipalRequest: #objectSchema & {
    #fields: {
      is_admin: #boolProperty
    }
  },
  VisualChartType: #enumSchema & {
    #values: [
      "bar",
      "line",
      "area",
      "pie",
      "doughnut",
      "scatter",
      "stacked_bar"
    ]
  },
  VisualEncodings: #objectSchema & {
    #fields: {
      label: #refProperty & {#ref: "VisualFieldBinding"},
      secondary: #refProperty & {#ref: "VisualFieldBinding"},
      series: #refProperty & {#ref: "VisualFieldBinding"},
      value: #refProperty & {#ref: "VisualFieldBinding"},
      x: #refProperty & {#ref: "VisualFieldBinding"},
      y: #refProperty & {#ref: "VisualFieldBinding"}
    }
  },
  VisualFieldBinding: #objectSchema & {
    #fields: {
      field: #stringProperty
    },
    #required: [
      "field"
    ]
  },
  VisualOutputKind: #enumSchema & {
    #values: [
      "table",
      "metric",
      "chart"
    ]
  },
  VisualSpec: #objectSchema & {
    #fields: {
      chart_type: #refProperty & {#ref: "VisualChartType"},
      color_palette: #stringProperty,
      encodings: #refProperty & {#ref: "VisualEncodings"},
      kind: #refProperty & {#ref: "VisualOutputKind"},
      legend: #boolProperty,
      stacked: #boolProperty,
      subtitle: #stringProperty,
      title: #stringProperty
    },
    #required: [
      "kind"
    ]
  },
}
