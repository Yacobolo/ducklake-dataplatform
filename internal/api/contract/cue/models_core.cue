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
      id: #idProperty,
      principal_name: #principalNameProperty,
      action: #stringProperty,
      statement_type: #stringProperty,
      original_sql: #stringProperty,
      rewritten_sql: #stringProperty,
      tables_accessed: #stringArrayProperty,
      status: #refProperty & {#ref: "AuditDecisionStatus"},
      error_message: #stringProperty,
      duration_ms: #int64Property,
      created_at: #createdAtProperty
    },
    #required: [
      "id"
    ]
  },
  CreateEnvironmentRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      kind: #refProperty & {#ref: "EnvironmentKind"},
      description: #descriptionProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      compute_endpoint: #stringProperty,
      defer_to_environment: #stringProperty,
      variables: #stringMapProperty,
      source_overrides: #stringMapProperty
    },
    #required: [
      "name",
      "target_catalog",
      "target_schema"
    ]
  },
  CreateFolderRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      parent_folder_id: #stringProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      default_project_id: #stringProperty,
      default_environment_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  CreateProjectRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      kind: #refProperty & {#ref: "ProjectKind"},
      description: #descriptionProperty,
      product_id: #stringProperty,
      default_branch: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  UpdateEnvironmentRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      target_catalog: #stringProperty,
      target_schema: #stringProperty,
      compute_endpoint: #stringProperty,
      defer_to_environment: #stringProperty,
      variables: #stringMapProperty,
      source_overrides: #stringMapProperty
    }
  },
  UpdateProjectRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      product_id: #stringProperty,
      default_branch: #stringProperty
    }
  },
  Error: #objectSchema & {
    title:       "Standard API error response."
    description: "Errors use a shared schema across the API so clients can handle failure responses consistently."
    #fields: {
      code: #int32Property,
      message: #stringProperty,
      details: #stringMapProperty
    },
    #required: [
      "code",
      "message"
    ]
  },
  GitSyncResult: #objectSchema & {
    #fields: {
      notebooks_created: #int32Property,
      notebooks_updated: #int32Property,
      notebooks_deleted: #int32Property,
      commit_sha: #stringProperty
    }
  },
  HealthResponse: #objectSchema & {
    title: "Service health status."
    #fields: {
      status: #statusProperty
    },
    #required: [
      "status"
    ]
  },
  LineageEdge: #objectSchema & {
    #fields: {
      id: #idProperty,
      source_table: #stringProperty,
      target_table: #stringProperty,
      source_schema: #stringProperty,
      target_schema: #stringProperty,
      edge_type: #stringProperty,
      principal_name: #principalNameProperty,
      created_at: #createdAtProperty
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
      metastore_type: #stringProperty,
      storage_backend: #stringProperty,
      data_path: #stringProperty,
      schema_count: #int32Property,
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
      parent_folder_id: #stringProperty,
      confirm_leave_git: #boolProperty,
      confirm_context_change: #boolProperty
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
      widget: #refProperty & {#ref: "DashboardWidget"},
      columns: #stringArrayProperty,
      rows: {
        schema: {
          type: "array",
          items: {
            type: "array",
            items: {}
          }
        }
      },
      row_count: #int64Property,
      generated_sql: #stringProperty
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
      row_count: #int64Property,
      size_bytes: #int64Property,
      column_count: #int32Property,
      last_profiled_at: #dateTimeProperty,
      profiled_by: #stringProperty
    }
  },
  TabularColumn: #objectSchema & {
    title: "Metadata for a result-set column."
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
      data_path: #stringProperty,
      comment: #commentProperty
    }
  },
  UpdateFolderRequest: #objectSchema & {
    #fields: {
      name: #nameProperty,
      git_repo_id: #stringProperty,
      git_root_path: #stringProperty,
      default_project_id: #stringProperty,
      default_environment_id: #stringProperty
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
      x: #refProperty & {#ref: "VisualFieldBinding"},
      y: #refProperty & {#ref: "VisualFieldBinding"},
      series: #refProperty & {#ref: "VisualFieldBinding"},
      label: #refProperty & {#ref: "VisualFieldBinding"},
      value: #refProperty & {#ref: "VisualFieldBinding"},
      secondary: #refProperty & {#ref: "VisualFieldBinding"}
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
  VisualLegendPosition: #enumSchema & {
    #values: [
      "top",
      "right",
      "bottom",
      "left"
    ]
  },
  VisualSpec: #objectSchema & {
    #fields: {
      kind: #refProperty & {#ref: "VisualOutputKind"},
      chart_type: #refProperty & {#ref: "VisualChartType"},
      encodings: #refProperty & {#ref: "VisualEncodings"},
      title: #stringProperty,
      subtitle: #stringProperty,
      legend: #boolProperty,
      legend_position: #refProperty & {#ref: "VisualLegendPosition"},
      stacked: #boolProperty,
      color_palette: #stringProperty
    },
    #required: [
      "kind"
    ]
  },
}
