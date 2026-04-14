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
    example: {
      id:             "audit_01hzyquery1"
      principal_name: "alice@example.com"
      action:         "query.execute"
      statement_type: "SELECT"
      original_sql:   "select * from mart.orders_daily limit 10"
      rewritten_sql:  "select * from mart.orders_daily limit 10"
      tables_accessed:["mart.orders_daily"]
      status:         "ALLOWED"
      error_message:  ""
      duration_ms:    184
      created_at:     "2026-04-13T10:12:00Z"
    }
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
    example: {
      code:    400
      message: "validation failed"
      details: {
        field:  "name"
        reason: "must not be empty"
      }
    }
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
    example: {
      status: "ok"
    }
    #fields: {
      status: #statusProperty
    },
    #required: [
      "status"
    ]
  },
  LineageEdge: #objectSchema & {
    example: {
      id:             "edge_01hzylineage"
      source_table:   "stg_orders"
      target_table:   "mart_orders_daily"
      source_schema:  "staging"
      target_schema:  "mart"
      edge_type:      "transforms_into"
      principal_name: "alice@example.com"
      created_at:     "2026-04-13T08:00:00Z"
    }
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
    example: {
      table_name: "mart.orders_daily"
      upstream: [
        {
          id:             "edge_01hzylineage"
          source_table:   "stg_orders"
          target_table:   "mart_orders_daily"
          source_schema:  "staging"
          target_schema:  "mart"
          edge_type:      "transforms_into"
          principal_name: "alice@example.com"
          created_at:     "2026-04-13T08:00:00Z"
        },
      ]
      downstream: []
    }
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
    example: {
      older_than_days: 30
    }
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
    example: {
      widget: {
        id:          "wgt_01hzymrr"
        dashboard_id:"dash_01hzyrev"
        key:         "mrr_by_plan"
        page_name:   "overview"
        name:        "MRR by plan tier"
        description: "Monthly recurring revenue by plan."
        source: {
          kind: "semantic_query"
        }
        visual_spec: {
          kind:       "chart"
          chart_type: "bar"
        }
        layout: {
          x: 0
          y: 0
          w: 6
          h: 4
        }
        created_at: "2026-04-13T09:00:00Z"
        updated_at: "2026-04-13T09:00:00Z"
      }
      columns: ["plan_tier", "monthly_recurring_revenue"]
      rows: [
        ["Enterprise", 182340.12],
        ["Pro", 98340.55],
      ]
      row_count:      2
      generated_sql:  "select plan_tier, sum(mrr) as monthly_recurring_revenue from mart_revenue group by 1"
    }
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
    example: {
      name: "monthly_recurring_revenue"
    }
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
