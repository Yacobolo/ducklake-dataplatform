package api

// Authored notebook and dashboard schemas.

schemas_notebooks_dashboards: {
  Cell: #objectSchema & {
    #fields: {
      id: #idProperty,
      notebook_id: #stringProperty,
      cell_type: #refProperty & {#ref: "CellCellType"},
      name: #nameProperty,
      role: #refProperty & {#ref: "CellRole"},
      disabled: #boolProperty,
      test: #refProperty & {#ref: "NotebookCellTestConfig"},
      visual_spec: #refProperty & {#ref: "VisualSpec"},
      content: #stringProperty,
      position: #int32Property,
      last_result: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  CellCellType: #enumSchema & {
    #values: [
      "sql",
      "markdown"
    ]
  },
  CellExecutionResult: #objectSchema & {
    example: {
      cell_id: "cell_01hzycellsql"
      columns: [
        {
          name: "region"
          type: "VARCHAR"
        },
        {
          name: "orders"
          type: "BIGINT"
        },
      ]
      rows: [
        {
          region: "EMEA"
          orders: 128
        },
        {
          region: "NA"
          orders: 212
        },
      ]
      row_count:    2
      error:        ""
      duration_ms:  184
    }
    #fields: {
      cell_id: #stringProperty,
      columns: #arrayRefProperty & {#ref: "TabularColumn"},
      rows: #anyMapArrayProperty,
      row_count: #int32Property,
      error: #stringProperty,
      duration_ms: #int64Property
    }
  },
  CellList: #objectSchema & {
    #fields: {
      data: #arrayRefProperty & {#ref: "Cell"}
    },
    #required: [
      "data"
    ]
  },
  CellRole: #enumSchema & {
    #values: [
      "transform",
      "output",
      "test",
      "markdown"
    ]
  },
  CreateCellRequest: #objectSchema & {
    example: {
      cell_type: "sql"
      name:      "regional_orders"
      role:      "output"
      content:   "select region, count(*) as orders from analytics.orders group by 1"
      position:  2
      visual_spec: {
        kind:       "chart"
        chart_type: "bar"
        encodings: {
          x: {
            field: "region"
          }
          value: {
            field: "orders"
          }
        }
        title:  "Orders by region"
        legend: false
      }
    }
    #fields: {
      cell_type: #refProperty & {#ref: "CellCellType"},
      name: #nameProperty,
      role: #refProperty & {#ref: "CellRole"},
      disabled: #boolProperty,
      test: #refProperty & {#ref: "NotebookCellTestConfig"},
      visual_spec: #refProperty & {#ref: "VisualSpec"},
      content: #stringProperty,
      position: #int32Property
    },
    #required: [
      "cell_type"
    ]
  },
  CreateDashboardRequest: #objectSchema & {
    example: {
      name:                "Revenue overview"
      description:         "Executive dashboard for weekly revenue and pipeline health."
      owner:               "team-analytics"
      folder_id:           "fld_01hzydashboards"
      semantic_project_name: "revenue"
      semantic_model_name: "executive_metrics"
      compute: {
        mode:          "warehouse"
        endpoint_name: "analytics-prod"
        fallback_local: false
      }
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      owner: #ownerProperty,
      folder_id: #stringProperty,
      semantic_project_name: #stringProperty,
      semantic_model_name: #stringProperty,
      compute: #refProperty & {#ref: "DashboardComputePolicy"}
    },
    #required: [
      "name"
    ]
  },
  CreateDashboardWidgetRequest: #objectSchema & {
    example: {
      key:       "mrr_by_plan"
      page_name: "overview"
      name:      "MRR by plan tier"
      description: "Monthly recurring revenue split by commercial plan."
      source: {
        kind: "semantic_query"
        semantic_query: {
          semantic_model_id: "sem_01hzymetrics"
          metrics:           ["monthly_recurring_revenue"]
          dimensions:        ["plan_tier"]
          order_by:          ["monthly_recurring_revenue DESC"]
          limit:             10
          time_grain:        "month"
        }
      }
      visual_spec: {
        kind:       "chart"
        chart_type: "bar"
        encodings: {
          x: {
            field: "plan_tier"
          }
          value: {
            field: "monthly_recurring_revenue"
          }
        }
        title:  "MRR by plan tier"
        legend: false
      }
      layout: {
        x: 0
        y: 0
        w: 6
        h: 4
      }
    }
    #fields: {
      key: #stringProperty,
      page_name: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      source: #refProperty & {#ref: "DashboardWidgetSource"},
      visual_spec: #refProperty & {#ref: "VisualSpec"},
      layout: #refProperty & {#ref: "DashboardWidgetLayout"}
    },
    #required: [
      "name",
      "source",
      "layout"
    ]
  },
  CreateNotebookRequest: #objectSchema & {
    example: {
      name:        "Revenue diagnostics"
      description: "Notebook for digging into weekly revenue variance."
      source:      "manual"
      folder_id:   "fld_01hzynotebooks"
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      source: #stringProperty,
      folder_id: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  Dashboard: #objectSchema & {
    #fields: {
      id: #idProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      owner: #ownerProperty,
      folder_id: #stringProperty,
      semantic_project_name: #stringProperty,
      semantic_model_name: #stringProperty,
      compute: #refProperty & {#ref: "DashboardComputePolicy"},
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  DashboardComputePolicy: #objectSchema & {
    example: {
      mode:           "warehouse"
      endpoint_name:  "analytics-prod"
      fallback_local: false
    }
    #fields: {
      mode: #stringProperty,
      endpoint_name: #stringProperty,
      fallback_local: #boolProperty
    }
  },
  DashboardDetail: #objectSchema & {
    #fields: {
      dashboard: #refProperty & {#ref: "Dashboard"},
      widgets: #arrayRefProperty & {#ref: "DashboardWidget"}
    }
  },
  DashboardNotebookCellSource: #objectSchema & {
    example: {
      notebook_id: "nb_01hzynotebook"
      cell_id:     "cell_01hzycellsql"
    }
    #fields: {
      notebook_id: #stringProperty,
      cell_id: #stringProperty
    },
    #required: [
      "notebook_id",
      "cell_id"
    ]
  },
  DashboardSQLQuerySource: #objectSchema & {
    example: {
      sql:     "select plan_tier, sum(mrr) as monthly_recurring_revenue from mart_revenue group by 1"
      catalog: "analytics"
      schema:  "mart"
    }
    #fields: {
      sql: #stringProperty,
      catalog: #stringProperty,
      schema: #stringProperty
    },
    #required: [
      "sql"
    ]
  },
  DashboardSemanticQuerySource: #objectSchema & {
    example: {
      semantic_model_id:   "sem_01hzymetrics"
      metrics:             ["monthly_recurring_revenue"]
      relationship_names:  ["account_to_subscription"]
      dimensions:          ["plan_tier"]
      filters:             ["billing_month >= '2026-01-01'"]
      order_by:            ["monthly_recurring_revenue DESC"]
      limit:               10
      time_grain:          "month"
    }
    #fields: {
      semantic_model_id: #stringProperty,
      metrics: #stringArrayProperty,
      relationship_names: #stringArrayProperty,
      dimensions: #stringArrayProperty,
      filters: #stringArrayProperty,
      order_by: #stringArrayProperty,
      limit: #int32Property,
      time_grain: #stringProperty
    },
    #required: [
      "semantic_model_id",
      "metrics"
    ]
  },
  DashboardWidget: #objectSchema & {
    example: {
      id:           "wid_01hzymrr"
      dashboard_id: "dash_01hzyexec"
      key:          "mrr_by_plan"
      page_name:    "overview"
      name:         "MRR by plan tier"
      description:  "Monthly recurring revenue split by plan tier."
      source: {
        kind: "semantic_query"
        semantic_query: {
          semantic_model_id: "sem_01hzymetrics"
          metrics:           ["monthly_recurring_revenue"]
          dimensions:        ["plan_tier"]
          limit:             12
        }
      }
      visual_spec: {
        kind:       "chart"
        chart_type: "bar"
        title:      "MRR by plan tier"
        encodings: {
          x: {
            field: "plan_tier"
          }
          value: {
            field: "monthly_recurring_revenue"
          }
        }
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
    #fields: {
      id: #idProperty,
      dashboard_id: #stringProperty,
      key: #stringProperty,
      page_name: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      source: #refProperty & {#ref: "DashboardWidgetSource"},
      visual_spec: #refProperty & {#ref: "VisualSpec"},
      layout: #refProperty & {#ref: "DashboardWidgetLayout"},
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  DashboardWidgetList: {
    type: "array"
    items: {
      ref: "DashboardWidget"
    }
    example: [{
      id:           "wid_01hzymrr"
      dashboard_id: "dash_01hzyexec"
      key:          "mrr_by_plan"
      page_name:    "overview"
      name:         "MRR by plan tier"
      description:  "Monthly recurring revenue split by plan tier."
      created_at:   "2026-04-13T09:00:00Z"
      updated_at:   "2026-04-13T09:00:00Z"
    }]
  },
  DashboardWidgetLayout: #objectSchema & {
    example: {
      x: 0
      y: 0
      w: 6
      h: 4
    }
    #fields: {
      x: #int32Property,
      y: #int32Property,
      w: #int32Property,
      h: #int32Property
    },
    #required: [
      "x",
      "y",
      "w",
      "h"
    ]
  },
  DashboardWidgetSource: #objectSchema & {
    example: {
      kind: "semantic_query"
      semantic_query: {
        semantic_model_id: "sem_01hzymetrics"
        metrics:           ["monthly_recurring_revenue"]
        dimensions:        ["plan_tier"]
        order_by:          ["monthly_recurring_revenue DESC"]
        limit:             10
        time_grain:        "month"
      }
    }
    #fields: {
      kind: #refProperty & {#ref: "DashboardWidgetSourceKind"},
      sql_query: #refProperty & {#ref: "DashboardSQLQuerySource"},
      notebook_cell: #refProperty & {#ref: "DashboardNotebookCellSource"},
      semantic_query: #refProperty & {#ref: "DashboardSemanticQuerySource"}
    },
    #required: [
      "kind"
    ]
  },
  DashboardWidgetSourceKind: #enumSchema & {
    #values: [
      "sql_query",
      "notebook_cell",
      "semantic_query"
    ]
  },
  DuplicateNotebookRequest: #objectSchema & {
    #fields: {
      folder_id: #stringProperty,
      name: #nameProperty,
      git_path: #stringProperty
    },
    #required: [
      "folder_id"
    ]
  },
  MoveNotebookRequest: #objectSchema & {
    #fields: {
      folder_id: #stringProperty,
      git_path: #stringProperty,
      confirm_leave_git: #boolProperty,
      confirm_context_change: #boolProperty
    },
    #required: [
      "folder_id"
    ]
  },
  Notebook: #objectSchema & {
    #fields: {
      id: #idProperty,
      folder_id: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      owner: #ownerProperty,
      git_repo_id: #stringProperty,
      git_path: #stringProperty,
      project_override_id: #stringProperty,
      environment_override_id: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  NotebookCellTestConfig: #objectSchema & {
    #fields: {
      severity: #refProperty & {#ref: "NotebookTestSeverity"}
    }
  },
  NotebookContext: #objectSchema & {
    #fields: {
      notebook_id: #stringProperty,
      folder_id: #stringProperty,
      workspace_id: #stringProperty,
      effective_project_id: #stringProperty,
      effective_environment_id: #stringProperty,
      effective_git_repo_id: #stringProperty,
      effective_git_root_path: #stringProperty,
      environment_source_id: #stringProperty,
      git_source_folder_id: #stringProperty,
      project_source_folder_id: #stringProperty
    }
  },
  NotebookDetail: #objectSchema & {
    #fields: {
      notebook: #refProperty & {#ref: "Notebook"},
      cells: #arrayRefProperty & {#ref: "Cell"},
      context: #refProperty & {#ref: "NotebookContext"},
      shares: #arrayRefProperty & {#ref: "NotebookShare"},
      publish_model: #refProperty & {#ref: "NotebookPublishModel"}
    }
  },
  NotebookJob: #objectSchema & {
    #fields: {
      id: #idProperty,
      notebook_id: #stringProperty,
      session_id: #stringProperty,
      state: #refProperty & {#ref: "NotebookJobState"},
      result: #stringProperty,
      error: #stringProperty,
      created_at: #createdAtProperty,
      updated_at: #updatedAtProperty
    }
  },
  NotebookJobState: #enumSchema & {
    #values: [
      "pending",
      "running",
      "complete",
      "failed"
    ]
  },
  NotebookPublishModel: #objectSchema & {
    #fields: {
      project_name: #stringProperty,
      name: #nameProperty,
      output_cell_id: #stringProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"}
    }
  },
  NotebookSession: #objectSchema & {
    #fields: {
      id: #idProperty,
      notebook_id: #stringProperty,
      principal: #stringProperty,
      state: #refProperty & {#ref: "NotebookSessionState"},
      created_at: #createdAtProperty,
      last_used_at: #dateTimeProperty
    }
  },
  NotebookSessionState: #enumSchema & {
    #values: [
      "active",
      "closed"
    ]
  },
  NotebookShare: #objectSchema & {
    example: {
      principal_name: "analytics-reviewers"
      role:           "viewer"
    }
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    }
  },
  NotebookShareList: {
    type: "array"
    items: {
      ref: "NotebookShare"
    }
    example: [{
      principal_name: "analytics-reviewers"
      role:           "viewer"
    }]
  },
  NotebookShareRole: #enumSchema & {
    #values: [
      "viewer",
      "editor",
      "manager"
    ]
  },
  NotebookTestSeverity: #enumSchema & {
    #values: [
      "error",
      "warn"
    ]
  },
  PromoteNotebookRequest: #objectSchema & {
    #fields: {
      cell_index: #int32Property,
      project_name: #stringProperty,
      name: #nameProperty,
      materialization: #refProperty & {#ref: "ModelMaterialization"}
    },
    #required: [
      "cell_index",
      "project_name",
      "name"
    ]
  },
  ReorderCellsRequest: #objectSchema & {
    example: {
      cell_ids: ["cell_01hzyintro", "cell_01hzycellsql", "cell_01hzychart"]
    }
    #fields: {
      cell_ids: #stringArrayProperty
    },
    #required: [
      "cell_ids"
    ]
  },
  ResolvedDashboardDetail: #objectSchema & {
    #fields: {
      dashboard: #refProperty & {#ref: "Dashboard"},
      widgets: #arrayRefProperty & {#ref: "ResolvedDashboardWidget"}
    }
  },
  ShareNotebookRequest: #objectSchema & {
    example: {
      principal_name: "analytics-reviewers"
      role:           "viewer"
    }
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    },
    #required: [
      "principal_name"
    ]
  },
  UpdateCellRequest: #objectSchema & {
    example: {
      name:    "regional_orders"
      role:    "output"
      content: "select region, count(*) as orders from analytics.orders where order_status = 'COMPLETED' group by 1"
      visual_spec: {
        chart_type: "bar"
        title:      "Completed orders by region"
        encodings: {
          x: {
            field: "region"
          }
          value: {
            field: "orders"
          }
        }
      }
      position: 2
    }
    #fields: {
      name: #nameProperty,
      role: #refProperty & {#ref: "CellRole"},
      disabled: #boolProperty,
      test: #refProperty & {#ref: "NotebookCellTestConfig"},
      visual_spec: #refProperty & {#ref: "VisualSpecUpdate"},
      content: #stringProperty,
      position: #int32Property
    }
  },
  UpdateDashboardRequest: #objectSchema & {
    example: {
      owner:                 "team-finance"
      name:                  "Revenue overview"
      description:           "Weekly revenue dashboard for GTM and finance stakeholders."
      semantic_project_name: "revenue"
      semantic_model_name:   "executive_metrics"
    }
    #fields: {
      owner: #ownerProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      folder_id: #stringProperty,
      semantic_project_name: #stringProperty,
      semantic_model_name: #stringProperty,
      compute: #refProperty & {#ref: "DashboardComputePolicy"}
    }
  },
  UpdateDashboardWidgetRequest: #objectSchema & {
    example: {
      name: "MRR by plan tier"
      source: {
        kind: "semantic_query"
        semantic_query: {
          metrics:    ["monthly_recurring_revenue"]
          dimensions: ["plan_tier", "sales_region"]
          limit:      12
        }
      }
      visual_spec: {
        title:      "MRR by plan tier and region"
        chart_type: "stacked_bar"
      }
      layout: {
        x: 0
        y: 0
        w: 8
        h: 4
      }
    }
    #fields: {
      key: #stringProperty,
      page_name: #stringProperty,
      name: #nameProperty,
      description: #descriptionProperty,
      source: #refProperty & {#ref: "DashboardWidgetSourceUpdate"},
      visual_spec: #refProperty & {#ref: "VisualSpecUpdate"},
      layout: #refProperty & {#ref: "DashboardWidgetLayoutUpdate"}
    }
  },
  UpdateNotebookRequest: #objectSchema & {
    example: {
      name:                    "Revenue diagnostics"
      description:             "Updated notebook for finance weekly review."
      project_override_id:     "prj_01hzyfinance"
      environment_override_id: "env_01hzyprod"
    }
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      project_override_id: #stringProperty
      environment_override_id: #stringProperty,
    }
  },
}
