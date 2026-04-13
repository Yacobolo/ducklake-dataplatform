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
  DashboardWidgetLayout: #objectSchema & {
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
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    }
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
    #fields: {
      principal_name: #principalNameProperty,
      role: #refProperty & {#ref: "NotebookShareRole"}
    },
    #required: [
      "principal_name"
    ]
  },
  UpdateCellRequest: #objectSchema & {
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
    #fields: {
      name: #nameProperty,
      description: #descriptionProperty,
      project_override_id: #stringProperty
      environment_override_id: #stringProperty,
    }
  },
}
