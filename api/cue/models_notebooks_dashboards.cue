package api

// Authored notebook and dashboard schemas.

schemas_notebooks_dashboards: {
  Cell: #objectSchema & {
    #fields: {
      cell_type: #refProperty & {#ref: "CellCellType"},
      content: #stringProperty,
      created_at: #createdAtProperty,
      disabled: #boolProperty,
      id: #idProperty,
      last_result: #stringProperty,
      name: #nameProperty,
      notebook_id: #stringProperty,
      position: #int32Property,
      role: #refProperty & {#ref: "CellRole"},
      test: #refProperty & {#ref: "NotebookCellTestConfig"},
      updated_at: #updatedAtProperty,
      visual_spec: #refProperty & {#ref: "VisualSpec"}
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
      duration_ms: #int64Property,
      error: #stringProperty,
      row_count: #int32Property,
      rows: #arrayRefProperty & {#ref: "Record"}
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
      content: #stringProperty,
      disabled: #boolProperty,
      name: #nameProperty,
      position: #int32Property,
      role: #refProperty & {#ref: "CellRole"},
      test: #refProperty & {#ref: "NotebookCellTestConfig"},
      visual_spec: #refProperty & {#ref: "VisualSpec"}
    },
    #required: [
      "cell_type"
    ]
  },
  CreateDashboardRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      folder_id: #stringProperty,
      name: #nameProperty
    },
    #required: [
      "name"
    ]
  },
  CreateDashboardWidgetRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      layout: #refProperty & {#ref: "DashboardWidgetLayout"},
      name: #nameProperty,
      source: #refProperty & {#ref: "DashboardWidgetSource"},
      visual_spec: #refProperty & {#ref: "VisualSpec"}
    },
    #required: [
      "name",
      "source",
      "layout"
    ]
  },
  CreateNotebookRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      folder_id: #stringProperty,
      name: #nameProperty,
      source: #stringProperty
    },
    #required: [
      "name"
    ]
  },
  Dashboard: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      folder_id: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      updated_at: #updatedAtProperty
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
      cell_id: #stringProperty,
      notebook_id: #stringProperty
    },
    #required: [
      "notebook_id",
      "cell_id"
    ]
  },
  DashboardSQLQuerySource: #objectSchema & {
    #fields: {
      catalog: #stringProperty,
      schema: #stringProperty,
      sql: #stringProperty
    },
    #required: [
      "sql"
    ]
  },
  DashboardSemanticQuerySource: #objectSchema & {
    #fields: {
      dimensions: #stringArrayProperty,
      filters: #stringArrayProperty,
      limit: #int32Property,
      metrics: #stringArrayProperty,
      order_by: #stringArrayProperty,
      relationship_names: #stringArrayProperty,
      semantic_model_id: #stringProperty,
      time_grain: #stringProperty
    },
    #required: [
      "semantic_model_id",
      "metrics"
    ]
  },
  DashboardWidget: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      dashboard_id: #stringProperty,
      description: #descriptionProperty,
      id: #idProperty,
      layout: #refProperty & {#ref: "DashboardWidgetLayout"},
      name: #nameProperty,
      source: #refProperty & {#ref: "DashboardWidgetSource"},
      updated_at: #updatedAtProperty,
      visual_spec: #refProperty & {#ref: "VisualSpec"}
    }
  },
  DashboardWidgetLayout: #objectSchema & {
    #fields: {
      h: #int32Property,
      w: #int32Property,
      x: #int32Property,
      y: #int32Property
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
      notebook_cell: #refProperty & {#ref: "DashboardNotebookCellSource"},
      semantic_query: #refProperty & {#ref: "DashboardSemanticQuerySource"},
      sql_query: #refProperty & {#ref: "DashboardSQLQuerySource"}
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
      git_path: #stringProperty,
      name: #nameProperty
    },
    #required: [
      "folder_id"
    ]
  },
  MoveNotebookRequest: #objectSchema & {
    #fields: {
      confirm_context_change: #boolProperty,
      confirm_leave_git: #boolProperty,
      folder_id: #stringProperty,
      git_path: #stringProperty
    },
    #required: [
      "folder_id"
    ]
  },
  Notebook: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      description: #descriptionProperty,
      environment_override_id: #stringProperty,
      folder_id: #stringProperty,
      git_path: #stringProperty,
      git_repo_id: #stringProperty,
      id: #idProperty,
      name: #nameProperty,
      owner: #ownerProperty,
      project_override_id: #stringProperty,
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
      effective_environment_id: #stringProperty,
      effective_git_repo_id: #stringProperty,
      effective_git_root_path: #stringProperty,
      effective_project_id: #stringProperty,
      environment_source_id: #stringProperty,
      folder_id: #stringProperty,
      git_source_folder_id: #stringProperty,
      notebook_id: #stringProperty,
      project_source_folder_id: #stringProperty,
      workspace_id: #stringProperty
    }
  },
  NotebookDetail: #objectSchema & {
    #fields: {
      cells: #arrayRefProperty & {#ref: "Cell"},
      context: #refProperty & {#ref: "NotebookContext"},
      notebook: #refProperty & {#ref: "Notebook"},
      publish_model: #refProperty & {#ref: "NotebookPublishModel"},
      shares: #arrayRefProperty & {#ref: "NotebookShare"}
    }
  },
  NotebookJob: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      error: #stringProperty,
      id: #idProperty,
      notebook_id: #stringProperty,
      result: #stringProperty,
      session_id: #stringProperty,
      state: #refProperty & {#ref: "NotebookJobState"},
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
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      name: #nameProperty,
      output_cell_id: #stringProperty,
      project_name: #stringProperty
    }
  },
  NotebookSession: #objectSchema & {
    #fields: {
      created_at: #createdAtProperty,
      id: #idProperty,
      last_used_at: #stringProperty,
      notebook_id: #stringProperty,
      principal: #stringProperty,
      state: #refProperty & {#ref: "NotebookSessionState"}
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
      materialization: #refProperty & {#ref: "ModelMaterialization"},
      name: #nameProperty,
      project_name: #stringProperty
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
      content: #stringProperty,
      disabled: #boolProperty,
      name: #nameProperty,
      position: #int32Property,
      role: #refProperty & {#ref: "CellRole"},
      test: #refProperty & {#ref: "NotebookCellTestConfig"},
      visual_spec: #refProperty & {#ref: "VisualSpec"}
    }
  },
  UpdateDashboardRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      folder_id: #stringProperty,
      name: #nameProperty
    }
  },
  UpdateDashboardWidgetRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      layout: #refProperty & {#ref: "DashboardWidgetLayout"},
      name: #nameProperty,
      source: #refProperty & {#ref: "DashboardWidgetSource"},
      visual_spec: #refProperty & {#ref: "VisualSpec"}
    }
  },
  UpdateNotebookRequest: #objectSchema & {
    #fields: {
      description: #descriptionProperty,
      environment_override_id: #stringProperty,
      name: #nameProperty,
      project_override_id: #stringProperty
    }
  },
}
