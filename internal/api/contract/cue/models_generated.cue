package api

// Shared authored helper schemas that support generated request/update models.

schemas_generated: {
  CatalogInfo: #objectSchema & {
    example: {
      name:           "analytics"
      comment:        "Primary analytics catalog."
      created_at:     "2026-03-01T08:00:00Z"
      updated_at:     "2026-04-13T08:00:00Z"
      system_managed: false
    }
    #fields: {
      name: #nameProperty
      comment: #commentProperty
      created_at: #createdAtProperty
      updated_at: #updatedAtProperty
      system_managed: #boolProperty
    }
    #required: [
      "name",
    ]
  }
  DashboardNotebookCellSourceUpdate: #objectSchema & {
    #fields: {
      notebook_id: #stringProperty
      cell_id: #stringProperty
    }
  }
  DashboardSQLQuerySourceUpdate: #objectSchema & {
    #fields: {
      sql: #stringProperty
      catalog: #stringProperty
      schema: #stringProperty
    }
  }
  DashboardSemanticQuerySourceUpdate: #objectSchema & {
    #fields: {
      semantic_model_id: #stringProperty
      metrics: #stringArrayProperty
      relationship_names: #stringArrayProperty
      dimensions: #stringArrayProperty
      filters: #stringArrayProperty
      order_by: #stringArrayProperty
      limit: #int32Property
      time_grain: #stringProperty
    }
  }
  DashboardWidgetLayoutUpdate: #objectSchema & {
    example: {
      x: 0
      y: 0
      w: 8
      h: 4
    }
    #fields: {
      x: #int32Property
      y: #int32Property
      w: #int32Property
      h: #int32Property
    }
  }
  DashboardWidgetSourceUpdate: #objectSchema & {
    example: {
      kind: "semantic_query"
      semantic_query: {
        semantic_model_id: "sem_01hzymetrics"
        metrics:           ["monthly_recurring_revenue"]
        dimensions:        ["plan_tier"]
        limit:             12
      }
    }
    #fields: {
      kind: #refProperty & {#ref: "DashboardWidgetSourceKind"}
      sql_query: #refProperty & {#ref: "DashboardSQLQuerySourceUpdate"}
      notebook_cell: #refProperty & {#ref: "DashboardNotebookCellSourceUpdate"}
      semantic_query: #refProperty & {#ref: "DashboardSemanticQuerySourceUpdate"}
    }
  }
  PaginatedSemanticRelationships: #paginatedItemsSchema & {
    #item_ref: "SemanticRelationship"
  }
  VisualEncodingsUpdate: #objectSchema & {
    #fields: {
      x: #refProperty & {#ref: "VisualFieldBindingUpdate"}
      y: #refProperty & {#ref: "VisualFieldBindingUpdate"}
      series: #refProperty & {#ref: "VisualFieldBindingUpdate"}
      label: #refProperty & {#ref: "VisualFieldBindingUpdate"}
      value: #refProperty & {#ref: "VisualFieldBindingUpdate"}
      secondary: #refProperty & {#ref: "VisualFieldBindingUpdate"}
    }
  }
  VisualFieldBindingUpdate: #objectSchema & {
    #fields: {
      field: #stringProperty
    }
  }
  VisualSpecUpdate: #objectSchema & {
    example: {
      kind:       "chart"
      chart_type: "stacked_bar"
      title:      "MRR by plan tier and region"
      legend:     true
      encodings: {
        x: {
          field: "plan_tier"
        }
        value: {
          field: "monthly_recurring_revenue"
        }
        series: {
          field: "sales_region"
        }
      }
    }
    #fields: {
      kind: #refProperty & {#ref: "VisualOutputKind"}
      chart_type: #refProperty & {#ref: "VisualChartType"}
      encodings: #refProperty & {#ref: "VisualEncodingsUpdate"}
      title: #stringProperty
      subtitle: #stringProperty
      legend: #boolProperty
      legend_position: #refProperty & {#ref: "VisualLegendPosition"}
      stacked: #boolProperty
      color_palette: #stringProperty
    }
  }
}
