package api

// OpenAPI-only authored schema set used for canonical parity.

openapi_extra_schemas: schemas_generated

schemas_generated: {
  CatalogInfo: #objectSchema & {
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
    #fields: {
      x: #int32Property
      y: #int32Property
      w: #int32Property
      h: #int32Property
    }
  }
  DashboardWidgetSourceUpdate: #objectSchema & {
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
