package api

// OpenAPI-only authored schema set used for canonical parity.

openapi_extra_schemas: schemas_generated

schemas_generated: {
  "CatalogInfo": {
    "type": "object"
    "required": [
      "name",
    ]
    "properties": {
      "name": {
        "schema": {
          "type": "string"
        }
      }
      "comment": {
        "schema": {
          "type": "string"
        }
      }
      "created_at": {
        "schema": {
          "type": "string"
          "format": "date-time"
        }
      }
      "updated_at": {
        "schema": {
          "type": "string"
          "format": "date-time"
        }
      }
      "system_managed": {
        "schema": {
          "type": "boolean"
        }
      }
    }
    "property_order": [
      "name",
      "comment",
      "created_at",
      "updated_at",
      "system_managed",
    ]
  }
  "DashboardNotebookCellSourceUpdate": {
    "type": "object"
    "properties": {
      "notebook_id": {
        "schema": {
          "type": "string"
        }
      }
      "cell_id": {
        "schema": {
          "type": "string"
        }
      }
    }
    "property_order": [
      "notebook_id",
      "cell_id",
    ]
  }
  "DashboardSQLQuerySourceUpdate": {
    "type": "object"
    "properties": {
      "sql": {
        "schema": {
          "type": "string"
        }
      }
      "catalog": {
        "schema": {
          "type": "string"
        }
      }
      "schema": {
        "schema": {
          "type": "string"
        }
      }
    }
    "property_order": [
      "sql",
      "catalog",
      "schema",
    ]
  }
  "DashboardSemanticQuerySourceUpdate": {
    "type": "object"
    "properties": {
      "semantic_model_id": {
        "schema": {
          "type": "string"
        }
      }
      "metrics": {
        "schema": {
          "type": "array"
          "items": {
            "type": "string"
          }
        }
      }
      "relationship_names": {
        "schema": {
          "type": "array"
          "items": {
            "type": "string"
          }
        }
      }
      "dimensions": {
        "schema": {
          "type": "array"
          "items": {
            "type": "string"
          }
        }
      }
      "filters": {
        "schema": {
          "type": "array"
          "items": {
            "type": "string"
          }
        }
      }
      "order_by": {
        "schema": {
          "type": "array"
          "items": {
            "type": "string"
          }
        }
      }
      "limit": {
        "schema": {
          "type": "integer"
          "format": "int32"
        }
      }
      "time_grain": {
        "schema": {
          "type": "string"
        }
      }
    }
    "property_order": [
      "semantic_model_id",
      "metrics",
      "relationship_names",
      "dimensions",
      "filters",
      "order_by",
      "limit",
      "time_grain",
    ]
  }
  "DashboardWidgetLayoutUpdate": {
    "type": "object"
    "properties": {
      "x": {
        "schema": {
          "type": "integer"
          "format": "int32"
        }
      }
      "y": {
        "schema": {
          "type": "integer"
          "format": "int32"
        }
      }
      "w": {
        "schema": {
          "type": "integer"
          "format": "int32"
        }
      }
      "h": {
        "schema": {
          "type": "integer"
          "format": "int32"
        }
      }
    }
    "property_order": [
      "x",
      "y",
      "w",
      "h",
    ]
  }
  "DashboardWidgetSourceUpdate": {
    "type": "object"
    "properties": {
      "kind": {
        "schema": {
          "ref": "DashboardWidgetSourceKind"
        }
      }
      "sql_query": {
        "schema": {
          "ref": "DashboardSQLQuerySourceUpdate"
        }
      }
      "notebook_cell": {
        "schema": {
          "ref": "DashboardNotebookCellSourceUpdate"
        }
      }
      "semantic_query": {
        "schema": {
          "ref": "DashboardSemanticQuerySourceUpdate"
        }
      }
    }
    "property_order": [
      "kind",
      "sql_query",
      "notebook_cell",
      "semantic_query",
    ]
  }
  "PaginatedSemanticRelationships": {
    "type": "object"
    "required": [
      "data",
    ]
    "properties": {
      "data": {
        "schema": {
          "type": "array"
          "items": {
            "ref": "SemanticRelationship"
          }
        }
      }
      "next_page_token": {
        "schema": {
          "type": "string"
        }
      }
    }
    "property_order": [
      "data",
      "next_page_token",
    ]
  }
  "VisualEncodingsUpdate": {
    "type": "object"
    "properties": {
      "x": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
      "y": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
      "series": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
      "label": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
      "value": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
      "secondary": {
        "schema": {
          "ref": "VisualFieldBindingUpdate"
        }
      }
    }
    "property_order": [
      "x",
      "y",
      "series",
      "label",
      "value",
      "secondary",
    ]
  }
  "VisualFieldBindingUpdate": {
    "type": "object"
    "properties": {
      "field": {
        "schema": {
          "type": "string"
        }
      }
    }
    "property_order": [
      "field",
    ]
  }
  "VisualSpecUpdate": {
    "type": "object"
    "properties": {
      "kind": {
        "schema": {
          "ref": "VisualOutputKind"
        }
      }
      "chart_type": {
        "schema": {
          "ref": "VisualChartType"
        }
      }
      "encodings": {
        "schema": {
          "ref": "VisualEncodingsUpdate"
        }
      }
      "title": {
        "schema": {
          "type": "string"
        }
      }
      "subtitle": {
        "schema": {
          "type": "string"
        }
      }
      "legend": {
        "schema": {
          "type": "boolean"
        }
      }
      "stacked": {
        "schema": {
          "type": "boolean"
        }
      }
      "color_palette": {
        "schema": {
          "type": "string"
        }
      }
    }
    "property_order": [
      "kind",
      "chart_type",
      "encodings",
      "title",
      "subtitle",
      "legend",
      "stacked",
      "color_palette",
    ]
  }
}
