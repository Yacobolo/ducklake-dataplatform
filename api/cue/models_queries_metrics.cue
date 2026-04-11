package api

// Authored query and metric schemas.

schemas_queries_metrics: {
  "CancelQueryResponse": #objectSchema & {
    #fields: {
      "query_id": #stringProperty,
      "status": #refProperty & {#ref: "QueryJobStatus"}
    },
    #required: [
      "query_id",
      "status"
    ]
  },
  "ManifestColumn": #objectSchema & {
    #fields: {
      "name": #nameProperty,
      "type": #stringProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  "ManifestResponse": #objectSchema & {
    #fields: {
      "column_masks": #refProperty & {#ref: "Record"},
      "columns": #arrayRefProperty & {#ref: "ManifestColumn"},
      "expires_at": #expiresAtProperty,
      "files": #stringArrayProperty,
      "row_filters": #stringArrayProperty,
      "schema": #stringProperty,
      "table": #stringProperty
    },
    #required: [
      "table"
    ]
  },
  "MetricFreshnessStatus": #objectSchema & {
    #fields: {
      "checked_at": #stringProperty,
      "freshness_basis": #stringArrayProperty,
      "freshness_status": #stringProperty,
      "metric_name": #stringProperty,
      "selected_pre_aggregation": #stringProperty,
      "semantic_model_id": #stringProperty,
      "semantic_model_name": #stringProperty
    }
  },
  "MetricQueryExplainResponse": #objectSchema & {
    #fields: {
      "plan": #refProperty & {#ref: "MetricQueryPlan"}
    }
  },
  "MetricQueryJoinStep": #objectSchema & {
    #fields: {
      "from_model": #stringProperty,
      "join_sql": #stringProperty,
      "relationship_name": #stringProperty,
      "relationship_type": #stringProperty,
      "to_model": #stringProperty
    }
  },
  "MetricQueryPlan": #objectSchema & {
    #fields: {
      "base_model_name": #stringProperty,
      "base_relation": #stringProperty,
      "dimensions": #stringArrayProperty,
      "freshness_basis": #stringArrayProperty,
      "freshness_status": #stringProperty,
      "generated_sql": #stringProperty,
      "join_path": #arrayRefProperty & {#ref: "MetricQueryJoinStep"},
      "metrics": #stringArrayProperty,
      "selected_pre_aggregation": #stringProperty,
      "time_grain": #stringProperty
    }
  },
  "MetricQueryRequest": #objectSchema & {
    #fields: {
      "dimensions": #stringArrayProperty,
      "filters": #stringArrayProperty,
      "limit": #int32Property,
      "metrics": #stringArrayProperty,
      "order_by": #stringArrayProperty,
      "relationship_names": #stringArrayProperty,
      "time_grain": #stringProperty
    },
    #required: [
      "metrics"
    ]
  },
  "MetricQueryRunResponse": #objectSchema & {
    #fields: {
      "plan": #refProperty & {#ref: "MetricQueryPlan"},
      "result": #refProperty & {#ref: "QueryResult"}
    }
  },
  "QueryHistoryEntry": #objectSchema & {
    #fields: {
      "created_at": #createdAtProperty,
      "duration_ms": #int64Property,
      "error_message": #stringProperty,
      "id": #idProperty,
      "original_sql": #stringProperty,
      "principal_name": #principalNameProperty,
      "rewritten_sql": #stringProperty,
      "rows_returned": #int64Property,
      "statement_type": #stringProperty,
      "status": #refProperty & {#ref: "AuditDecisionStatus"},
      "tables_accessed": #stringArrayProperty
    },
    #required: [
      "id"
    ]
  },
  "QueryJob": #objectSchema & {
    #fields: {
      "completed_at": #stringProperty,
      "created_at": #createdAtProperty,
      "error": #stringProperty,
      "query_id": #stringProperty,
      "request_id": #stringProperty,
      "row_count": #int64Property,
      "started_at": #stringProperty,
      "status": #refProperty & {#ref: "QueryJobStatus"}
    },
    #required: [
      "query_id",
      "status",
      "row_count"
    ]
  },
  "QueryJobStatus": #enumSchema & {
    #values: [
      "QUEUED",
      "RUNNING",
      "SUCCEEDED",
      "FAILED",
      "CANCELED"
    ]
  },
  "QueryRequest": #objectSchema & {
    #fields: {
      "sql": #stringProperty
    },
    #required: [
      "sql"
    ]
  },
  "QueryResult": #objectSchema & {
    #fields: {
      "columns": #arrayRefProperty & {#ref: "TabularColumn"},
      "next_page_token": #stringProperty,
      "row_count": #int64Property,
      "rows": #arrayRefProperty & {#ref: "Record"}
    },
    #required: [
      "columns",
      "rows"
    ]
  },
  "SubmitQueryRequest": #objectSchema & {
    #fields: {
      "request_id": #stringProperty,
      "sql": #stringProperty
    },
    #required: [
      "sql"
    ]
  },
  "SubmitQueryResponse": #objectSchema & {
    #fields: {
      "query_id": #stringProperty,
      "status": #refProperty & {#ref: "QueryJobStatus"}
    },
    #required: [
      "query_id",
      "status"
    ]
  },
  "TriggerModelRunRequest": #objectSchema & {
    #fields: {
      "environment_name": #stringProperty,
      "full_refresh": #boolProperty,
      "model_names": #stringArrayProperty,
      "project_name": #stringProperty
    },
    #required: [
      "project_name"
    ]
  },
}
