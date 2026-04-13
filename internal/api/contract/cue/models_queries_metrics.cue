package api

// Authored query and metric schemas.

schemas_queries_metrics: {
  CancelQueryResponse: #objectSchema & {
    #fields: {
      query_id: #stringProperty,
      status: #refProperty & {#ref: "QueryJobStatus"}
    },
    #required: [
      "query_id",
      "status"
    ]
  },
  ManifestColumn: #objectSchema & {
    #fields: {
      name: #nameProperty,
      type: #stringProperty
    },
    #required: [
      "name",
      "type"
    ]
  },
  ManifestResponse: #objectSchema & {
    #fields: {
      table: #stringProperty,
      schema: #stringProperty,
      columns: #arrayRefProperty & {#ref: "ManifestColumn"},
      files: #stringArrayProperty,
      row_filters: #stringArrayProperty,
      column_masks: #stringMapProperty,
      expires_at: #expiresAtProperty
    },
    #required: [
      "table"
    ]
  },
  MetricFreshnessStatus: #objectSchema & {
    #fields: {
      metric_name: #stringProperty,
      semantic_model_id: #stringProperty,
      semantic_model_name: #stringProperty,
      freshness_status: #stringProperty,
      freshness_basis: #stringArrayProperty,
      selected_pre_aggregation: #stringProperty,
      checked_at: #dateTimeProperty
    }
  },
  MetricQueryExplainResponse: #objectSchema & {
    #fields: {
      plan: #refProperty & {#ref: "MetricQueryPlan"}
    }
  },
  MetricQueryJoinStep: #objectSchema & {
    #fields: {
      relationship_name: #stringProperty,
      from_model: #stringProperty,
      to_model: #stringProperty,
      relationship_type: #stringProperty,
      join_sql: #stringProperty
    }
  },
  MetricQueryPlan: #objectSchema & {
    #fields: {
      base_model_name: #stringProperty,
      base_relation: #stringProperty,
      metrics: #stringArrayProperty,
      dimensions: #stringArrayProperty,
      time_grain: #stringProperty,
      join_path: #arrayRefProperty & {#ref: "MetricQueryJoinStep"},
      selected_pre_aggregation: #stringProperty,
      generated_sql: #stringProperty,
      freshness_basis: #stringArrayProperty,
      freshness_status: #stringProperty,
    }
  },
  MetricQueryRequest: #objectSchema & {
    #fields: {
      metrics: #stringArrayProperty,
      relationship_names: #stringArrayProperty,
      dimensions: #stringArrayProperty,
      filters: #stringArrayProperty,
      order_by: #stringArrayProperty,
      limit: #int32Property,
      time_grain: #stringProperty
    },
    #required: [
      "metrics"
    ]
  },
  MetricQueryRunResponse: #objectSchema & {
    #fields: {
      plan: #refProperty & {#ref: "MetricQueryPlan"},
      result: #refProperty & {#ref: "QueryResult"}
    }
  },
  QueryHistoryEntry: #objectSchema & {
    #fields: {
      id: #idProperty,
      principal_name: #principalNameProperty,
      original_sql: #stringProperty,
      rewritten_sql: #stringProperty,
      statement_type: #stringProperty,
      tables_accessed: #stringArrayProperty,
      status: #refProperty & {#ref: "AuditDecisionStatus"},
      error_message: #stringProperty,
      duration_ms: #int64Property,
      rows_returned: #int64Property,
      created_at: #createdAtProperty
    },
    #required: [
      "id"
    ]
  },
  QueryJob: #objectSchema & {
    #fields: {
      query_id: #stringProperty,
      status: #refProperty & {#ref: "QueryJobStatus"},
      row_count: #int64Property,
      request_id: #stringProperty,
      error: #stringProperty,
      created_at: #createdAtProperty,
      started_at: #dateTimeProperty,
      completed_at: #dateTimeProperty
    },
    #required: [
      "query_id",
      "status",
      "row_count"
    ]
  },
  QueryJobStatus: #enumSchema & {
    #values: [
      "QUEUED",
      "RUNNING",
      "SUCCEEDED",
      "FAILED",
      "CANCELED"
    ]
  },
  QueryRequest: #objectSchema & {
    title:       "Synchronous SQL query request."
    description: "Submits a SQL statement for immediate execution and returns a tabular result when the request completes."
    #fields: {
      sql: #stringProperty
    },
    #required: [
      "sql"
    ]
  },
  QueryResult: #objectSchema & {
    title: "Tabular SQL query result."
    description: "Contains result-set columns, row data, and an optional continuation token when additional rows are available."
    #fields: {
      columns: #arrayRefProperty & {#ref: "TabularColumn"},
      rows: #anyMapArrayProperty,
      row_count: #int64Property,
      next_page_token: #stringProperty
    },
    #required: [
      "columns",
      "rows"
    ]
  },
  SubmitQueryRequest: #objectSchema & {
    #fields: {
      sql: #stringProperty,
      request_id: #stringProperty
    },
    #required: [
      "sql"
    ]
  },
  SubmitQueryResponse: #objectSchema & {
    #fields: {
      query_id: #stringProperty,
      status: #refProperty & {#ref: "QueryJobStatus"}
    },
    #required: [
      "query_id",
      "status"
    ]
  },
  TriggerModelRunRequest: #objectSchema & {
    #fields: {
      project_name: #stringProperty,
      environment_name: #stringProperty,
      model_names: #stringArrayProperty,
      full_refresh: #boolProperty
    },
    #required: [
      "project_name"
    ]
  },
}
