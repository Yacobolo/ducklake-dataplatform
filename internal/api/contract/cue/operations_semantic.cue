package api

// Authored semantic-layer operations.

#semanticLayerTag: "Semantic Layer"

#plainSemanticOperation: #genericOperationSpec & {
	wrapped: false
}

#semanticModelIDPathParameter: #pathStringParameter & {
	#name: "semantic_model_id"
}

#semanticWorkspaceIDPathParameter: #pathStringParameter & {
	#name: "workspace_id"
}

#metricNamePathParameter: #pathStringParameter & {
	#name: "metric_name"
}

#preAggregationNamePathParameter: #pathStringParameter & {
	#name: "pre_aggregation_name"
}

#relationshipNamePathParameter: #pathStringParameter & {
	#name: "relationship_name"
}

#sourceSchemaPathParameter: #pathStringParameter & {
	#name: "source_schema"
}

#sourceTablePathParameter: #pathStringParameter & {
	#name: "source_table"
}

#semanticModelIDQueryParameter: #queryStringParameter & {
	#name: "semantic_model_id"
}

#timestampColumnQueryParameter: #queryStringParameter & {
	#name: "timestamp_column"
}

#maxLagSecondsQueryParameter: #queryInt64Parameter & {
	#name: "max_lag_seconds"
}

#semanticModelPathParameters: [
	#semanticWorkspaceIDPathParameter,
	#semanticModelIDPathParameter,
]

#semanticMetricPathParameters: [
	#semanticWorkspaceIDPathParameter,
	#semanticModelIDPathParameter,
	#metricNamePathParameter,
]

#semanticPreAggregationPathParameters: [
	#semanticWorkspaceIDPathParameter,
	#semanticModelIDPathParameter,
	#preAggregationNamePathParameter,
]

#semanticRelationshipPathParameters: [
	#semanticWorkspaceIDPathParameter,
	#semanticModelIDPathParameter,
	#relationshipNamePathParameter,
]

#semanticWorkspaceListParameters: [
	#semanticWorkspaceIDPathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
]

#metricFreshnessParameters: [
	#metricNamePathParameter,
	#semanticModelIDQueryParameter,
]

#sourceFreshnessParameters: [
	#sourceSchemaPathParameter,
	#sourceTablePathParameter,
	#timestampColumnQueryParameter,
	#maxLagSecondsQueryParameter,
]

#semanticOps: [
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "checkMetricFreshness"
		path:         "/semantic-metrics/{metric_name}/freshness"
		summary:      "Check metric freshness"
		cli:          "semantic freshness check-metric-freshness"
		returns:      "MetricFreshnessStatus"
		error_family: "resource"
		params:       #metricFreshnessParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "listSemanticModels"
		path:         "/workspaces/{workspace_id}/semantic-models"
		summary:      "List semantic models"
		cli:          "semantic semantic-models list"
		returns:      "PaginatedSemanticModels"
		error_family: "standard"
		params:       #semanticWorkspaceListParameters
	},
	#plainSemanticOperation & {
		kind:           "response"
		method:         "post"
		op:             "createSemanticModel"
		path:           "/workspaces/{workspace_id}/semantic-models"
		summary:        "Create semantic model"
		cli:            "semantic semantic-models create"
		returns:        "SemanticModel"
		success_status: 201
		error_family:   "mutating"
		params:         [#semanticWorkspaceIDPathParameter]
		body_ref:       "CreateSemanticModelRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "getSemanticModel"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}"
		summary:      "Get semantic model"
		cli:          "semantic semantic-models get"
		returns:      "SemanticModel"
		error_family: "resource"
		params:       #semanticModelPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateSemanticModel"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}"
		summary:      "Update semantic model"
		cli:          "semantic semantic-models update"
		returns:      "SemanticModel"
		error_family: "mutating"
		params:       #semanticModelPathParameters
		body_ref:     "UpdateSemanticModelRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteSemanticModel"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}"
		summary:      "Delete semantic model"
		cli:          "semantic semantic-models delete"
		error_family: "mutating"
		params:       #semanticModelPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "listSemanticMetrics"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/metrics"
		summary:      "List semantic metrics"
		cli:          "semantic metrics list"
		returns:      "SemanticMetricList"
		error_family: "resource"
		params:       #semanticModelPathParameters
	},
	#plainSemanticOperation & {
		kind:           "response"
		method:         "post"
		op:             "createSemanticMetric"
		path:           "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/metrics"
		summary:        "Create semantic metric"
		cli:            "semantic metrics create"
		returns:        "SemanticMetric"
		success_status: 201
		error_family:   "mutating"
		params:         #semanticModelPathParameters
		body_ref:       "CreateSemanticMetricRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "getSemanticMetric"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/metrics/{metric_name}"
		summary:      "Get semantic metric"
		returns:      "SemanticMetric"
		error_family: "resource"
		params:       #semanticMetricPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateSemanticMetric"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/metrics/{metric_name}"
		summary:      "Update semantic metric"
		cli:          "semantic metrics update"
		returns:      "SemanticMetric"
		error_family: "mutating"
		params:       #semanticMetricPathParameters
		body_ref:     "UpdateSemanticMetricRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteSemanticMetric"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/metrics/{metric_name}"
		summary:      "Delete semantic metric"
		cli:          "semantic metrics delete"
		error_family: "mutating"
		params:       #semanticMetricPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "listSemanticPreAggregations"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/pre-aggregations"
		summary:      "List semantic pre aggregations"
		cli:          "semantic pre-aggregations list"
		returns:      "SemanticPreAggregationList"
		error_family: "resource"
		params:       #semanticModelPathParameters
	},
	#plainSemanticOperation & {
		kind:           "response"
		method:         "post"
		op:             "createSemanticPreAggregation"
		path:           "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/pre-aggregations"
		summary:        "Create semantic pre aggregation"
		cli:            "semantic pre-aggregations create"
		returns:        "SemanticPreAggregation"
		success_status: 201
		error_family:   "mutating"
		params:         #semanticModelPathParameters
		body_ref:       "CreateSemanticPreAggregationRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "getSemanticPreAggregation"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}"
		summary:      "Get semantic pre aggregation"
		returns:      "SemanticPreAggregation"
		error_family: "resource"
		params:       #semanticPreAggregationPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateSemanticPreAggregation"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}"
		summary:      "Update semantic pre aggregation"
		cli:          "semantic pre-aggregations update"
		returns:      "SemanticPreAggregation"
		error_family: "mutating"
		params:       #semanticPreAggregationPathParameters
		body_ref:     "UpdateSemanticPreAggregationRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteSemanticPreAggregation"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}"
		summary:      "Delete semantic pre aggregation"
		cli:          "semantic pre-aggregations delete"
		error_family: "mutating"
		params:       #semanticPreAggregationPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "listSemanticModelRelationships"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/relationships"
		summary:      "List semantic relationships for a semantic model"
		cli:          "semantic semantic-relationships list"
		returns:      "SemanticRelationshipList"
		error_family: "resource"
		params:       #semanticModelPathParameters
	},
	#plainSemanticOperation & {
		kind:           "response"
		method:         "post"
		op:             "createSemanticModelRelationship"
		path:           "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/relationships"
		summary:        "Create semantic relationship for a semantic model"
		cli:            "semantic semantic-relationships create"
		returns:        "SemanticRelationship"
		success_status: 201
		error_family:   "mutating"
		params:         #semanticModelPathParameters
		body_ref:       "CreateSemanticRelationshipRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "getSemanticModelRelationship"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/relationships/{relationship_name}"
		summary:      "Get semantic relationship for a semantic model"
		returns:      "SemanticRelationship"
		error_family: "resource"
		params:       #semanticRelationshipPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updateSemanticModelRelationship"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/relationships/{relationship_name}"
		summary:      "Update semantic relationship for a semantic model"
		cli:          "semantic semantic-relationships update"
		returns:      "SemanticRelationship"
		error_family: "mutating"
		params:       #semanticRelationshipPathParameters
		body_ref:     "UpdateSemanticRelationshipRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deleteSemanticModelRelationship"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/relationships/{relationship_name}"
		summary:      "Delete semantic relationship for a semantic model"
		cli:          "semantic semantic-relationships delete"
		error_family: "mutating"
		params:       #semanticRelationshipPathParameters
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "post"
		op:           "explainMetricQuery"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/query-explanations"
		summary:      "Explain metric query"
		cli:          "semantic explain"
		returns:      "MetricQueryExplainResponse"
		error_family: "mutating"
		params:       #semanticModelPathParameters
		body_ref:     "MetricQueryRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "post"
		op:           "runMetricQuery"
		path:         "/workspaces/{workspace_id}/semantic-models/{semantic_model_id}/query-runs"
		summary:      "Run metric query"
		cli:          "semantic run"
		returns:      "MetricQueryRunResponse"
		error_family: "mutating"
		params:       #semanticModelPathParameters
		body_ref:     "MetricQueryRequest"
		body_description: "Request payload"
	},
	#plainSemanticOperation & {
		kind:         "response"
		method:       "get"
		op:           "checkSourceFreshness"
		path:         "/semantic-sources/{source_schema}/{source_table}/freshness"
		summary:      "Check source freshness"
		cli:          "models freshness check-source-freshness"
		returns:      "SourceFreshnessStatus"
		error_family: "resource"
		params:       #sourceFreshnessParameters
	},
]

endpoints_semantic: [
	for op in #semanticOps {
		(#endpointFromGenericOperation & {
			tag:  #semanticLayerTag
			spec: op
		}).endpoint
	},
]
