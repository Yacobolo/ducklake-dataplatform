package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateSemanticMetric"
		_table: "semantic_metrics"
		params: [
			{name: "ID", type: "string"},
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "MetricType", type: "string"},
			{name: "ExpressionMode", type: "string"},
			{name: "Expression", type: "string"},
			{name: "Label", type: "string"},
			{name: "RelationshipNames", type: "string"},
			{name: "FilterSql", type: "string"},
			{name: "DefaultTimeGrain", type: "string"},
			{name: "Format", type: "string"},
			{name: "Owner", type: "string"},
			{name: "CertificationState", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"semantic_model_id",
				"name",
				"description",
				"metric_type",
				"expression_mode",
				"expression",
				"label",
				"relationship_names",
				"filter_sql",
				"default_time_grain",
				"format",
				"owner",
				"certification_state",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "SemanticModelID"},
				{param: "Name"},
				{param: "Description"},
				{param: "MetricType"},
				{param: "ExpressionMode"},
				{param: "Expression"},
				{param: "Label"},
				{param: "RelationshipNames"},
				{param: "FilterSql"},
				{param: "DefaultTimeGrain"},
				{param: "Format"},
				{param: "Owner"},
				{param: "CertificationState"},
				{param: "CreatedBy"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteSemanticMetric"
		_table: "semantic_metrics"
	},
	#GetByID & {
		name:   "GetSemanticMetricByID"
		_table: "semantic_metrics"
	},
	#GetByTwoStringFields & {
		name:    "GetSemanticMetricByName"
		_table:  "semantic_metrics"
		_field1: "semantic_model_id"
		_param1: "SemanticModelID"
		_field2: "name"
		_param2: "Name"
	},
	{
		name: "ListSemanticMetricsByModel"
		kind: "many"
		params: [
			{name: "semanticModelID", type: "string"},
		]
		result: {table: "semantic_metrics"}
		select: {
			from: "semantic_metrics"
			where: [
				{column: "semantic_model_id", op: "=", param: "semanticModelID"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	#UpdateByIDTouch & {
		name:   "UpdateSemanticMetric"
		_table: "semantic_metrics"
		params: [
			{name: "Description", type: "string"},
			{name: "Label", type: "string"},
			{name: "MetricType", type: "string"},
			{name: "ExpressionMode", type: "string"},
			{name: "Expression", type: "string"},
			{name: "RelationshipNames", type: "string"},
			{name: "FilterSql", type: "string"},
			{name: "DefaultTimeGrain", type: "string"},
			{name: "Format", type: "string"},
			{name: "Owner", type: "string"},
			{name: "CertificationState", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "description", value: {param: "Description"}, coalesceWith: true},
			{column: "label", value: {param: "Label"}, coalesceWith: true},
			{column: "metric_type", value: {param: "MetricType"}, coalesceWith: true},
			{column: "expression_mode", value: {param: "ExpressionMode"}, coalesceWith: true},
			{column: "expression", value: {param: "Expression"}, coalesceWith: true},
			{column: "relationship_names", value: {param: "RelationshipNames"}, coalesceWith: true},
			{column: "filter_sql", value: {param: "FilterSql"}, coalesceWith: true},
			{column: "default_time_grain", value: {param: "DefaultTimeGrain"}, coalesceWith: true},
			{column: "format", value: {param: "Format"}, coalesceWith: true},
			{column: "owner", value: {param: "Owner"}, coalesceWith: true},
			{column: "certification_state", value: {param: "CertificationState"}, coalesceWith: true},
		]
	},
]
