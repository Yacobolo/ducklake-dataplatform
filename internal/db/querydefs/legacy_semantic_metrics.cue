package querydefs

queries: [
	{
		name: "CreateSemanticMetric"
		kind: "one"
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
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		insert: {
			into: "semantic_metrics"
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
			returningColumns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "metric_type"},
				{expr: "expression_mode"},
				{expr: "expression"},
				{expr: "default_time_grain"},
				{expr: "format"},
				{expr: "owner"},
				{expr: "certification_state"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "label"},
				{expr: "filter_sql"},
				{expr: "relationship_names"},
			]
		}
	},
	{
		name: "DeleteSemanticMetric"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "semantic_metrics"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticMetricByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		select: {
			from: "semantic_metrics"
			columns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "metric_type"},
				{expr: "expression_mode"},
				{expr: "expression"},
				{expr: "default_time_grain"},
				{expr: "format"},
				{expr: "owner"},
				{expr: "certification_state"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "label"},
				{expr: "filter_sql"},
				{expr: "relationship_names"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticMetricByName"
		kind: "one"
		params: [
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		select: {
			from: "semantic_metrics"
			columns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "metric_type"},
				{expr: "expression_mode"},
				{expr: "expression"},
				{expr: "default_time_grain"},
				{expr: "format"},
				{expr: "owner"},
				{expr: "certification_state"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "label"},
				{expr: "filter_sql"},
				{expr: "relationship_names"},
			]
			where: [
				{column: "semantic_model_id", op: "=", param: "SemanticModelID"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
	{
		name: "ListSemanticMetricsByModel"
		kind: "many"
		params: [
			{name: "semanticModelID", type: "string"},
		]
		result: {
			row: "SemanticMetric"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "MetricType", type: "string"},
				{name: "ExpressionMode", type: "string"},
				{name: "Expression", type: "string"},
				{name: "DefaultTimeGrain", type: "string"},
				{name: "Format", type: "string"},
				{name: "Owner", type: "string"},
				{name: "CertificationState", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Label", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "RelationshipNames", type: "string"},
			]
		}
		select: {
			from: "semantic_metrics"
			columns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "metric_type"},
				{expr: "expression_mode"},
				{expr: "expression"},
				{expr: "default_time_grain"},
				{expr: "format"},
				{expr: "owner"},
				{expr: "certification_state"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "label"},
				{expr: "filter_sql"},
				{expr: "relationship_names"},
			]
			where: [
				{column: "semantic_model_id", op: "=", param: "semanticModelID"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	{
		name: "UpdateSemanticMetric"
		kind: "exec"
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
		update: {
			table: "semantic_metrics"
			set: [
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
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
