package querydefs

queries: [
	{
		name: "CreateSemanticPreAggregation"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
			{name: "MetricSet", type: "string"},
			{name: "DimensionSet", type: "string"},
			{name: "Grain", type: "string"},
			{name: "TargetRelation", type: "string"},
			{name: "RefreshPolicy", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "semantic_pre_aggregations"
			columns: [
				"id",
				"semantic_model_id",
				"name",
				"metric_set",
				"dimension_set",
				"grain",
				"target_relation",
				"refresh_policy",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "SemanticModelID"},
				{param: "Name"},
				{param: "MetricSet"},
				{param: "DimensionSet"},
				{param: "Grain"},
				{param: "TargetRelation"},
				{param: "RefreshPolicy"},
				{param: "CreatedBy"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "metric_set"},
				{expr: "dimension_set"},
				{expr: "grain"},
				{expr: "target_relation"},
				{expr: "refresh_policy"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteSemanticPreAggregation"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "semantic_pre_aggregations"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticPreAggregationByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_pre_aggregations"
			columns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "metric_set"},
				{expr: "dimension_set"},
				{expr: "grain"},
				{expr: "target_relation"},
				{expr: "refresh_policy"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticPreAggregationByName"
		kind: "one"
		params: [
			{name: "SemanticModelID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_pre_aggregations"
			columns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "metric_set"},
				{expr: "dimension_set"},
				{expr: "grain"},
				{expr: "target_relation"},
				{expr: "refresh_policy"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "semantic_model_id", op: "=", param: "SemanticModelID"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
	{
		name: "ListSemanticPreAggregationsByModel"
		kind: "many"
		params: [
			{name: "semanticModelID", type: "string"},
		]
		result: {
			row: "SemanticPreAggregation"
			fields: [
				{name: "ID", type: "string"},
				{name: "SemanticModelID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MetricSet", type: "string"},
				{name: "DimensionSet", type: "string"},
				{name: "Grain", type: "string"},
				{name: "TargetRelation", type: "string"},
				{name: "RefreshPolicy", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_pre_aggregations"
			columns: [
				{expr: "id"},
				{expr: "semantic_model_id"},
				{expr: "name"},
				{expr: "metric_set"},
				{expr: "dimension_set"},
				{expr: "grain"},
				{expr: "target_relation"},
				{expr: "refresh_policy"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
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
		name: "UpdateSemanticPreAggregation"
		kind: "exec"
		params: [
			{name: "MetricSet", type: "string"},
			{name: "DimensionSet", type: "string"},
			{name: "Grain", type: "string"},
			{name: "TargetRelation", type: "string"},
			{name: "RefreshPolicy", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "semantic_pre_aggregations"
			set: [
				{column: "metric_set", value: {param: "MetricSet"}, coalesceWith: true},
				{column: "dimension_set", value: {param: "DimensionSet"}, coalesceWith: true},
				{column: "grain", value: {param: "Grain"}, coalesceWith: true},
				{column: "target_relation", value: {param: "TargetRelation"}, coalesceWith: true},
				{column: "refresh_policy", value: {param: "RefreshPolicy"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
