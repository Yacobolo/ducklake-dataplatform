package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateSemanticPreAggregation"
		_table: "semantic_pre_aggregations"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteSemanticPreAggregation"
		_table: "semantic_pre_aggregations"
	},
	#GetByID & {
		name:   "GetSemanticPreAggregationByID"
		_table: "semantic_pre_aggregations"
	},
	#GetByTwoStringFields & {
		name:    "GetSemanticPreAggregationByName"
		_table:  "semantic_pre_aggregations"
		_field1: "semantic_model_id"
		_param1: "SemanticModelID"
		_field2: "name"
		_param2: "Name"
	},
	{
		name: "ListSemanticPreAggregationsByModel"
		kind: "many"
		params: [
			{name: "semanticModelID", type: "string"},
		]
		result: {table: "semantic_pre_aggregations"}
		select: {
			from: "semantic_pre_aggregations"
			where: [
				{column: "semantic_model_id", op: "=", param: "semanticModelID"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	#UpdateByIDTouch & {
		name:   "UpdateSemanticPreAggregation"
		_table: "semantic_pre_aggregations"
		params: [
			{name: "MetricSet", type: "string"},
			{name: "DimensionSet", type: "string"},
			{name: "Grain", type: "string"},
			{name: "TargetRelation", type: "string"},
			{name: "RefreshPolicy", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "metric_set", value: {param: "MetricSet"}, coalesceWith: true},
			{column: "dimension_set", value: {param: "DimensionSet"}, coalesceWith: true},
			{column: "grain", value: {param: "Grain"}, coalesceWith: true},
			{column: "target_relation", value: {param: "TargetRelation"}, coalesceWith: true},
			{column: "refresh_policy", value: {param: "RefreshPolicy"}, coalesceWith: true},
		]
	},
]
