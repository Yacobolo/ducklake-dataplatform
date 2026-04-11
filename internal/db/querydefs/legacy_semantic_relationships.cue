package querydefs

queries: [
	#CountAll & {
		name:   "CountSemanticRelationships"
		_table: "semantic_relationships"
	},
	#InsertReturningTable & {
		name:   "CreateSemanticRelationship"
		_table: "semantic_relationships"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "FromSemanticID", type: "string"},
			{name: "ToSemanticID", type: "string"},
			{name: "RelationshipType", type: "string"},
			{name: "JoinSql", type: "string"},
			{name: "Cost", type: "int64"},
			{name: "MaxHops", type: "int64"},
			{name: "CreatedBy", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"from_semantic_id",
				"to_semantic_id",
				"relationship_type",
				"join_sql",
				"cost",
				"max_hops",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "FromSemanticID"},
				{param: "ToSemanticID"},
				{param: "RelationshipType"},
				{param: "JoinSql"},
				{param: "Cost"},
				{param: "MaxHops"},
				{param: "CreatedBy"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteSemanticRelationship"
		_table: "semantic_relationships"
	},
	#GetByID & {
		name:   "GetSemanticRelationshipByID"
		_table: "semantic_relationships"
	},
	#GetByTwoStringFields & {
		name:    "GetSemanticRelationshipByName"
		_table:  "semantic_relationships"
		_field1: "from_semantic_id"
		_param1: "FromSemanticID"
		_field2: "name"
		_param2: "Name"
	},
	#ListPaginatedOrdered & {
		name:   "ListSemanticRelationships"
		_table: "semantic_relationships"
		_order: [
			{expr: "name"},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateSemanticRelationship"
		_table: "semantic_relationships"
		params: [
			{name: "RelationshipType", type: "string"},
			{name: "JoinSql", type: "string"},
			{name: "Cost", type: "int64"},
			{name: "MaxHops", type: "int64"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "relationship_type", value: {param: "RelationshipType"}, coalesceWith: true},
			{column: "join_sql", value: {param: "JoinSql"}, coalesceWith: true},
			{column: "cost", value: {param: "Cost"}, coalesceWith: true},
			{column: "max_hops", value: {param: "MaxHops"}, coalesceWith: true},
		]
	},
]
