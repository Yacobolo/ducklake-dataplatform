package querydefs

queries: [
	{
		name: "CountSemanticRelationships"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "semantic_relationships"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateSemanticRelationship"
		kind: "one"
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
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "semantic_relationships"
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
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "from_semantic_id"},
				{expr: "to_semantic_id"},
				{expr: "relationship_type"},
				{expr: "join_sql"},
				{expr: "cost"},
				{expr: "max_hops"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteSemanticRelationship"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "semantic_relationships"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticRelationshipByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_relationships"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "from_semantic_id"},
				{expr: "to_semantic_id"},
				{expr: "relationship_type"},
				{expr: "join_sql"},
				{expr: "cost"},
				{expr: "max_hops"},
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
		name: "GetSemanticRelationshipByName"
		kind: "one"
		params: [
			{name: "FromSemanticID", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_relationships"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "from_semantic_id"},
				{expr: "to_semantic_id"},
				{expr: "relationship_type"},
				{expr: "join_sql"},
				{expr: "cost"},
				{expr: "max_hops"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "from_semantic_id", op: "=", param: "FromSemanticID"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
	{
		name: "ListSemanticRelationships"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "SemanticRelationship"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "FromSemanticID", type: "string"},
				{name: "ToSemanticID", type: "string"},
				{name: "RelationshipType", type: "string"},
				{name: "JoinSql", type: "string"},
				{name: "Cost", type: "int64"},
				{name: "MaxHops", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_relationships"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "from_semantic_id"},
				{expr: "to_semantic_id"},
				{expr: "relationship_type"},
				{expr: "join_sql"},
				{expr: "cost"},
				{expr: "max_hops"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateSemanticRelationship"
		kind: "exec"
		params: [
			{name: "RelationshipType", type: "string"},
			{name: "JoinSql", type: "string"},
			{name: "Cost", type: "int64"},
			{name: "MaxHops", type: "int64"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "semantic_relationships"
			set: [
				{column: "relationship_type", value: {param: "RelationshipType"}, coalesceWith: true},
				{column: "join_sql", value: {param: "JoinSql"}, coalesceWith: true},
				{column: "cost", value: {param: "Cost"}, coalesceWith: true},
				{column: "max_hops", value: {param: "MaxHops"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
