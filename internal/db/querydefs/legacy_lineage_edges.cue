package querydefs

queries: [
	{
		name: "CountDownstreamLineage"
		kind: "one"
		params: [
			{name: "sourceTable", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "lineage_edges"
			columns: [
				{expr: "COUNT(DISTINCT source_table || '->' || COALESCE(target_table, ''))", alias: "cnt"},
			]
			where: [
				{column: "source_table", op: "=", param: "sourceTable"},
			]
		}
	},
	{
		name: "CountUpstreamLineage"
		kind: "one"
		params: [
			{name: "targetTable", type: "sql.NullString"},
		]
		result: {scalar: "int64"}
		select: {
			from: "lineage_edges"
			columns: [
				{expr: "COUNT(DISTINCT source_table)", alias: "cnt"},
			]
			where: [
				{column: "target_table", op: "=", param: "targetTable"},
			]
		}
	},
	{
		name: "DeleteLineageByTable"
		kind: "exec"
		params: [
			{name: "SourceTable", type: "string"},
			{name: "TargetTable", type: "sql.NullString"},
		]
		delete: {
			from: "lineage_edges"
			where: [
				{
					any: [
						{column: "source_table", op: "=", param: "SourceTable"},
						{column: "target_table", op: "=", param: "TargetTable"},
					]
				},
			]
		}
	},
	{
		name: "DeleteLineageByTablePattern"
		kind: "exec"
		params: [
			{name: "SourceTable", type: "string"},
			{name: "TargetTable", type: "sql.NullString"},
		]
		delete: {
			from: "lineage_edges"
			where: [
				{
					any: [
						{column: "source_table", op: "LIKE", param: "SourceTable"},
						{column: "target_table", op: "LIKE", param: "TargetTable"},
					]
				},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteLineageEdge"
		_table: "lineage_edges"
	},
	{
		name: "GetDownstreamLineage"
		kind: "many"
		params: [
			{name: "SourceTable", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GetDownstreamLineageRow"
			fields: [
				{name: "SourceTable", type: "string"},
				{name: "TargetTable", type: "sql.NullString"},
				{name: "EdgeType", type: "string"},
				{name: "PrincipalName", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "SourceSchema", type: "sql.NullString"},
				{name: "TargetSchema", type: "sql.NullString"},
			]
		}
		select: {
			from: "lineage_edges"
			columns: [
				{expr: "DISTINCT source_table"},
				{expr: "target_table"},
				{expr: "edge_type"},
				{expr: "principal_name"},
				{expr: "created_at"},
				{expr: "source_schema"},
				{expr: "target_schema"},
			]
			where: [
				{column: "source_table", op: "=", param: "SourceTable"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "GetUpstreamLineage"
		kind: "many"
		params: [
			{name: "TargetTable", type: "sql.NullString"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "GetUpstreamLineageRow"
			fields: [
				{name: "SourceTable", type: "string"},
				{name: "TargetTable", type: "sql.NullString"},
				{name: "EdgeType", type: "string"},
				{name: "PrincipalName", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "SourceSchema", type: "sql.NullString"},
				{name: "TargetSchema", type: "sql.NullString"},
			]
		}
		select: {
			from: "lineage_edges"
			columns: [
				{expr: "DISTINCT source_table"},
				{expr: "target_table"},
				{expr: "edge_type"},
				{expr: "principal_name"},
				{expr: "created_at"},
				{expr: "source_schema"},
				{expr: "target_schema"},
			]
			where: [
				{column: "target_table", op: "=", param: "TargetTable"},
			]
			orderBy: [
				{expr: "created_at", desc: true},
			]
			limitParam:  "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "InsertLineageEdge"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "SourceTable", type: "string"},
			{name: "TargetTable", type: "sql.NullString"},
			{name: "EdgeType", type: "string"},
			{name: "PrincipalName", type: "string"},
			{name: "QueryHash", type: "sql.NullString"},
			{name: "SourceSchema", type: "sql.NullString"},
			{name: "TargetSchema", type: "sql.NullString"},
		]
		insert: {
			into: "lineage_edges"
			columns: [
				"id",
				"source_table",
				"target_table",
				"edge_type",
				"principal_name",
				"query_hash",
				"source_schema",
				"target_schema",
			]
			values: [
				{param: "ID"},
				{param: "SourceTable"},
				{param: "TargetTable"},
				{param: "EdgeType"},
				{param: "PrincipalName"},
				{param: "QueryHash"},
				{param: "SourceSchema"},
				{param: "TargetSchema"},
			]
		}
	},
	{
		name: "PurgeLineageOlderThan"
		kind: "execrows"
		params: [
			{name: "createdAt", type: "string"},
		]
		delete: {
			from: "lineage_edges"
			where: [
				{column: "created_at", op: "<", param: "createdAt"},
			]
		}
	},
]
