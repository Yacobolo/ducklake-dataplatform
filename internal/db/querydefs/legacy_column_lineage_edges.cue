package querydefs

queries: [
	{
		name: "DeleteColumnLineageByEdgeID"
		kind: "exec"
		params: [
			{name: "lineageEdgeID", type: "string"},
		]
		delete: {
			from: "column_lineage_edges"
			where: [
				{column: "lineage_edge_id", op: "=", param: "lineageEdgeID"},
			]
		}
	},
	{
		name: "GetColumnLineageByEdgeID"
		kind: "many"
		params: [
			{name: "lineageEdgeID", type: "string"},
		]
		result: {
			row: "ColumnLineageEdge"
			fields: [
				{name: "ID", type: "int64"},
				{name: "LineageEdgeID", type: "string"},
				{name: "TargetColumn", type: "string"},
				{name: "SourceSchema", type: "string"},
				{name: "SourceTable", type: "string"},
				{name: "SourceColumn", type: "string"},
				{name: "TransformType", type: "string"},
				{name: "FunctionName", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "column_lineage_edges"
			columns: [
				{expr: "id"},
				{expr: "lineage_edge_id"},
				{expr: "target_column"},
				{expr: "source_schema"},
				{expr: "source_table"},
				{expr: "source_column"},
				{expr: "transform_type"},
				{expr: "function_name"},
				{expr: "created_at"},
			]
			where: [
				{column: "lineage_edge_id", op: "=", param: "lineageEdgeID"},
			]
			orderBy: [
				{expr: "target_column"},
				{expr: "source_table"},
				{expr: "source_column"},
			]
		}
	},
	{
		name: "GetColumnLineageForSourceColumn"
		kind: "many"
		params: [
			{name: "SourceSchema", type: "string"},
			{name: "SourceTable", type: "string"},
			{name: "SourceColumn", type: "string"},
		]
		result: {
			row: "ColumnLineageEdge"
			fields: [
				{name: "ID", type: "int64"},
				{name: "LineageEdgeID", type: "string"},
				{name: "TargetColumn", type: "string"},
				{name: "SourceSchema", type: "string"},
				{name: "SourceTable", type: "string"},
				{name: "SourceColumn", type: "string"},
				{name: "TransformType", type: "string"},
				{name: "FunctionName", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "column_lineage_edges"
			alias: "cle"
			columns: [
				{expr: "cle.id"},
				{expr: "cle.lineage_edge_id"},
				{expr: "cle.target_column"},
				{expr: "cle.source_schema"},
				{expr: "cle.source_table"},
				{expr: "cle.source_column"},
				{expr: "cle.transform_type"},
				{expr: "cle.function_name"},
				{expr: "cle.created_at"},
			]
			joins: [
				{type: "JOIN", table: "lineage_edges", alias: "le", on: "le.id = cle.lineage_edge_id"},
			]
			where: [
				{column: "cle.source_schema", op: "=", param: "SourceSchema"},
				{column: "cle.source_table", op: "=", param: "SourceTable"},
				{column: "cle.source_column", op: "=", param: "SourceColumn"},
			]
			orderBy: [
				{expr: "cle.target_column"},
			]
		}
	},
	{
		name: "GetColumnLineageForTable"
		kind: "many"
		params: [
			{name: "TargetSchema", type: "sql.NullString"},
			{name: "TargetTable", type: "sql.NullString"},
		]
		result: {
			row: "ColumnLineageEdge"
			fields: [
				{name: "ID", type: "int64"},
				{name: "LineageEdgeID", type: "string"},
				{name: "TargetColumn", type: "string"},
				{name: "SourceSchema", type: "string"},
				{name: "SourceTable", type: "string"},
				{name: "SourceColumn", type: "string"},
				{name: "TransformType", type: "string"},
				{name: "FunctionName", type: "string"},
				{name: "CreatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "column_lineage_edges"
			alias: "cle"
			columns: [
				{expr: "cle.id"},
				{expr: "cle.lineage_edge_id"},
				{expr: "cle.target_column"},
				{expr: "cle.source_schema"},
				{expr: "cle.source_table"},
				{expr: "cle.source_column"},
				{expr: "cle.transform_type"},
				{expr: "cle.function_name"},
				{expr: "cle.created_at"},
			]
			joins: [
				{type: "JOIN", table: "lineage_edges", alias: "le", on: "le.id = cle.lineage_edge_id"},
			]
			where: [
				{column: "le.target_schema", op: "=", param: "TargetSchema"},
				{column: "le.target_table", op: "=", param: "TargetTable"},
			]
			orderBy: [
				{expr: "cle.target_column"},
				{expr: "cle.source_table"},
				{expr: "cle.source_column"},
			]
		}
	},
	{
		name: "InsertColumnLineageEdge"
		kind: "exec"
		params: [
			{name: "LineageEdgeID", type: "string"},
			{name: "TargetColumn", type: "string"},
			{name: "SourceSchema", type: "string"},
			{name: "SourceTable", type: "string"},
			{name: "SourceColumn", type: "string"},
			{name: "TransformType", type: "string"},
			{name: "FunctionName", type: "string"},
		]
		insert: {
			into: "column_lineage_edges"
			columns: [
				"lineage_edge_id",
				"target_column",
				"source_schema",
				"source_table",
				"source_column",
				"transform_type",
				"function_name",
			]
			values: [
				{param: "LineageEdgeID"},
				{param: "TargetColumn"},
				{param: "SourceSchema"},
				{param: "SourceTable"},
				{param: "SourceColumn"},
				{param: "TransformType"},
				{param: "FunctionName"},
			]
		}
	},
]
