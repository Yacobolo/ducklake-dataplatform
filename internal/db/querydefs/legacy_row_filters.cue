package querydefs

queries: [
	{
		name: "CountRowFiltersForTable"
		kind: "one"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "row_filters"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "table_id", op: "=", param: "tableID"},
			]
		}
	},
	{
		name: "CreateRowFilter"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "TableID", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "FilterSql", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		insert: {
			into: "row_filters"
			columns: [
				"id",
				"table_id",
				"name",
				"filter_sql",
				"description",
			]
			values: [
				{param: "ID"},
				{param: "TableID"},
				{param: "Name"},
				{param: "FilterSql"},
				{param: "Description"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "table_id"},
				{expr: "filter_sql"},
				{expr: "description"},
				{expr: "created_at"},
				{expr: "name"},
			]
		}
	},
	{
		name: "DeleteRowFilter"
		kind: "execresult"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "row_filters"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "DeleteRowFiltersByTable"
		kind: "exec"
		params: [
			{name: "tableID", type: "string"},
		]
		delete: {
			from: "row_filters"
			where: [
				{column: "table_id", op: "=", param: "tableID"},
			]
		}
	},
	{
		name: "GetRowFiltersForTable"
		kind: "many"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		select: {
			from: "row_filters"
			columns: [
				{expr: "id"},
				{expr: "table_id"},
				{expr: "filter_sql"},
				{expr: "description"},
				{expr: "created_at"},
				{expr: "name"},
			]
			where: [
				{column: "table_id", op: "=", param: "tableID"},
			]
		}
	},
	{
		name: "GetRowFiltersForTableAndPrincipal"
		kind: "many"
		params: [
			{name: "TableID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		select: {
			from: "row_filters"
			alias: "rf"
			columns: [
				{expr: "rf.id"},
				{expr: "rf.table_id"},
				{expr: "rf.filter_sql"},
				{expr: "rf.description"},
				{expr: "rf.created_at"},
				{expr: "rf.name"},
			]
			joins: [
				{type: "JOIN", table: "row_filter_bindings", alias: "rfb", on: "rf.id = rfb.row_filter_id"},
			]
			where: [
				{column: "rf.table_id", op: "=", param: "TableID"},
				{column: "rfb.principal_id", op: "=", param: "PrincipalID"},
				{column: "rfb.principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
	{
		name: "ListRowFiltersForTablePaginated"
		kind: "many"
		params: [
			{name: "TableID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "RowFilter"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "FilterSql", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		select: {
			from: "row_filters"
			columns: [
				{expr: "id"},
				{expr: "table_id"},
				{expr: "filter_sql"},
				{expr: "description"},
				{expr: "created_at"},
				{expr: "name"},
			]
			where: [
				{column: "table_id", op: "=", param: "TableID"},
			]
			orderBy: [
				{expr: "id"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
]
