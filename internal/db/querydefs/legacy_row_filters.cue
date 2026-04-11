package querydefs

queries: [
	#CountFiltered & {
		name:   "CountRowFiltersForTable"
		_table: "row_filters"
		_params: [
			{name: "tableID", type: "string"},
		]
		_where: [
			{column: "table_id", op: "=", param: "tableID"},
		]
	},
	#InsertReturningTable & {
		name:   "CreateRowFilter"
		_table: "row_filters"
		params: [
			{name: "ID", type: "string"},
			{name: "TableID", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "FilterSql", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		insert: {
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
		result: {table: "row_filters"}
		select: {
			from: "row_filters"
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
		result: {table: "row_filters"}
		select: {
			from:  "row_filters"
			alias: "rf"
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
	#ListFilteredPaginatedOrdered & {
		name:   "ListRowFiltersForTablePaginated"
		_table: "row_filters"
		_params: [
			{name: "TableID", type: "string"},
		]
		_where: [
			{column: "table_id", op: "=", param: "TableID"},
		]
		_order: [
			{expr: "id"},
		]
	},
]
