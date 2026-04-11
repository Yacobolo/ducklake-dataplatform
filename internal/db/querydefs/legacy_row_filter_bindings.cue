package querydefs

queries: [
	{
		name: "GetRowFilterBindingsForFilter"
		kind: "many"
		params: [
			{name: "rowFilterID", type: "string"},
		]
		result: {
			row: "RowFilterBinding"
			fields: [
				{name: "ID", type: "string"},
				{name: "RowFilterID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
			]
		}
		select: {
			from: "row_filter_bindings"
			columns: [
				{expr: "id"},
				{expr: "row_filter_id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
			]
			where: [
				{column: "row_filter_id", op: "=", param: "rowFilterID"},
			]
		}
	},
	{
		name: "UnbindRowFilter"
		kind: "execresult"
		params: [
			{name: "RowFilterID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		delete: {
			from: "row_filter_bindings"
			where: [
				{column: "row_filter_id", op: "=", param: "RowFilterID"},
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
]
