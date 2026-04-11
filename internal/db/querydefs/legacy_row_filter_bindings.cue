package querydefs

queries: [
	{
		name: "GetRowFilterBindingsForFilter"
		kind: "many"
		params: [
			{name: "rowFilterID", type: "string"},
		]
		result: {table: "row_filter_bindings"}
		select: {
			from: "row_filter_bindings"
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
