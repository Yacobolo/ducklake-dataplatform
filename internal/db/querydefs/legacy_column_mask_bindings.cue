package querydefs

queries: [
	{
		name: "GetColumnMaskBindingsForMask"
		kind: "many"
		params: [
			{name: "columnMaskID", type: "string"},
		]
		result: {table: "column_mask_bindings"}
		select: {
			from: "column_mask_bindings"
			where: [
				{column: "column_mask_id", op: "=", param: "columnMaskID"},
			]
		}
	},
	{
		name: "UnbindColumnMask"
		kind: "exec"
		params: [
			{name: "ColumnMaskID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		delete: {
			from: "column_mask_bindings"
			where: [
				{column: "column_mask_id", op: "=", param: "ColumnMaskID"},
				{column: "principal_id", op: "=", param: "PrincipalID"},
				{column: "principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
]
