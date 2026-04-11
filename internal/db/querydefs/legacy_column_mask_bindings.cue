package querydefs

queries: [
	{
		name: "GetColumnMaskBindingsForMask"
		kind: "many"
		params: [
			{name: "columnMaskID", type: "string"},
		]
		result: {
			row: "ColumnMaskBinding"
			fields: [
				{name: "ID", type: "string"},
				{name: "ColumnMaskID", type: "string"},
				{name: "PrincipalID", type: "string"},
				{name: "PrincipalType", type: "string"},
				{name: "SeeOriginal", type: "int64"},
			]
		}
		select: {
			from: "column_mask_bindings"
			columns: [
				{expr: "id"},
				{expr: "column_mask_id"},
				{expr: "principal_id"},
				{expr: "principal_type"},
				{expr: "see_original"},
			]
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
