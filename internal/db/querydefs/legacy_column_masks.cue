package querydefs

queries: [
	#CountFiltered & {
		name:   "CountColumnMasksForTable"
		_table: "column_masks"
		_params: [
			{name: "tableID", type: "string"},
		]
		_where: [
			{column: "table_id", op: "=", param: "tableID"},
		]
	},
	#InsertReturningTable & {
		name:   "CreateColumnMask"
		_table: "column_masks"
		params: [
			{name: "ID", type: "string"},
			{name: "TableID", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "ColumnName", type: "string"},
			{name: "MaskExpression", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"table_id",
				"name",
				"column_name",
				"mask_expression",
				"description",
			]
			values: [
				{param: "ID"},
				{param: "TableID"},
				{param: "Name"},
				{param: "ColumnName"},
				{param: "MaskExpression"},
				{param: "Description"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteColumnMask"
		_table: "column_masks"
	},
	{
		name: "DeleteColumnMasksByTable"
		kind: "exec"
		params: [
			{name: "tableID", type: "string"},
		]
		delete: {
			from: "column_masks"
			where: [
				{column: "table_id", op: "=", param: "tableID"},
			]
		}
	},
	{
		name: "GetColumnMaskBindingsForPrincipal"
		kind: "many"
		params: [
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "GetColumnMaskBindingsForPrincipalRow"
			fields: [
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "SeeOriginal", type: "int64"},
			]
		}
		select: {
			from:  "column_masks"
			alias: "cm"
			columns: [
				{expr: "cm.table_id"},
				{expr: "cm.column_name"},
				{expr: "cm.mask_expression"},
				{expr: "cmb.see_original"},
			]
			joins: [
				{type: "JOIN", table: "column_mask_bindings", alias: "cmb", on: "cm.id = cmb.column_mask_id"},
			]
			where: [
				{column: "cmb.principal_id", op: "=", param: "PrincipalID"},
				{column: "cmb.principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
	{
		name: "GetColumnMaskForTableAndPrincipal"
		kind: "many"
		params: [
			{name: "TableID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		result: {
			row: "GetColumnMaskForTableAndPrincipalRow"
			fields: [
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "SeeOriginal", type: "int64"},
			]
		}
		select: {
			from:  "column_masks"
			alias: "cm"
			columns: [
				{expr: "cm.column_name"},
				{expr: "cm.mask_expression"},
				{expr: "cmb.see_original"},
			]
			joins: [
				{type: "JOIN", table: "column_mask_bindings", alias: "cmb", on: "cm.id = cmb.column_mask_id"},
			]
			where: [
				{column: "cm.table_id", op: "=", param: "TableID"},
				{column: "cmb.principal_id", op: "=", param: "PrincipalID"},
				{column: "cmb.principal_type", op: "=", param: "PrincipalType"},
			]
		}
	},
	{
		name: "GetColumnMasksForTable"
		kind: "many"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {table: "column_masks"}
		select: {
			from: "column_masks"
			where: [
				{column: "table_id", op: "=", param: "tableID"},
			]
		}
	},
	#ListFilteredPaginatedOrdered & {
		name:   "ListColumnMasksForTablePaginated"
		_table: "column_masks"
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
