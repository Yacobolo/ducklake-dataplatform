package querydefs

queries: [
	{
		name: "CountColumnMasksForTable"
		kind: "one"
		params: [
			{name: "tableID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "column_masks"
			columns: [
				{expr: "COUNT(*)", alias: "cnt"},
			]
			where: [
				{column: "table_id", op: "=", param: "tableID"},
			]
		}
	},
	{
		name: "CreateColumnMask"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "TableID", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "ColumnName", type: "string"},
			{name: "MaskExpression", type: "string"},
			{name: "Description", type: "sql.NullString"},
		]
		result: {
			row: "ColumnMask"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		insert: {
			into: "column_masks"
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
			returningColumns: [
				{expr: "id"},
				{expr: "table_id"},
				{expr: "column_name"},
				{expr: "mask_expression"},
				{expr: "description"},
				{expr: "created_at"},
				{expr: "name"},
			]
		}
	},
	{
		name: "DeleteColumnMask"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "column_masks"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
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
			from: "column_masks"
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
			from: "column_masks"
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
		result: {
			row: "ColumnMask"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		select: {
			from: "column_masks"
			columns: [
				{expr: "id"},
				{expr: "table_id"},
				{expr: "column_name"},
				{expr: "mask_expression"},
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
		name: "ListColumnMasksForTablePaginated"
		kind: "many"
		params: [
			{name: "TableID", type: "string"},
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "ColumnMask"
			fields: [
				{name: "ID", type: "string"},
				{name: "TableID", type: "string"},
				{name: "ColumnName", type: "string"},
				{name: "MaskExpression", type: "string"},
				{name: "Description", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
			]
		}
		select: {
			from: "column_masks"
			columns: [
				{expr: "id"},
				{expr: "table_id"},
				{expr: "column_name"},
				{expr: "mask_expression"},
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
