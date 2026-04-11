package querydefs

queries: [
	{
		name: "AddGroupMember"
		kind: "exec"
		params: [
			{name: "GroupID", type: "string"},
			{name: "MemberType", type: "string"},
			{name: "MemberID", type: "string"},
		]
		insert: {
			modifier: "OR IGNORE"
			into:     "group_members"
			columns: [
				"group_id",
				"member_type",
				"member_id",
			]
			values: [
				{param: "GroupID"},
				{param: "MemberType"},
				{param: "MemberID"},
			]
		}
	},
	{
		name: "BindColumnMask"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "ColumnMaskID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
			{name: "SeeOriginal", type: "int64"},
		]
		insert: {
			modifier: "OR IGNORE"
			into:     "column_mask_bindings"
			columns: [
				"id",
				"column_mask_id",
				"principal_id",
				"principal_type",
				"see_original",
			]
			values: [
				{param: "ID"},
				{param: "ColumnMaskID"},
				{param: "PrincipalID"},
				{param: "PrincipalType"},
				{param: "SeeOriginal"},
			]
		}
	},
	{
		name: "BindRowFilter"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "RowFilterID", type: "string"},
			{name: "PrincipalID", type: "string"},
			{name: "PrincipalType", type: "string"},
		]
		insert: {
			modifier: "OR IGNORE"
			into:     "row_filter_bindings"
			columns: [
				"id",
				"row_filter_id",
				"principal_id",
				"principal_type",
			]
			values: [
				{param: "ID"},
				{param: "RowFilterID"},
				{param: "PrincipalID"},
				{param: "PrincipalType"},
			]
		}
	},
	{
		name: "InsertOrReplaceCatalogMetadata"
		kind: "exec"
		params: [
			{name: "SecurableType", type: "string"},
			{name: "SecurableName", type: "string"},
			{name: "Comment", type: "sql.NullString"},
			{name: "Owner", type: "sql.NullString"},
		]
		insert: {
			modifier: "OR REPLACE"
			into:     "catalog_metadata"
			columns: [
				"securable_type",
				"securable_name",
				"comment",
				"owner",
			]
			values: [
				{param: "SecurableType"},
				{param: "SecurableName"},
				{param: "Comment"},
				{param: "Owner"},
			]
		}
	},
]
