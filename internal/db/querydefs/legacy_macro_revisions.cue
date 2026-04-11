package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateMacroRevision"
		_table: "macro_revisions"
		params: [
			{name: "ID", type: "string"},
			{name: "MacroID", type: "string"},
			{name: "MacroName", type: "string"},
			{name: "Version", type: "int64"},
			{name: "ContentHash", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "Body", type: "string"},
			{name: "Description", type: "string"},
			{name: "Status", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"macro_id",
				"macro_name",
				"version",
				"content_hash",
				"parameters",
				"body",
				"description",
				"status",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "MacroID"},
				{param: "MacroName"},
				{param: "Version"},
				{param: "ContentHash"},
				{param: "Parameters"},
				{param: "Body"},
				{param: "Description"},
				{param: "Status"},
				{param: "CreatedBy"},
			]
		}
	},
	{
		name: "GetLatestMacroRevisionVersion"
		kind: "one"
		params: [
			{name: "macroID", type: "string"},
		]
		result: {scalar: "int64"}
		select: {
			from: "macro_revisions"
			columns: [
				{expr: "CAST(COALESCE(MAX(version), 0) AS INTEGER)"},
			]
			where: [
				{column: "macro_id", op: "=", param: "macroID"},
			]
		}
	},
	{
		name: "GetMacroRevisionByVersion"
		kind: "one"
		params: [
			{name: "MacroName", type: "string"},
			{name: "Version", type: "int64"},
		]
		result: {table: "macro_revisions"}
		select: {
			from: "macro_revisions"
			where: [
				{column: "macro_name", op: "=", param: "MacroName"},
				{column: "version", op: "=", param: "Version"},
			]
		}
	},
	{
		name: "ListMacroRevisions"
		kind: "many"
		params: [
			{name: "macroName", type: "string"},
		]
		result: {table: "macro_revisions"}
		select: {
			from: "macro_revisions"
			where: [
				{column: "macro_name", op: "=", param: "macroName"},
			]
			orderBy: [
				{expr: "version", desc: true},
			]
		}
	},
]
