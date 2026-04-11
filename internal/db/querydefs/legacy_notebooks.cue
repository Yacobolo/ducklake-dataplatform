package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateNotebook"
		_table: "notebooks"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "sql.NullString"},
			{name: "Owner", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"description",
				"owner",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
				{param: "Owner"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteNotebook"
		_table: "notebooks"
	},
	#GetByID & {
		name:   "GetNotebook"
		_table: "notebooks"
	},
	#UpdateByIDTouch & {
		name:   "UpdateNotebook"
		_table: "notebooks"
		_kind:  "one"
		params: [
			{name: "Name", type: "string"},
			{name: "Description", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "name", value: {param: "Name"}},
			{column: "description", value: {param: "Description"}},
		]
	},
]
