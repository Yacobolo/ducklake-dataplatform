package querydefs

queries: [
	{
		name: "DeleteNotebookModelLinkByNotebookID"
		kind: "exec"
		params: [
			{name: "notebookID", type: "string"},
		]
		delete: {
			from: "notebook_model_links"
			where: [
				{column: "notebook_id", op: "=", param: "notebookID"},
			]
		}
	},
	#GetByStringField & {
		name:   "GetNotebookModelLinkByModelID"
		_table: "notebook_model_links"
		_field: "model_id"
		_param: "modelID"
	},
	#GetByStringField & {
		name:   "GetNotebookModelLinkByNotebookID"
		_table: "notebook_model_links"
		_field: "notebook_id"
		_param: "notebookID"
	},
	{
		name: "UpsertNotebookModelLink"
		kind: "exec"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "ModelID", type: "string"},
			{name: "OutputCellID", type: "string"},
		]
		insert: {
			into: "notebook_model_links"
			columns: [
				"id",
				"notebook_id",
				"model_id",
				"output_cell_id",
			]
			values: [
				{param: "ID"},
				{param: "NotebookID"},
				{param: "ModelID"},
				{param: "OutputCellID"},
			]
			conflict: {
				targets: [
					"notebook_id",
				]
				doUpdate: [
					{column: "model_id", value: {sql: "excluded.model_id"}},
					{column: "output_cell_id", value: {sql: "excluded.output_cell_id"}},
					{column: "updated_at", value: {sql: "datetime('now')"}},
				]
			}
		}
	},
]
