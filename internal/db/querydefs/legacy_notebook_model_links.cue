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
	{
		name: "GetNotebookModelLinkByModelID"
		kind: "one"
		params: [
			{name: "modelID", type: "string"},
		]
		result: {
			row: "NotebookModelLink"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "OutputCellID", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "notebook_model_links"
			columns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "model_id"},
				{expr: "output_cell_id"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "model_id", op: "=", param: "modelID"},
			]
		}
	},
	{
		name: "GetNotebookModelLinkByNotebookID"
		kind: "one"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {
			row: "NotebookModelLink"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "ModelID", type: "string"},
				{name: "OutputCellID", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "notebook_model_links"
			columns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "model_id"},
				{expr: "output_cell_id"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "notebook_id", op: "=", param: "notebookID"},
			]
		}
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
			columns: ["id", "notebook_id", "model_id", "output_cell_id"]
			values: [
				{param: "ID"},
				{param: "NotebookID"},
				{param: "ModelID"},
				{param: "OutputCellID"},
			]
			conflict: {
				targets: ["notebook_id"]
				doUpdate: [
					{column: "model_id", value: {sql: "excluded.model_id"}},
					{column: "output_cell_id", value: {sql: "excluded.output_cell_id"}},
					{column: "updated_at", value: {sql: "datetime('now')"}},
				]
			}
		}
	},
]
