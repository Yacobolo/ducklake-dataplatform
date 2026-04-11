package querydefs

queries: [
	{
		name: "CreateCell"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "CellType", type: "string"},
			{name: "Name", type: "sql.NullString"},
			{name: "Role", type: "string"},
			{name: "Disabled", type: "int64"},
			{name: "TestConfig", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "Content", type: "string"},
			{name: "Position", type: "int64"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		insert: {
			into: "cells"
			columns: [
				"id",
				"notebook_id",
				"cell_type",
				"name",
				"role",
				"disabled",
				"test_config",
				"visual_spec",
				"content",
				"position",
			]
			values: [
				{param: "ID"},
				{param: "NotebookID"},
				{param: "CellType"},
				{param: "Name"},
				{param: "Role"},
				{param: "Disabled"},
				{param: "TestConfig"},
				{param: "VisualSpec"},
				{param: "Content"},
				{param: "Position"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "cell_type"},
				{expr: "content"},
				{expr: "position"},
				{expr: "last_result"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "name"},
				{expr: "role"},
				{expr: "disabled"},
				{expr: "test_config"},
				{expr: "visual_spec"},
			]
		}
	},
	{
		name: "DeleteCell"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "cells"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetCell"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		select: {
			from: "cells"
			columns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "cell_type"},
				{expr: "content"},
				{expr: "position"},
				{expr: "last_result"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "name"},
				{expr: "role"},
				{expr: "disabled"},
				{expr: "test_config"},
				{expr: "visual_spec"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetMaxCellPosition"
		kind: "one"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {scalar: "interface{}"}
		select: {
			from: "cells"
			columns: [
				{expr: "COALESCE(MAX(position), -1)"},
			]
			where: [
				{column: "notebook_id", op: "=", param: "notebookID"},
			]
		}
	},
	{
		name: "ListCells"
		kind: "many"
		params: [
			{name: "notebookID", type: "string"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		select: {
			from: "cells"
			columns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "cell_type"},
				{expr: "content"},
				{expr: "position"},
				{expr: "last_result"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "name"},
				{expr: "role"},
				{expr: "disabled"},
				{expr: "test_config"},
				{expr: "visual_spec"},
			]
			where: [
				{column: "notebook_id", op: "=", param: "notebookID"},
			]
			orderBy: [
				{expr: "position"},
			]
		}
	},
	{
		name: "UpdateCell"
		kind: "one"
		params: [
			{name: "Name", type: "sql.NullString"},
			{name: "Role", type: "string"},
			{name: "Disabled", type: "int64"},
			{name: "TestConfig", type: "string"},
			{name: "VisualSpec", type: "string"},
			{name: "Content", type: "string"},
			{name: "Position", type: "int64"},
			{name: "ID", type: "string"},
		]
		result: {
			row: "Cell"
			fields: [
				{name: "ID", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "CellType", type: "string"},
				{name: "Content", type: "string"},
				{name: "Position", type: "int64"},
				{name: "LastResult", type: "sql.NullString"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Name", type: "sql.NullString"},
				{name: "Role", type: "string"},
				{name: "Disabled", type: "int64"},
				{name: "TestConfig", type: "string"},
				{name: "VisualSpec", type: "string"},
			]
		}
		update: {
			table: "cells"
			set: [
				{column: "name", value: {param: "Name"}},
				{column: "role", value: {param: "Role"}},
				{column: "disabled", value: {param: "Disabled"}},
				{column: "test_config", value: {param: "TestConfig"}},
				{column: "visual_spec", value: {param: "VisualSpec"}},
				{column: "content", value: {param: "Content"}},
				{column: "position", value: {param: "Position"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "notebook_id"},
				{expr: "cell_type"},
				{expr: "content"},
				{expr: "position"},
				{expr: "last_result"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "name"},
				{expr: "role"},
				{expr: "disabled"},
				{expr: "test_config"},
				{expr: "visual_spec"},
			]
		}
	},
	{
		name: "UpdateCellPosition"
		kind: "exec"
		params: [
			{name: "Position", type: "int64"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "cells"
			set: [
				{column: "position", value: {param: "Position"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateCellResult"
		kind: "exec"
		params: [
			{name: "LastResult", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "cells"
			set: [
				{column: "last_result", value: {param: "LastResult"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
