package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateCell"
		_table: "cells"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteCell"
		_table: "cells"
	},
	#GetByID & {
		name:   "GetCell"
		_table: "cells"
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
		result: {table: "cells"}
		select: {
			from: "cells"
			where: [
				{column: "notebook_id", op: "=", param: "notebookID"},
			]
			orderBy: [
				{expr: "position"},
			]
		}
	},
	#UpdateByIDTouch & {
		name:   "UpdateCell"
		_table: "cells"
		_kind:  "one"
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
		_set: [
			{column: "name", value: {param: "Name"}},
			{column: "role", value: {param: "Role"}},
			{column: "disabled", value: {param: "Disabled"}},
			{column: "test_config", value: {param: "TestConfig"}},
			{column: "visual_spec", value: {param: "VisualSpec"}},
			{column: "content", value: {param: "Content"}},
			{column: "position", value: {param: "Position"}},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateCellPosition"
		_table: "cells"
		params: [
			{name: "Position", type: "int64"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "position", value: {param: "Position"}},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateCellResult"
		_table: "cells"
		params: [
			{name: "LastResult", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "last_result", value: {param: "LastResult"}},
		]
	},
]
