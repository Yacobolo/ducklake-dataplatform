package querydefs

queries: [
	#CountAll & {
		name:   "CountMacros"
		_table: "macros"
	},
	#InsertReturningTable & {
		name:   "CreateMacro"
		_table: "macros"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "MacroType", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "Body", type: "string"},
			{name: "Description", type: "string"},
			{name: "CatalogName", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "Visibility", type: "string"},
			{name: "Owner", type: "string"},
			{name: "Properties", type: "string"},
			{name: "Tags", type: "string"},
			{name: "Status", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"name",
				"macro_type",
				"parameters",
				"body",
				"description",
				"catalog_name",
				"project_name",
				"visibility",
				"owner",
				"properties",
				"tags",
				"status",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "MacroType"},
				{param: "Parameters"},
				{param: "Body"},
				{param: "Description"},
				{param: "CatalogName"},
				{param: "ProjectName"},
				{param: "Visibility"},
				{param: "Owner"},
				{param: "Properties"},
				{param: "Tags"},
				{param: "Status"},
				{param: "CreatedBy"},
			]
		}
	},
	{
		name: "DeleteMacro"
		kind: "exec"
		params: [
			{name: "name", type: "string"},
		]
		delete: {
			from: "macros"
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	#GetByStringField & {
		name:   "GetMacroByName"
		_table: "macros"
		_field: "name"
		_param: "name"
	},
	#ListAllOrdered & {
		name:   "ListAllMacros"
		_table: "macros"
		_order: [
			{expr: "name"},
		]
	},
	#ListPaginatedOrdered & {
		name:   "ListMacros"
		_table: "macros"
		_order: [
			{expr: "name"},
		]
	},
	{
		name: "UpdateMacro"
		kind: "exec"
		params: [
			{name: "Body", type: "string"},
			{name: "Description", type: "string"},
			{name: "Parameters", type: "string"},
			{name: "Status", type: "string"},
			{name: "CatalogName", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "Visibility", type: "string"},
			{name: "Owner", type: "string"},
			{name: "Properties", type: "string"},
			{name: "Tags", type: "string"},
			{name: "Name", type: "string"},
		]
		update: {
			table: "macros"
			set: [
				{column: "body", value: {param: "Body"}},
				{column: "description", value: {param: "Description"}},
				{column: "parameters", value: {param: "Parameters"}},
				{column: "status", value: {param: "Status"}},
				{column: "catalog_name", value: {param: "CatalogName"}},
				{column: "project_name", value: {param: "ProjectName"}},
				{column: "visibility", value: {param: "Visibility"}},
				{column: "owner", value: {param: "Owner"}},
				{column: "properties", value: {param: "Properties"}},
				{column: "tags", value: {param: "Tags"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
]
