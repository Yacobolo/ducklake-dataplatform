package querydefs

queries: [
	{
		name: "CountMacros"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "macros"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateMacro"
		kind: "one"
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
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		insert: {
			into: "macros"
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
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "macro_type"},
				{expr: "parameters"},
				{expr: "body"},
				{expr: "description"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "catalog_name"},
				{expr: "project_name"},
				{expr: "visibility"},
				{expr: "owner"},
				{expr: "properties"},
				{expr: "tags"},
				{expr: "status"},
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
	{
		name: "GetMacroByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		select: {
			from: "macros"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "macro_type"},
				{expr: "parameters"},
				{expr: "body"},
				{expr: "description"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "catalog_name"},
				{expr: "project_name"},
				{expr: "visibility"},
				{expr: "owner"},
				{expr: "properties"},
				{expr: "tags"},
				{expr: "status"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "ListAllMacros"
		kind: "many"
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		select: {
			from: "macros"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "macro_type"},
				{expr: "parameters"},
				{expr: "body"},
				{expr: "description"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "catalog_name"},
				{expr: "project_name"},
				{expr: "visibility"},
				{expr: "owner"},
				{expr: "properties"},
				{expr: "tags"},
				{expr: "status"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	{
		name: "ListMacros"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Macro"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "MacroType", type: "string"},
				{name: "Parameters", type: "string"},
				{name: "Body", type: "string"},
				{name: "Description", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "CatalogName", type: "string"},
				{name: "ProjectName", type: "string"},
				{name: "Visibility", type: "string"},
				{name: "Owner", type: "string"},
				{name: "Properties", type: "string"},
				{name: "Tags", type: "string"},
				{name: "Status", type: "string"},
			]
		}
		select: {
			from: "macros"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "macro_type"},
				{expr: "parameters"},
				{expr: "body"},
				{expr: "description"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "catalog_name"},
				{expr: "project_name"},
				{expr: "visibility"},
				{expr: "owner"},
				{expr: "properties"},
				{expr: "tags"},
				{expr: "status"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
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
