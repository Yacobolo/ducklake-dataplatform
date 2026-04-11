package querydefs

queries: [
	{
		name: "CountSemanticModels"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "semantic_models"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreateSemanticModel"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "BaseModelRef", type: "string"},
			{name: "DefaultTimeDimension", type: "string"},
			{name: "Tags", type: "string"},
			{name: "CreatedBy", type: "string"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		insert: {
			into: "semantic_models"
			columns: [
				"id",
				"name",
				"description",
				"owner",
				"base_model_ref",
				"default_time_dimension",
				"tags",
				"created_by",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
				{param: "Owner"},
				{param: "BaseModelRef"},
				{param: "DefaultTimeDimension"},
				{param: "Tags"},
				{param: "CreatedBy"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "base_model_ref"},
				{expr: "default_time_dimension"},
				{expr: "tags"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
		}
	},
	{
		name: "DeleteSemanticModel"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "semantic_models"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticModelByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_models"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "base_model_ref"},
				{expr: "default_time_dimension"},
				{expr: "tags"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetSemanticModelByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_models"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "base_model_ref"},
				{expr: "default_time_dimension"},
				{expr: "tags"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "ListAllSemanticModels"
		kind: "many"
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_models"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "base_model_ref"},
				{expr: "default_time_dimension"},
				{expr: "tags"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			orderBy: [
				{expr: "name"},
			]
		}
	},
	{
		name: "ListSemanticModels"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "SemanticModel"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "Owner", type: "string"},
				{name: "BaseModelRef", type: "string"},
				{name: "DefaultTimeDimension", type: "string"},
				{name: "Tags", type: "string"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
			]
		}
		select: {
			from: "semantic_models"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "base_model_ref"},
				{expr: "default_time_dimension"},
				{expr: "tags"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "UpdateSemanticModel"
		kind: "exec"
		params: [
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "BaseModelRef", type: "string"},
			{name: "DefaultTimeDimension", type: "string"},
			{name: "Tags", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "semantic_models"
			set: [
				{column: "description", value: {param: "Description"}, coalesceWith: true},
				{column: "owner", value: {param: "Owner"}, coalesceWith: true},
				{column: "base_model_ref", value: {param: "BaseModelRef"}, coalesceWith: true},
				{column: "default_time_dimension", value: {param: "DefaultTimeDimension"}, coalesceWith: true},
				{column: "tags", value: {param: "Tags"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
