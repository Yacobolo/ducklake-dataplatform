package querydefs

queries: [
	#CountAll & {
		name:   "CountSemanticModels"
		_table: "semantic_models"
	},
	#InsertReturningTable & {
		name:   "CreateSemanticModel"
		_table: "semantic_models"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeleteSemanticModel"
		_table: "semantic_models"
	},
	#GetByID & {
		name:   "GetSemanticModelByID"
		_table: "semantic_models"
	},
	#GetByStringField & {
		name:   "GetSemanticModelByName"
		_table: "semantic_models"
		_field: "name"
		_param: "name"
	},
	#ListAllOrdered & {
		name:   "ListAllSemanticModels"
		_table: "semantic_models"
		_order: [
			{expr: "name"},
		]
	},
	#ListPaginatedOrdered & {
		name:   "ListSemanticModels"
		_table: "semantic_models"
		_order: [
			{expr: "name"},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateSemanticModel"
		_table: "semantic_models"
		params: [
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "BaseModelRef", type: "string"},
			{name: "DefaultTimeDimension", type: "string"},
			{name: "Tags", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "description", value: {param: "Description"}, coalesceWith: true},
			{column: "owner", value: {param: "Owner"}, coalesceWith: true},
			{column: "base_model_ref", value: {param: "BaseModelRef"}, coalesceWith: true},
			{column: "default_time_dimension", value: {param: "DefaultTimeDimension"}, coalesceWith: true},
			{column: "tags", value: {param: "Tags"}, coalesceWith: true},
		]
	},
]
