package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreateModel"
		_table: "models"
		params: [
			{name: "ID", type: "string"},
			{name: "ProjectName", type: "string"},
			{name: "Name", type: "string"},
			{name: "SqlBody", type: "string"},
			{name: "Materialization", type: "string"},
			{name: "Description", type: "string"},
			{name: "Owner", type: "string"},
			{name: "Tags", type: "string"},
			{name: "DependsOn", type: "string"},
			{name: "Config", type: "string"},
			{name: "CreatedBy", type: "string"},
			{name: "Contract", type: "string"},
			{name: "FreshnessMaxLag", type: "sql.NullInt64"},
			{name: "FreshnessCron", type: "sql.NullString"},
		]
		insert: {
			columns: [
				"id",
				"project_name",
				"name",
				"sql_body",
				"materialization",
				"description",
				"owner",
				"tags",
				"depends_on",
				"config",
				"created_by",
				"contract",
				"freshness_max_lag",
				"freshness_cron",
			]
			values: [
				{param: "ID"},
				{param: "ProjectName"},
				{param: "Name"},
				{param: "SqlBody"},
				{param: "Materialization"},
				{param: "Description"},
				{param: "Owner"},
				{param: "Tags"},
				{param: "DependsOn"},
				{param: "Config"},
				{param: "CreatedBy"},
				{param: "Contract"},
				{param: "FreshnessMaxLag"},
				{param: "FreshnessCron"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeleteModel"
		_table: "models"
	},
	#GetByID & {
		name:   "GetModelByID"
		_table: "models"
	},
	#GetByTwoStringFields & {
		name:    "GetModelByName"
		_table:  "models"
		_field1: "project_name"
		_param1: "ProjectName"
		_field2: "name"
		_param2: "Name"
	},
	#ListAllOrdered & {
		name:   "ListAllModels"
		_table: "models"
		_order: [{expr: "project_name"}, {expr: "name"}]
	},
	#UpdateByIDTouch & {
		name:   "UpdateModel"
		_table: "models"
		params: [
			{name: "SqlBody", type: "string"},
			{name: "Materialization", type: "string"},
			{name: "Description", type: "string"},
			{name: "Tags", type: "string"},
			{name: "Config", type: "string"},
			{name: "Contract", type: "string"},
			{name: "FreshnessMaxLag", type: "sql.NullInt64"},
			{name: "FreshnessCron", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "sql_body", value: {param: "SqlBody"}, coalesceWith: true},
			{column: "materialization", value: {param: "Materialization"}, coalesceWith: true},
			{column: "description", value: {param: "Description"}, coalesceWith: true},
			{column: "tags", value: {param: "Tags"}, coalesceWith: true},
			{column: "config", value: {param: "Config"}, coalesceWith: true},
			{column: "contract", value: {param: "Contract"}, coalesceWith: true},
			{column: "freshness_max_lag", value: {param: "FreshnessMaxLag"}},
			{column: "freshness_cron", value: {param: "FreshnessCron"}},
		]
	},
	#UpdateByIDTouch & {
		name:   "UpdateModelDependencies"
		_table: "models"
		params: [
			{name: "DependsOn", type: "string"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "depends_on", value: {param: "DependsOn"}},
		]
	},
]
