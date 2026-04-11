package querydefs

queries: [
	{
		name: "CreateModel"
		kind: "one"
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
		result: {
			row: "Model"
			fields: [
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
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		insert: {
			into: "models"
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
			returningColumns: [
				{expr: "id"},
				{expr: "project_name"},
				{expr: "name"},
				{expr: "sql_body"},
				{expr: "materialization"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "tags"},
				{expr: "depends_on"},
				{expr: "config"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "contract"},
				{expr: "freshness_max_lag"},
				{expr: "freshness_cron"},
			]
		}
	},
	{
		name: "DeleteModel"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "models"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetModelByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Model"
			fields: [
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
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		select: {
			from: "models"
			columns: [
				{expr: "id"},
				{expr: "project_name"},
				{expr: "name"},
				{expr: "sql_body"},
				{expr: "materialization"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "tags"},
				{expr: "depends_on"},
				{expr: "config"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "contract"},
				{expr: "freshness_max_lag"},
				{expr: "freshness_cron"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetModelByName"
		kind: "one"
		params: [
			{name: "ProjectName", type: "string"},
			{name: "Name", type: "string"},
		]
		result: {
			row: "Model"
			fields: [
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
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		select: {
			from: "models"
			columns: [
				{expr: "id"},
				{expr: "project_name"},
				{expr: "name"},
				{expr: "sql_body"},
				{expr: "materialization"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "tags"},
				{expr: "depends_on"},
				{expr: "config"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "contract"},
				{expr: "freshness_max_lag"},
				{expr: "freshness_cron"},
			]
			where: [
				{column: "project_name", op: "=", param: "ProjectName"},
				{column: "name", op: "=", param: "Name"},
			]
		}
	},
	{
		name: "ListAllModels"
		kind: "many"
		result: {
			row: "Model"
			fields: [
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
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "Contract", type: "string"},
				{name: "FreshnessMaxLag", type: "sql.NullInt64"},
				{name: "FreshnessCron", type: "sql.NullString"},
			]
		}
		select: {
			from: "models"
			columns: [
				{expr: "id"},
				{expr: "project_name"},
				{expr: "name"},
				{expr: "sql_body"},
				{expr: "materialization"},
				{expr: "description"},
				{expr: "owner"},
				{expr: "tags"},
				{expr: "depends_on"},
				{expr: "config"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "contract"},
				{expr: "freshness_max_lag"},
				{expr: "freshness_cron"},
			]
			orderBy: [
				{expr: "project_name"},
				{expr: "name"},
			]
		}
	},
	{
		name: "UpdateModel"
		kind: "exec"
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
		update: {
			table: "models"
			set: [
				{column: "sql_body", value: {param: "SqlBody"}, coalesceWith: true},
				{column: "materialization", value: {param: "Materialization"}, coalesceWith: true},
				{column: "description", value: {param: "Description"}, coalesceWith: true},
				{column: "tags", value: {param: "Tags"}, coalesceWith: true},
				{column: "config", value: {param: "Config"}, coalesceWith: true},
				{column: "contract", value: {param: "Contract"}, coalesceWith: true},
				{column: "freshness_max_lag", value: {param: "FreshnessMaxLag"}},
				{column: "freshness_cron", value: {param: "FreshnessCron"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
	{
		name: "UpdateModelDependencies"
		kind: "exec"
		params: [
			{name: "DependsOn", type: "string"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "models"
			set: [
				{column: "depends_on", value: {param: "DependsOn"}},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
