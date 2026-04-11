package querydefs

queries: [
	{
		name: "CountPipelines"
		kind: "one"
		result: {scalar: "int64"}
		select: {
			from: "pipelines"
			columns: [
				{expr: "COUNT(*)"},
			]
		}
	},
	{
		name: "CreatePipeline"
		kind: "one"
		params: [
			{name: "ID", type: "string"},
			{name: "Name", type: "string"},
			{name: "Description", type: "string"},
			{name: "ScheduleCron", type: "sql.NullString"},
			{name: "IsPaused", type: "int64"},
			{name: "ConcurrencyLimit", type: "int64"},
			{name: "CreatedBy", type: "string"},
			{name: "FolderID", type: "sql.NullString"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		insert: {
			into: "pipelines"
			columns: [
				"id",
				"name",
				"description",
				"schedule_cron",
				"is_paused",
				"concurrency_limit",
				"created_by",
				"folder_id",
			]
			values: [
				{param: "ID"},
				{param: "Name"},
				{param: "Description"},
				{param: "ScheduleCron"},
				{param: "IsPaused"},
				{param: "ConcurrencyLimit"},
				{param: "CreatedBy"},
				{param: "FolderID"},
			]
			returningColumns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "schedule_cron"},
				{expr: "is_paused"},
				{expr: "concurrency_limit"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
			]
		}
	},
	{
		name: "DeletePipeline"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "pipelines"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetPipelineByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		select: {
			from: "pipelines"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "schedule_cron"},
				{expr: "is_paused"},
				{expr: "concurrency_limit"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "GetPipelineByName"
		kind: "one"
		params: [
			{name: "name", type: "string"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		select: {
			from: "pipelines"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "schedule_cron"},
				{expr: "is_paused"},
				{expr: "concurrency_limit"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
			]
			where: [
				{column: "name", op: "=", param: "name"},
			]
		}
	},
	{
		name: "ListPipelines"
		kind: "many"
		params: [
			{name: "Limit", type: "int64"},
			{name: "Offset", type: "int64"},
		]
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		select: {
			from: "pipelines"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "schedule_cron"},
				{expr: "is_paused"},
				{expr: "concurrency_limit"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
			]
			orderBy: [
				{expr: "name"},
			]
			limitParam: "Limit"
			offsetParam: "Offset"
		}
	},
	{
		name: "ListScheduledPipelines"
		kind: "many"
		result: {
			row: "Pipeline"
			fields: [
				{name: "ID", type: "string"},
				{name: "Name", type: "string"},
				{name: "Description", type: "string"},
				{name: "ScheduleCron", type: "sql.NullString"},
				{name: "IsPaused", type: "int64"},
				{name: "ConcurrencyLimit", type: "int64"},
				{name: "CreatedBy", type: "string"},
				{name: "CreatedAt", type: "string"},
				{name: "UpdatedAt", type: "string"},
				{name: "FolderID", type: "sql.NullString"},
			]
		}
		select: {
			from: "pipelines"
			columns: [
				{expr: "id"},
				{expr: "name"},
				{expr: "description"},
				{expr: "schedule_cron"},
				{expr: "is_paused"},
				{expr: "concurrency_limit"},
				{expr: "created_by"},
				{expr: "created_at"},
				{expr: "updated_at"},
				{expr: "folder_id"},
			]
			where: [
				{column: "schedule_cron", op: "IS NOT", valueSQL: "NULL"},
				{column: "is_paused", op: "=", valueSQL: "0"},
			]
		}
	},
	{
		name: "UpdatePipeline"
		kind: "exec"
		params: [
			{name: "Description", type: "string"},
			{name: "ScheduleCron", type: "sql.NullString"},
			{name: "IsPaused", type: "int64"},
			{name: "ConcurrencyLimit", type: "int64"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		update: {
			table: "pipelines"
			set: [
				{column: "description", value: {param: "Description"}, coalesceWith: true},
				{column: "schedule_cron", value: {param: "ScheduleCron"}, coalesceWith: true},
				{column: "is_paused", value: {param: "IsPaused"}, coalesceWith: true},
				{column: "concurrency_limit", value: {param: "ConcurrencyLimit"}, coalesceWith: true},
				{column: "folder_id", value: {param: "FolderID"}, coalesceWith: true},
				{column: "updated_at", value: {sql: "datetime('now')"}},
			]
			where: [
				{column: "id", op: "=", param: "ID"},
			]
		}
	},
]
