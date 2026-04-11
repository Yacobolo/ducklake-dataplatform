package querydefs

queries: [
	#CountAll & {
		name:   "CountPipelines"
		_table: "pipelines"
	},
	#InsertReturningTable & {
		name:   "CreatePipeline"
		_table: "pipelines"
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
		insert: {
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
		}
	},
	#DeleteByID & {
		name:   "DeletePipeline"
		_table: "pipelines"
	},
	#GetByID & {
		name:   "GetPipelineByID"
		_table: "pipelines"
	},
	#GetByStringField & {
		name:   "GetPipelineByName"
		_table: "pipelines"
		_field: "name"
		_param: "name"
	},
	#ListPaginatedOrdered & {
		name:   "ListPipelines"
		_table: "pipelines"
		_order: [
			{expr: "name"},
		]
	},
	#ListAllOrdered & {
		name:   "ListScheduledPipelines"
		_table: "pipelines"
		_order: []
		select: {
			where: [
				{column: "schedule_cron", op: "IS NOT", valueSQL: "NULL"},
				{column: "is_paused", op: "=", valueSQL: "0"},
			]
		}
	},
	#UpdateByIDTouch & {
		name:   "UpdatePipeline"
		_table: "pipelines"
		params: [
			{name: "Description", type: "string"},
			{name: "ScheduleCron", type: "sql.NullString"},
			{name: "IsPaused", type: "int64"},
			{name: "ConcurrencyLimit", type: "int64"},
			{name: "FolderID", type: "sql.NullString"},
			{name: "ID", type: "string"},
		]
		_set: [
			{column: "description", value: {param: "Description"}, coalesceWith: true},
			{column: "schedule_cron", value: {param: "ScheduleCron"}, coalesceWith: true},
			{column: "is_paused", value: {param: "IsPaused"}, coalesceWith: true},
			{column: "concurrency_limit", value: {param: "ConcurrencyLimit"}, coalesceWith: true},
			{column: "folder_id", value: {param: "FolderID"}, coalesceWith: true},
		]
	},
]
