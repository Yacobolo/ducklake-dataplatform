package querydefs

queries: [
	#InsertReturningTable & {
		name:   "CreatePipelineJob"
		_table: "pipeline_jobs"
		params: [
			{name: "ID", type: "string"},
			{name: "PipelineID", type: "string"},
			{name: "Name", type: "string"},
			{name: "ComputeEndpointID", type: "sql.NullString"},
			{name: "DependsOn", type: "string"},
			{name: "NotebookID", type: "string"},
			{name: "TimeoutSeconds", type: "sql.NullInt64"},
			{name: "RetryCount", type: "int64"},
			{name: "JobOrder", type: "int64"},
			{name: "JobType", type: "string"},
			{name: "ModelSelector", type: "string"},
		]
		insert: {
			columns: [
				"id",
				"pipeline_id",
				"name",
				"compute_endpoint_id",
				"depends_on",
				"notebook_id",
				"timeout_seconds",
				"retry_count",
				"job_order",
				"job_type",
				"model_selector",
			]
			values: [
				{param: "ID"},
				{param: "PipelineID"},
				{param: "Name"},
				{param: "ComputeEndpointID"},
				{param: "DependsOn"},
				{param: "NotebookID"},
				{param: "TimeoutSeconds"},
				{param: "RetryCount"},
				{param: "JobOrder"},
				{param: "JobType"},
				{param: "ModelSelector"},
			]
		}
	},
	#DeleteByID & {
		name:   "DeletePipelineJob"
		_table: "pipeline_jobs"
	},
	{
		name: "DeletePipelineJobsByPipeline"
		kind: "exec"
		params: [
			{name: "pipelineID", type: "string"},
		]
		delete: {
			from: "pipeline_jobs"
			where: [
				{column: "pipeline_id", op: "=", param: "pipelineID"},
			]
		}
	},
	#GetByID & {
		name:   "GetPipelineJobByID"
		_table: "pipeline_jobs"
	},
	{
		name: "ListPipelineJobsByPipeline"
		kind: "many"
		params: [
			{name: "pipelineID", type: "string"},
		]
		result: {table: "pipeline_jobs"}
		select: {
			from: "pipeline_jobs"
			where: [
				{column: "pipeline_id", op: "=", param: "pipelineID"},
			]
			orderBy: [
				{expr: "job_order"},
				{expr: "name"},
			]
		}
	},
]
