package querydefs

queries: [
	{
		name: "CreatePipelineJob"
		kind: "one"
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
		result: {
			row: "PipelineJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ComputeEndpointID", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "TimeoutSeconds", type: "sql.NullInt64"},
				{name: "RetryCount", type: "int64"},
				{name: "JobOrder", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "JobType", type: "string"},
				{name: "ModelSelector", type: "string"},
			]
		}
		insert: {
			into: "pipeline_jobs"
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
			returningColumns: [
				{expr: "id"},
				{expr: "pipeline_id"},
				{expr: "name"},
				{expr: "compute_endpoint_id"},
				{expr: "depends_on"},
				{expr: "notebook_id"},
				{expr: "timeout_seconds"},
				{expr: "retry_count"},
				{expr: "job_order"},
				{expr: "created_at"},
				{expr: "job_type"},
				{expr: "model_selector"},
			]
		}
	},
	{
		name: "DeletePipelineJob"
		kind: "exec"
		params: [
			{name: "id", type: "string"},
		]
		delete: {
			from: "pipeline_jobs"
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
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
	{
		name: "GetPipelineJobByID"
		kind: "one"
		params: [
			{name: "id", type: "string"},
		]
		result: {
			row: "PipelineJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ComputeEndpointID", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "TimeoutSeconds", type: "sql.NullInt64"},
				{name: "RetryCount", type: "int64"},
				{name: "JobOrder", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "JobType", type: "string"},
				{name: "ModelSelector", type: "string"},
			]
		}
		select: {
			from: "pipeline_jobs"
			columns: [
				{expr: "id"},
				{expr: "pipeline_id"},
				{expr: "name"},
				{expr: "compute_endpoint_id"},
				{expr: "depends_on"},
				{expr: "notebook_id"},
				{expr: "timeout_seconds"},
				{expr: "retry_count"},
				{expr: "job_order"},
				{expr: "created_at"},
				{expr: "job_type"},
				{expr: "model_selector"},
			]
			where: [
				{column: "id", op: "=", param: "id"},
			]
		}
	},
	{
		name: "ListPipelineJobsByPipeline"
		kind: "many"
		params: [
			{name: "pipelineID", type: "string"},
		]
		result: {
			row: "PipelineJob"
			fields: [
				{name: "ID", type: "string"},
				{name: "PipelineID", type: "string"},
				{name: "Name", type: "string"},
				{name: "ComputeEndpointID", type: "sql.NullString"},
				{name: "DependsOn", type: "string"},
				{name: "NotebookID", type: "string"},
				{name: "TimeoutSeconds", type: "sql.NullInt64"},
				{name: "RetryCount", type: "int64"},
				{name: "JobOrder", type: "int64"},
				{name: "CreatedAt", type: "string"},
				{name: "JobType", type: "string"},
				{name: "ModelSelector", type: "string"},
			]
		}
		select: {
			from: "pipeline_jobs"
			columns: [
				{expr: "id"},
				{expr: "pipeline_id"},
				{expr: "name"},
				{expr: "compute_endpoint_id"},
				{expr: "depends_on"},
				{expr: "notebook_id"},
				{expr: "timeout_seconds"},
				{expr: "retry_count"},
				{expr: "job_order"},
				{expr: "created_at"},
				{expr: "job_type"},
				{expr: "model_selector"},
			]
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
