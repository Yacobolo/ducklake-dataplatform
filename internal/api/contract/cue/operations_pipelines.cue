package api

// Authored pipeline operations.

#pipelinesTag: "Pipelines"

#wrappedPipelineOperation: #genericOperationSpec & {
	wrapped: true
}

#plainPipelineOperation: #genericOperationSpec & {
	wrapped: false
}

#pipelineNamePathParameter: #pathStringParameter & {
	#name: "pipeline_name"
}

#jobIDPathParameter: #pathStringParameter & {
	#name: "job_id"
}

#runIDPathParameter: #pathStringParameter & {
	#name: "run_id"
}

#pipelineRunStatusQueryParameter: #queryStringParameter & {
	#name: "status"
}

#pipelinePathParameters: [
	#pipelineNamePathParameter,
]

#pipelineJobPathParameters: [
	#pipelineNamePathParameter,
	#jobIDPathParameter,
]

#pipelineRunPathParameters: [
	#runIDPathParameter,
]

#pipelineRunsParameters: [
	#pipelineNamePathParameter,
	#paginationParameters[0],
	#paginationParameters[1],
	#pipelineRunStatusQueryParameter,
]

#pipelineOps: [
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "listPipelines"
		path:         "/pipelines"
		summary:      "List pipelines"
		cli:          "pipelines list"
		returns:      "PaginatedPipelines"
		error_family: "standard"
		params:       #paginationParameters
	},
	#wrappedPipelineOperation & {
		kind:           "response"
		method:         "post"
		op:             "createPipeline"
		path:           "/pipelines"
		summary:        "Create pipeline"
		cli:            "pipelines create"
		returns:        "Pipeline"
		success_status: 201
		error_family:   "mutating"
		body_ref:       "CreatePipelineRequest"
		body_description: "Request payload"
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "getPipeline"
		path:         "/pipelines/{pipeline_name}"
		summary:      "Get pipeline"
		cli:          "pipelines get"
		returns:      "Pipeline"
		error_family: "resource"
		params:       #pipelinePathParameters
	},
	#plainPipelineOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updatePipeline"
		path:         "/pipelines/{pipeline_name}"
		summary:      "Update pipeline"
		cli:          "pipelines update"
		returns:      "Pipeline"
		error_family: "mutating"
		params:       #pipelinePathParameters
		body_ref:     "UpdatePipelineRequest"
		body_description: "Request payload"
	},
	#plainPipelineOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deletePipeline"
		path:         "/pipelines/{pipeline_name}"
		summary:      "Delete pipeline"
		cli:          "pipelines delete"
		error_family: "mutating"
		params:       #pipelinePathParameters
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "listPipelineJobs"
		path:         "/pipelines/{pipeline_name}/jobs"
		summary:      "List pipeline jobs"
		cli:          "pipelines jobs list"
		returns:      "PipelineJobList"
		error_family: "resource"
		params:       #pipelinePathParameters
	},
	#wrappedPipelineOperation & {
		kind:           "response"
		method:         "post"
		op:             "createPipelineJob"
		path:           "/pipelines/{pipeline_name}/jobs"
		summary:        "Create pipeline job"
		cli:            "pipelines jobs create"
		returns:        "PipelineJob"
		success_status: 201
		error_family:   "mutating"
		params:         #pipelinePathParameters
		body_ref:       "CreatePipelineJobRequest"
		body_description: "Request payload"
	},
	#plainPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "getPipelineJob"
		path:         "/pipelines/{pipeline_name}/jobs/{job_id}"
		summary:      "Get pipeline job"
		returns:      "PipelineJob"
		error_family: "resource"
		params:       #pipelineJobPathParameters
	},
	#plainPipelineOperation & {
		kind:         "response"
		method:       "patch"
		op:           "updatePipelineJob"
		path:         "/pipelines/{pipeline_name}/jobs/{job_id}"
		summary:      "Update pipeline job"
		returns:      "PipelineJob"
		error_family: "mutating"
		params:       #pipelineJobPathParameters
		body_ref:     "UpdatePipelineJobRequest"
		body_description: "Request payload"
	},
	#plainPipelineOperation & {
		kind:         "no_content"
		method:       "delete"
		op:           "deletePipelineJob"
		path:         "/pipelines/{pipeline_name}/jobs/{job_id}"
		summary:      "Delete pipeline job"
		cli:          "pipelines jobs delete"
		error_family: "mutating"
		params:       #pipelineJobPathParameters
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "post"
		op:           "triggerPipelineRun"
		path:         "/pipelines/{pipeline_name}/runs"
		summary:      "Trigger pipeline run"
		cli:          "pipelines runs trigger"
		returns:      "PipelineRun"
		error_family: "mutating"
		params:       #pipelinePathParameters
		body_ref:     "TriggerPipelineRunRequest"
		body_required: false
		body_description: "Request payload"
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "listPipelineRuns"
		path:         "/pipelines/{pipeline_name}/runs"
		summary:      "List pipeline runs"
		cli:          "pipelines runs list"
		returns:      "PaginatedPipelineRuns"
		error_family: "resource"
		params:       #pipelineRunsParameters
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "getPipelineRun"
		path:         "/pipelines/runs/{run_id}"
		summary:      "Get pipeline run"
		cli:          "pipelines runs get"
		returns:      "PipelineRun"
		error_family: "resource"
		params:       #pipelineRunPathParameters
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "post"
		op:           "cancelPipelineRun"
		path:         "/pipelines/runs/{run_id}/cancellations"
		summary:      "Cancel pipeline run"
		cli:          "pipelines runs cancel"
		returns:      "PipelineRun"
		error_family: "mutating"
		params:       #pipelineRunPathParameters
	},
	#wrappedPipelineOperation & {
		kind:         "response"
		method:       "get"
		op:           "listPipelineJobRuns"
		path:         "/pipelines/runs/{run_id}/jobs"
		summary:      "List pipeline job runs"
		cli:          "pipelines runs list-job-runs"
		returns:      "PipelineJobRunList"
		error_family: "resource"
		params:       #pipelineRunPathParameters
	},
]

endpoints_pipelines: [
	for op in #pipelineOps {
		(#endpointFromGenericOperation & {
			tag:  #pipelinesTag
			spec: op
		}).endpoint
	},
]
