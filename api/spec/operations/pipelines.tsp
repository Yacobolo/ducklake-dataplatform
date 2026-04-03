using TypeSpec.Http;

namespace Duck;

@route("/pipelines")
@get
@tag("Pipelines")
@summary("List pipelines")
op listPipelines(@query max_results?: int32, @query page_token?: string): Ok<PaginatedPipelines> | StandardErrorResponses;

@route("/pipelines")
@post
@tag("Pipelines")
@summary("Create pipeline")
op createPipeline(@body body: CreatePipelineRequest): Created<Pipeline> | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}")
@get
@tag("Pipelines")
@summary("Get pipeline")
op getPipeline(pipeline_name: string): Ok<Pipeline> | ResourceErrorResponses;

@route("/pipelines/{pipeline_name}")
@patch(#{ implicitOptionality: true })
@tag("Pipelines")
@summary("Update pipeline")
op updatePipeline(pipeline_name: string, @body body: UpdatePipelineRequest): Ok<Pipeline> | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}")
@delete
@tag("Pipelines")
@summary("Delete pipeline")
op deletePipeline(pipeline_name: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}/jobs")
@get
@tag("Pipelines")
@summary("List pipeline jobs")
op listPipelineJobs(pipeline_name: string): Ok<PipelineJobList> | ResourceErrorResponses;

@route("/pipelines/{pipeline_name}/jobs")
@post
@tag("Pipelines")
@summary("Create pipeline job")
op createPipelineJob(pipeline_name: string, @body body: CreatePipelineJobRequest): Created<PipelineJob> | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}/jobs/{job_id}")
@get
@tag("Pipelines")
@summary("Get pipeline job")
op getPipelineJob(pipeline_name: string, job_id: string): Ok<PipelineJob> | ResourceErrorResponses;

@route("/pipelines/{pipeline_name}/jobs/{job_id}")
@patch(#{ implicitOptionality: true })
@tag("Pipelines")
@summary("Update pipeline job")
op updatePipelineJob(pipeline_name: string, job_id: string, @body body: UpdatePipelineJobRequest): Ok<PipelineJob> | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}/jobs/{job_id}")
@delete
@tag("Pipelines")
@summary("Delete pipeline job")
op deletePipelineJob(pipeline_name: string, job_id: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}/runs")
@post
@tag("Pipelines")
@summary("Trigger pipeline run")
op triggerPipelineRun(pipeline_name: string, @body body?: TriggerPipelineRunRequest): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/{pipeline_name}/runs")
@get
@tag("Pipelines")
@summary("List pipeline runs")
op listPipelineRuns(pipeline_name: string, @query max_results?: int32, @query page_token?: string, @query status?: string): Ok<PaginatedPipelineRuns> | ResourceErrorResponses;

@route("/pipelines/runs/{run_id}")
@get
@tag("Pipelines")
@summary("Get pipeline run")
op getPipelineRun(run_id: string): Ok<PipelineRun> | ResourceErrorResponses;

@route("/pipelines/runs/{run_id}/cancellations")
@post
@tag("Pipelines")
@summary("Cancel pipeline run")
op cancelPipelineRun(run_id: string): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/runs/{run_id}/jobs")
@get
@tag("Pipelines")
@summary("List pipeline job runs")
op listPipelineJobRuns(run_id: string): Ok<PipelineJobRunList> | ResourceErrorResponses;
