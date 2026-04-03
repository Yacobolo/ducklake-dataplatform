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

@route("/pipelines/{pipelineName}")
@get
@tag("Pipelines")
@summary("Get pipeline")
op getPipeline(pipelineName: string): Ok<Pipeline> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}")
@patch(#{ implicitOptionality: true })
@tag("Pipelines")
@summary("Update pipeline")
op updatePipeline(pipelineName: string, @body body: UpdatePipelineRequest): Ok<Pipeline> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@delete
@tag("Pipelines")
@summary("Delete pipeline")
op deletePipeline(pipelineName: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@get
@tag("Pipelines")
@summary("List pipeline jobs")
op listPipelineJobs(pipelineName: string): Ok<PipelineJobList> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@post
@tag("Pipelines")
@summary("Create pipeline job")
op createPipelineJob(pipelineName: string, @body body: CreatePipelineJobRequest): Created<PipelineJob> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs/{jobId}")
@delete
@tag("Pipelines")
@summary("Delete pipeline job")
op deletePipelineJob(pipelineName: string, jobId: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@post
@tag("Pipelines")
@summary("Trigger pipeline run")
op triggerPipelineRun(pipelineName: string, @body body?: TriggerPipelineRunRequest): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@get
@tag("Pipelines")
@summary("List pipeline runs")
op listPipelineRuns(pipelineName: string, @query max_results?: int32, @query page_token?: string, @query status?: string): Ok<PaginatedPipelineRuns> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}")
@get
@tag("Pipelines")
@summary("Get pipeline run")
op getPipelineRun(runId: string): Ok<PipelineRun> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}/cancellations")
@post
@tag("Pipelines")
@summary("Cancel pipeline run")
op cancelPipelineRun(runId: string): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/runs/{runId}/jobs")
@get
@tag("Pipelines")
@summary("List pipeline job runs")
op listPipelineJobRuns(runId: string): Ok<PipelineJobRunList> | ResourceErrorResponses;
