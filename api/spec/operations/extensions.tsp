using TypeSpec.Http;

namespace Duck;

@route("/queries")
@post
op submitQuery(@body body: SubmitQueryRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/queries/{queryId}")
@get
op getQuery(queryId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/queries/{queryId}")
@delete
op deleteQuery(queryId: string): NoContent | MutatingErrorResponses;

@route("/queries/{queryId}/results")
@get
op getQueryResults(queryId: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/queries/{queryId}/cancel")
@post
op cancelQuery(queryId: string): Created<GenericResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/upload-url")
@post
op createUploadUrl(catalogName: string, schemaName: string, tableName: string, @body body: UploadUrlRequest): Ok<UploadUrlResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/commit")
@post
op commitTableIngestion(catalogName: string, schemaName: string, tableName: string, @body body: CommitIngestionRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/load")
@post
op loadTableExternalFiles(catalogName: string, schemaName: string, tableName: string, @body body: LoadExternalRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/search")
@get
op searchCatalog(@query query: string, @query type?: string, @query catalog?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
op getTableLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
op getUpstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
op getDownstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/edges/{edgeId}")
@delete
op deleteLineageEdge(edgeId: string): NoContent | MutatingErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
op getColumnLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impact")
@get
op getColumnImpact(schemaName: string, tableName: string, columnName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/lineage/purge")
@post
op purgeLineage(@body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/tags")
@get
op listTags(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/tags")
@post
op createTag(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/tags/{tagId}")
@delete
op deleteTag(tagId: string): NoContent | MutatingErrorResponses;

@route("/tags/{tagId}/assignments")
@post
op createTagAssignment(tagId: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/tag-assignments/{assignmentId}")
@delete
op deleteTagAssignment(assignmentId: string): NoContent | MutatingErrorResponses;

@route("/classifications")
@get
op listClassifications(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/storage-credentials")
@get
op listStorageCredentials(@query max_results?: int32, @query page_token?: string): Ok<PaginatedStorageCredentials> | StandardErrorResponses;

@route("/storage-credentials")
@post
op createStorageCredential(@body body: CreateStorageCredentialRequest): Created<StorageCredential> | MutatingErrorResponses;

@route("/storage-credentials/{credentialName}")
@get
op getStorageCredential(credentialName: string): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credentialName}")
@patch(#{ implicitOptionality: true })
op updateStorageCredential(credentialName: string, @body body: UpdateStorageCredentialRequest): Ok<StorageCredential> | MutatingErrorResponses;

@route("/storage-credentials/{credentialName}")
@delete
op deleteStorageCredential(credentialName: string): NoContent | MutatingErrorResponses;

@route("/external-locations")
@get
op listExternalLocations(@query max_results?: int32, @query page_token?: string): Ok<PaginatedExternalLocations> | StandardErrorResponses;

@route("/external-locations")
@post
op createExternalLocation(@body body: CreateExternalLocationRequest): Created<ExternalLocation> | MutatingErrorResponses;

@route("/external-locations/{locationName}")
@get
op getExternalLocation(locationName: string): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{locationName}")
@patch(#{ implicitOptionality: true })
op updateExternalLocation(locationName: string, @body body: UpdateExternalLocationRequest): Ok<ExternalLocation> | MutatingErrorResponses;

@route("/external-locations/{locationName}")
@delete
op deleteExternalLocation(locationName: string): NoContent | MutatingErrorResponses;

@route("/compute-endpoints")
@get
op listComputeEndpoints(@query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeEndpoints> | StandardErrorResponses;

@route("/compute-endpoints")
@post
op createComputeEndpoint(@body body: CreateComputeEndpointRequest): Created<ComputeEndpoint> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}")
@get
op getComputeEndpoint(endpointName: string): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@patch(#{ implicitOptionality: true })
op updateComputeEndpoint(endpointName: string, @body body: UpdateComputeEndpointRequest): Ok<ComputeEndpoint> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}")
@delete
op deleteComputeEndpoint(endpointName: string): NoContent | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@get
op listComputeAssignments(endpointName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeAssignments> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@post
op createComputeAssignment(endpointName: string, @body body: CreateComputeAssignmentRequest): Created<ComputeAssignment> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}/health")
@get
op getComputeEndpointHealth(endpointName: string): Ok<ComputeEndpointHealth> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
op deleteComputeAssignment(endpointName: string, assignmentId: string): NoContent | MutatingErrorResponses;

@route("/notebooks")
@get
op listNotebooks(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/notebooks")
@post
op createNotebook(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@get
op getNotebook(notebookId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
op updateNotebook(notebookId: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@delete
op deleteNotebook(notebookId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells")
@post
op createCell(notebookId: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/reorder")
@post
op reorderCells(notebookId: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
op updateCell(notebookId: string, cellId: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
op deleteCell(notebookId: string, cellId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions")
@post
op createNotebookSession(notebookId: string): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
op closeNotebookSession(notebookId: string, sessionId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
op executeCell(notebookId: string, sessionId: string, cellId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
op runAllCells(notebookId: string, sessionId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
op runAllCellsAsync(notebookId: string, sessionId: string): Created<GenericResponse> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/jobs")
@get
op listNotebookJobs(notebookId: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
op getNotebookJob(notebookId: string, jobId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/git-repos")
@get
op listGitRepos(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/git-repos")
@post
op createGitRepo(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}")
@get
op getGitRepo(gitRepoId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}")
@delete
op deleteGitRepo(gitRepoId: string): NoContent | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}/sync")
@post
op syncGitRepo(gitRepoId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines")
@get
op listPipelines(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/pipelines")
@post
op createPipeline(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@get
op getPipeline(pipelineName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}")
@patch(#{ implicitOptionality: true })
op updatePipeline(pipelineName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@delete
op deletePipeline(pipelineName: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@get
op listPipelineJobs(pipelineName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@post
op createPipelineJob(pipelineName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs/{jobId}")
@delete
op deletePipelineJob(pipelineName: string, jobId: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@post
op triggerPipelineRun(pipelineName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@get
op listPipelineRuns(pipelineName: string, @query max_results?: int32, @query page_token?: string, @query status?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}")
@get
op getPipelineRun(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}/cancel")
@post
op cancelPipelineRun(runId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/pipelines/runs/{runId}/jobs")
@get
op listPipelineJobRuns(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models")
@get
op listModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/models")
@post
op createModel(@body body: CreateModelRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@get
op getModel(projectName: string, modelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
op updateModel(projectName: string, modelName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@delete
op deleteModel(projectName: string, modelName: string): NoContent | MutatingErrorResponses;

@route("/models/dag")
@get
op getModelDAG(@query project_name?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/model-runs")
@post
op triggerModelRun(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/model-runs")
@get
op listModelRuns(@query status?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/model-runs/{runId}")
@get
op getModelRun(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/model-runs/{runId}/steps")
@get
op listModelRunSteps(runId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@post
op createModelTest(projectName: string, modelName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@get
op listModelTests(projectName: string, modelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
op deleteModelTest(projectName: string, modelName: string, testId: string): NoContent | MutatingErrorResponses;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
op listModelTestResults(runId: string, stepId: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/freshness")
@get
op checkModelFreshness(projectName: string, modelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
op checkSourceFreshness(sourceSchema: string, sourceTable: string, @query timestamp_column?: string, @query max_lag_seconds?: int64): Ok<GenericResponse> | ResourceErrorResponses;

@route("/models/from-notebook")
@post
op promoteNotebookToModel(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/model-runs/{runId}/cancel")
@post
op cancelModelRun(runId: string): Ok<GenericResponse> | MutatingErrorResponses;

@route("/macros")
@get
op listMacros(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/macros")
@post
op createMacro(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/macros/{macroName}")
@get
op getMacro(macroName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
op updateMacro(macroName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/macros/{macroName}")
@delete
op deleteMacro(macroName: string): NoContent | MutatingErrorResponses;

@route("/macros/{macroName}/revisions")
@get
op listMacroRevisions(macroName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/macros/{macroName}/impact")
@get
op getMacroImpact(macroName: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/macros/{macroName}/diff")
@get
op diffMacroRevisions(macroName: string, @query from_version: int32, @query to_version: int32): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models")
@get
op listSemanticModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/semantic-models")
@post
op createSemanticModel(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@get
op getSemanticModel(projectName: string, semanticModelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@patch(#{ implicitOptionality: true })
op updateSemanticModel(projectName: string, semanticModelName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@delete
op deleteSemanticModel(projectName: string, semanticModelName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@get
op listSemanticMetrics(projectName: string, semanticModelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@post
op createSemanticMetric(projectName: string, semanticModelName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
op updateSemanticMetric(projectName: string, semanticModelName: string, metricName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@delete
op deleteSemanticMetric(projectName: string, semanticModelName: string, metricName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@get
op listSemanticPreAggregations(projectName: string, semanticModelName: string): Ok<GenericResponse> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@post
op createSemanticPreAggregation(projectName: string, semanticModelName: string, @body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
op updateSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@delete
op deleteSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string): NoContent | MutatingErrorResponses;

@route("/semantic-relationships")
@get
op listSemanticRelationships(@query max_results?: int32, @query page_token?: string): Ok<GenericResponse> | StandardErrorResponses;

@route("/semantic-relationships")
@post
op createSemanticRelationship(@body body: GenericRequest): Created<GenericResponse> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
op updateSemanticRelationship(relationshipName: string, @body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@delete
op deleteSemanticRelationship(relationshipName: string): NoContent | MutatingErrorResponses;

@route("/metric-queries:explain")
@post
op explainMetricQuery(@body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/metric-queries:run")
@post
op runMetricQuery(@body body: GenericRequest): Ok<GenericResponse> | MutatingErrorResponses;

@route("/metrics/{metricName}/freshness")
@get
op checkMetricFreshness(metricName: string, @query project_name?: string, @query semantic_model_name?: string): Ok<GenericResponse> | ResourceErrorResponses;
