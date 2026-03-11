using TypeSpec.Http;

namespace Duck;

@route("/queries")
@post
op submitQuery(@body body: SubmitQueryRequest): Accepted<SubmitQueryResponse> | MutatingErrorResponses;

@route("/queries/{queryId}")
@get
op getQuery(queryId: string): Ok<QueryJob> | ResourceErrorResponses;

@route("/queries/{queryId}")
@delete
op deleteQuery(queryId: string): NoContent | MutatingErrorResponses;

@route("/queries/{queryId}/results")
@get
op getQueryResults(queryId: string, @query max_results?: int32, @query page_token?: string): Ok<QueryResult> | ResourceErrorResponses;

@route("/queries/{queryId}/cancel")
@post
op cancelQuery(queryId: string): Accepted<CancelQueryResponse> | ResourceErrorResponses;

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
op searchCatalog(@query query: string, @query type?: string, @query catalog?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSearchResults> | StandardErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
op getTableLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<LineageNode> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
op getUpstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
op getDownstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/edges/{edgeId}")
@delete
op deleteLineageEdge(edgeId: string): NoContent | MutatingErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
op getColumnLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impact")
@get
op getColumnImpact(schemaName: string, tableName: string, columnName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/purge")
@post
op purgeLineage(@body body: PurgeLineageRequest): Ok<PurgeLineageResponse> | MutatingErrorResponses;

@route("/tags")
@get
op listTags(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;

@route("/tags")
@post
op createTag(@body body: CreateTagRequest): Created<Tag> | MutatingErrorResponses;

@route("/tags/{tagId}")
@delete
op deleteTag(tagId: string): NoContent | MutatingErrorResponses;

@route("/tags/{tagId}/assignments")
@get
op listTagAssignments(
  tagId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedTagAssignments> | ResourceErrorResponses;

@route("/tags/{tagId}/assignments")
@post
op createTagAssignment(tagId: string, @body body: CreateTagAssignmentRequest): Created<TagAssignment> | MutatingErrorResponses;

@route("/tag-assignments/{assignmentId}")
@delete
op deleteTagAssignment(assignmentId: string): NoContent | MutatingErrorResponses;

@route("/classifications")
@get
op listClassifications(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;

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
@doc("Creates a new external location and configures DuckDB with the associated credential. Catalog registrations are managed separately; creating a location does not attach or create a DuckLake catalog.")
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
op getComputeEndpointHealth(endpointName: string): Ok<ComputeEndpointHealth> | BadRequestResponse | ResourceErrorResponses | BadGatewayResponse;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
op deleteComputeAssignment(endpointName: string, assignmentId: string): NoContent | MutatingErrorResponses;

@route("/notebooks")
@get
op listNotebooks(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebooks> | StandardErrorResponses;

@route("/notebooks")
@post
op createNotebook(@body body: CreateNotebookRequest): Created<Notebook> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@get
op getNotebook(notebookId: string): Ok<NotebookDetail> | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
op updateNotebook(notebookId: string, @body body: UpdateNotebookRequest): Ok<Notebook> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@delete
op deleteNotebook(notebookId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells")
@post
op createCell(notebookId: string, @body body: CreateCellRequest): Created<Cell> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/reorder")
@post
op reorderCells(notebookId: string, @body body: ReorderCellsRequest): Ok<CellList> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
op updateCell(notebookId: string, cellId: string, @body body: UpdateCellRequest): Ok<Cell> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
op deleteCell(notebookId: string, cellId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions")
@post
op createNotebookSession(notebookId: string): Created<NotebookSession> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
op closeNotebookSession(notebookId: string, sessionId: string): NoContent | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
op executeCell(notebookId: string, sessionId: string, cellId: string): Ok<CellExecutionResult> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
op runAllCells(notebookId: string, sessionId: string): Ok<RunAllResult> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
op runAllCellsAsync(notebookId: string, sessionId: string): Accepted<NotebookJob> | MutatingErrorResponses;

@route("/notebooks/{notebookId}/jobs")
@get
op listNotebookJobs(notebookId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebookJobs> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
op getNotebookJob(notebookId: string, jobId: string): Ok<NotebookJob> | ResourceErrorResponses;

@route("/dashboards")
@get
op listDashboards(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedDashboards> | StandardErrorResponses;

@route("/dashboards")
@post
op createDashboard(@body body: CreateDashboardRequest): Created<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}")
@get
op getDashboard(dashboardId: string): Ok<DashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboardId}/resolved")
@get
op getResolvedDashboard(dashboardId: string): Ok<ResolvedDashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboardId}")
@patch(#{ implicitOptionality: true })
op updateDashboard(dashboardId: string, @body body: UpdateDashboardRequest): Ok<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}")
@delete
op deleteDashboard(dashboardId: string): NoContent | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets")
@post
op createDashboardWidget(dashboardId: string, @body body: CreateDashboardWidgetRequest): Created<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets/{widgetId}")
@patch(#{ implicitOptionality: true })
op updateDashboardWidget(dashboardId: string, widgetId: string, @body body: UpdateDashboardWidgetRequest): Ok<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets/{widgetId}")
@delete
op deleteDashboardWidget(dashboardId: string, widgetId: string): NoContent | MutatingErrorResponses;

@route("/git-repos")
@get
op listGitRepos(@query max_results?: int32, @query page_token?: string): Ok<PaginatedGitRepos> | StandardErrorResponses;

@route("/git-repos")
@post
op createGitRepo(@body body: CreateGitRepoRequest): Created<GitRepo> | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}")
@get
op getGitRepo(gitRepoId: string): Ok<GitRepo> | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}")
@delete
op deleteGitRepo(gitRepoId: string): NoContent | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}/sync")
@post
op syncGitRepo(gitRepoId: string): Ok<GitSyncResult> | MutatingErrorResponses;

@route("/pipelines")
@get
op listPipelines(@query max_results?: int32, @query page_token?: string): Ok<PaginatedPipelines> | StandardErrorResponses;

@route("/pipelines")
@post
op createPipeline(@body body: CreatePipelineRequest): Created<Pipeline> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@get
op getPipeline(pipelineName: string): Ok<Pipeline> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}")
@patch(#{ implicitOptionality: true })
op updatePipeline(pipelineName: string, @body body: UpdatePipelineRequest): Ok<Pipeline> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}")
@delete
op deletePipeline(pipelineName: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@get
op listPipelineJobs(pipelineName: string): Ok<PipelineJobList> | ResourceErrorResponses;

@route("/pipelines/{pipelineName}/jobs")
@post
op createPipelineJob(pipelineName: string, @body body: CreatePipelineJobRequest): Created<PipelineJob> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/jobs/{jobId}")
@delete
op deletePipelineJob(pipelineName: string, jobId: string): NoContent | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@post
op triggerPipelineRun(pipelineName: string, @body body?: TriggerPipelineRunRequest): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/{pipelineName}/runs")
@get
op listPipelineRuns(pipelineName: string, @query max_results?: int32, @query page_token?: string, @query status?: string): Ok<PaginatedPipelineRuns> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}")
@get
op getPipelineRun(runId: string): Ok<PipelineRun> | ResourceErrorResponses;

@route("/pipelines/runs/{runId}/cancel")
@post
op cancelPipelineRun(runId: string): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/runs/{runId}/jobs")
@get
op listPipelineJobRuns(runId: string): Ok<PipelineJobRunList> | ResourceErrorResponses;

@route("/models")
@get
op listModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedModels> | StandardErrorResponses;

@route("/models")
@post
op createModel(@body body: CreateModelRequest): Created<Model> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@get
op getModel(projectName: string, modelName: string): Ok<Model> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
op updateModel(projectName: string, modelName: string, @body body: UpdateModelRequest): Ok<Model> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@delete
op deleteModel(projectName: string, modelName: string): NoContent | MutatingErrorResponses;

@route("/models/dag")
@get
op getModelDAG(@query project_name?: string): Ok<ModelDAG> | StandardErrorResponses;

@route("/model-runs")
@post
op triggerModelRun(@body body: TriggerModelRunRequest): Created<ModelRun> | MutatingErrorResponses;

@route("/model-runs")
@get
op listModelRuns(@query status?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedModelRuns> | StandardErrorResponses;

@route("/model-runs/{runId}")
@get
op getModelRun(runId: string): Ok<ModelRun> | ResourceErrorResponses;

@route("/model-runs/{runId}/steps")
@get
op listModelRunSteps(runId: string): Ok<ModelRunStepList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@post
op createModelTest(projectName: string, modelName: string, @body body: CreateModelTestRequest): Created<ModelTest> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@get
op listModelTests(projectName: string, modelName: string): Ok<ModelTestList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
op deleteModelTest(projectName: string, modelName: string, testId: string): NoContent | MutatingErrorResponses;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
op listModelTestResults(runId: string, stepId: string): Ok<ModelTestResultList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/freshness")
@get
op checkModelFreshness(projectName: string, modelName: string): Ok<FreshnessStatus> | ResourceErrorResponses;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
op checkSourceFreshness(sourceSchema: string, sourceTable: string, @query timestamp_column?: string, @query max_lag_seconds?: int64): Ok<SourceFreshnessStatus> | ResourceErrorResponses;

@route("/models/from-notebook")
@post
op promoteNotebookToModel(@body body: PromoteNotebookRequest): Created<Model> | MutatingErrorResponses;

@route("/model-runs/{runId}/cancel")
@post
op cancelModelRun(runId: string): Ok<ModelRun> | MutatingErrorResponses;

@route("/macros")
@get
op listMacros(@query max_results?: int32, @query page_token?: string): Ok<PaginatedMacros> | StandardErrorResponses;

@route("/macros")
@post
op createMacro(@body body: CreateMacroRequest): Created<Macro> | MutatingErrorResponses;

@route("/macros/{macroName}")
@get
op getMacro(macroName: string): Ok<Macro> | ResourceErrorResponses;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
op updateMacro(macroName: string, @body body: UpdateMacroRequest): Ok<Macro> | MutatingErrorResponses;

@route("/macros/{macroName}")
@delete
op deleteMacro(macroName: string): NoContent | MutatingErrorResponses;

@route("/macros/{macroName}/revisions")
@get
op listMacroRevisions(macroName: string): Ok<MacroRevisionList> | ResourceErrorResponses;

@route("/macros/{macroName}/impact")
@get
op getMacroImpact(macroName: string, @query max_results?: int32, @query page_token?: string): Ok<MacroImpactList> | ResourceErrorResponses;

@route("/macros/{macroName}/diff")
@get
op diffMacroRevisions(macroName: string, @query from_version: int32, @query to_version: int32): Ok<MacroRevisionDiff> | ResourceErrorResponses;

@route("/semantic-models")
@get
op listSemanticModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSemanticModels> | StandardErrorResponses;

@route("/semantic-models")
@post
op createSemanticModel(@body body: CreateSemanticModelRequest): Created<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@get
op getSemanticModel(projectName: string, semanticModelName: string): Ok<SemanticModel> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@patch(#{ implicitOptionality: true })
op updateSemanticModel(projectName: string, semanticModelName: string, @body body: UpdateSemanticModelRequest): Ok<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@delete
op deleteSemanticModel(projectName: string, semanticModelName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@get
op listSemanticMetrics(projectName: string, semanticModelName: string): Ok<SemanticMetricList> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@post
op createSemanticMetric(projectName: string, semanticModelName: string, @body body: CreateSemanticMetricRequest): Created<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
op updateSemanticMetric(projectName: string, semanticModelName: string, metricName: string, @body body: UpdateSemanticMetricRequest): Ok<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@delete
op deleteSemanticMetric(projectName: string, semanticModelName: string, metricName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@get
op listSemanticPreAggregations(projectName: string, semanticModelName: string): Ok<SemanticPreAggregationList> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@post
op createSemanticPreAggregation(projectName: string, semanticModelName: string, @body body: CreateSemanticPreAggregationRequest): Created<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
op updateSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string, @body body: UpdateSemanticPreAggregationRequest): Ok<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@delete
op deleteSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string): NoContent | MutatingErrorResponses;

@route("/semantic-relationships")
@get
op listSemanticRelationships(@query max_results?: int32, @query page_token?: string): Ok<PaginatedSemanticRelationships> | StandardErrorResponses;

@route("/semantic-relationships")
@post
op createSemanticRelationship(@body body: CreateSemanticRelationshipRequest): Created<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
op updateSemanticRelationship(relationshipName: string, @body body: UpdateSemanticRelationshipRequest): Ok<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@delete
op deleteSemanticRelationship(relationshipName: string): NoContent | MutatingErrorResponses;

@route("/metric-queries:explain")
@post
op explainMetricQuery(@body body: MetricQueryRequest): Ok<MetricQueryExplainResponse> | MutatingErrorResponses;

@route("/metric-queries:run")
@post
op runMetricQuery(@body body: MetricQueryRequest): Ok<MetricQueryRunResponse> | MutatingErrorResponses;

@route("/metrics/{metricName}/freshness")
@get
op checkMetricFreshness(metricName: string, @query project_name?: string, @query semantic_model_name?: string): Ok<MetricFreshnessStatus> | ResourceErrorResponses;
