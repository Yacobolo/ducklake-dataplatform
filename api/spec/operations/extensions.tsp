using TypeSpec.Http;

namespace Duck;

@route("/queries")
@post
@tag("Queries")
@summary("Submit query")
@doc("Submits a SQL query for asynchronous execution and returns a query job identifier for polling and result retrieval.")
op submitQuery(@body body: SubmitQueryRequest): Accepted<SubmitQueryResponse> | MutatingErrorResponses;

@route("/queries/{queryId}")
@get
@tag("Queries")
@summary("Get query")
op getQuery(queryId: string): Ok<QueryJob> | ResourceErrorResponses;

@route("/queries/{queryId}")
@delete
@tag("Queries")
@summary("Delete query")
op deleteQuery(queryId: string): NoContent | MutatingErrorResponses;

@route("/queries/{queryId}/results")
@get
@tag("Queries")
@summary("Get query results")
@doc("Returns a page of rows for a previously submitted query using the stored query job identifier.")
op getQueryResults(queryId: string, @query max_results?: int32, @query page_token?: string): Ok<QueryResult> | ResourceErrorResponses;

@route("/queries/{queryId}/cancel")
@post
@tag("Queries")
@summary("Cancel query")
op cancelQuery(queryId: string): Accepted<CancelQueryResponse> | ResourceErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/upload-url")
@post
@tag("Catalogs")
@summary("Create upload URL")
op createUploadUrl(catalogName: string, schemaName: string, tableName: string, @body body: UploadUrlRequest): Ok<UploadUrlResponse> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/commit")
@post
@tag("Catalogs")
@summary("Commit table ingestion")
op commitTableIngestion(catalogName: string, schemaName: string, tableName: string, @body body: CommitIngestionRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/load")
@post
@tag("Catalogs")
@summary("Load table external files")
op loadTableExternalFiles(catalogName: string, schemaName: string, tableName: string, @body body: LoadExternalRequest): Ok<IngestionResult> | MutatingErrorResponses;

@route("/search")
@get
@tag("Queries")
@summary("Search catalog")
op searchCatalog(@query query: string, @query type?: string, @query catalog?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSearchResults> | StandardErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
@tag("Lineage")
@summary("Get table lineage")
op getTableLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<LineageNode> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
@tag("Lineage")
@summary("Get upstream lineage")
op getUpstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
@tag("Lineage")
@summary("Get downstream lineage")
op getDownstreamLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedLineageEdges> | ResourceErrorResponses;

@route("/lineage/edges/{edgeId}")
@delete
@tag("Lineage")
@summary("Delete lineage edge")
op deleteLineageEdge(edgeId: string): NoContent | MutatingErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
@tag("Lineage")
@summary("Get column lineage")
op getColumnLineage(schemaName: string, tableName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impact")
@get
@tag("Lineage")
@summary("Get column impact")
op getColumnImpact(schemaName: string, tableName: string, columnName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedColumnLineageEdges> | ResourceErrorResponses;

@route("/lineage/purge")
@post
@tag("Lineage")
@summary("Purge lineage")
op purgeLineage(@body body: PurgeLineageRequest): Ok<PurgeLineageResponse> | MutatingErrorResponses;

@route("/tags")
@get
@tag("Governance")
@summary("List tags")
op listTags(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;

@route("/tags")
@post
@tag("Governance")
@summary("Create tag")
op createTag(@body body: CreateTagRequest): Created<Tag> | MutatingErrorResponses;

@route("/tags/{tagId}")
@delete
@tag("Governance")
@summary("Delete tag")
op deleteTag(tagId: string): NoContent | MutatingErrorResponses;

@route("/tags/{tagId}/assignments")
@get
@tag("Governance")
@summary("List tag assignments")
op listTagAssignments(
  tagId: string,
  @query max_results?: int32,
  @query page_token?: string,
): Ok<PaginatedTagAssignments> | ResourceErrorResponses;

@route("/tags/{tagId}/assignments")
@post
@tag("Governance")
@summary("Create tag assignment")
op createTagAssignment(tagId: string, @body body: CreateTagAssignmentRequest): Created<TagAssignment> | MutatingErrorResponses;

@route("/tag-assignments/{assignmentId}")
@delete
@tag("Governance")
@summary("Delete tag assignment")
op deleteTagAssignment(assignmentId: string): NoContent | MutatingErrorResponses;

@route("/classifications")
@get
@tag("Governance")
@summary("List classifications")
op listClassifications(@query max_results?: int32, @query page_token?: string): Ok<PaginatedTags> | StandardErrorResponses;

@route("/storage-credentials")
@get
@tag("Storage")
@summary("List storage credentials")
op listStorageCredentials(@query max_results?: int32, @query page_token?: string): Ok<PaginatedStorageCredentials> | StandardErrorResponses;

@route("/storage-credentials")
@post
@tag("Storage")
@summary("Create storage credential")
@doc("Creates a reusable storage credential that can be referenced by external locations and managed catalogs.")
op createStorageCredential(@body body: CreateStorageCredentialRequest): Created<StorageCredential> | MutatingErrorResponses;

@route("/storage-credentials/{credentialName}")
@get
@tag("Storage")
@summary("Get storage credential")
op getStorageCredential(credentialName: string): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credentialName}")
@patch(#{ implicitOptionality: true })
@tag("Storage")
@summary("Update storage credential")
op updateStorageCredential(credentialName: string, @body body: UpdateStorageCredentialRequest): Ok<StorageCredential> | ResourceErrorResponses;

@route("/storage-credentials/{credentialName}")
@delete
@tag("Storage")
@summary("Delete storage credential")
op deleteStorageCredential(credentialName: string): NoContent | ResourceErrorResponses;

@route("/external-locations")
@get
@tag("Storage")
@summary("List external locations")
op listExternalLocations(@query max_results?: int32, @query page_token?: string): Ok<PaginatedExternalLocations> | StandardErrorResponses;

@route("/external-locations")
@post
@doc("Creates a new external location and configures DuckDB with the associated credential. Catalog registrations are managed separately; creating a location does not attach or create a DuckLake catalog.")
@tag("Storage")
@summary("Create external location")
op createExternalLocation(@body body: CreateExternalLocationRequest): Created<ExternalLocation> | MutatingErrorResponses;

@route("/external-locations/{locationName}")
@get
@tag("Storage")
@summary("Get external location")
op getExternalLocation(locationName: string): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{locationName}")
@patch(#{ implicitOptionality: true })
@tag("Storage")
@summary("Update external location")
op updateExternalLocation(locationName: string, @body body: UpdateExternalLocationRequest): Ok<ExternalLocation> | ResourceErrorResponses;

@route("/external-locations/{locationName}")
@delete
@tag("Storage")
@summary("Delete external location")
op deleteExternalLocation(locationName: string): NoContent | ResourceErrorResponses;

@route("/compute-endpoints")
@get
@tag("Compute")
@summary("List compute endpoints")
op listComputeEndpoints(@query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeEndpoints> | StandardErrorResponses;

@route("/compute-endpoints")
@post
@tag("Compute")
@summary("Create compute endpoint")
@doc("Registers a compute endpoint that can execute remote workloads and accept assignment-based routing.")
op createComputeEndpoint(@body body: CreateComputeEndpointRequest): Created<ComputeEndpoint> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}")
@get
@tag("Compute")
@summary("Get compute endpoint")
op getComputeEndpoint(endpointName: string): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@patch(#{ implicitOptionality: true })
@tag("Compute")
@summary("Update compute endpoint")
op updateComputeEndpoint(endpointName: string, @body body: UpdateComputeEndpointRequest): Ok<ComputeEndpoint> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}")
@delete
@tag("Compute")
@summary("Delete compute endpoint")
op deleteComputeEndpoint(endpointName: string): NoContent | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@get
@tag("Compute")
@summary("List compute assignments")
op listComputeAssignments(endpointName: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedComputeAssignments> | ResourceErrorResponses;

@route("/compute-endpoints/{endpointName}/assignments")
@post
@tag("Compute")
@summary("Create compute assignment")
op createComputeAssignment(endpointName: string, @body body: CreateComputeAssignmentRequest): Created<ComputeAssignment> | MutatingErrorResponses;

@route("/compute-endpoints/{endpointName}/health")
@get
@tag("Compute")
@summary("Get compute endpoint health")
op getComputeEndpointHealth(endpointName: string): Ok<ComputeEndpointHealth> | BadRequestResponse | ResourceErrorResponses | BadGatewayResponse;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
@tag("Compute")
@summary("Delete compute assignment")
op deleteComputeAssignment(endpointName: string, assignmentId: string): NoContent | ResourceErrorResponses;

@route("/notebooks")
@get
@tag("Notebooks")
@summary("List notebooks")
op listNotebooks(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebooks> | StandardErrorResponses;

@route("/notebooks")
@post
@tag("Notebooks")
@summary("Create notebook")
op createNotebook(@body body: CreateNotebookRequest): Created<Notebook> | MutatingErrorResponses;

@route("/notebooks/{notebookId}")
@get
@tag("Notebooks")
@summary("Get notebook")
op getNotebook(notebookId: string): Ok<NotebookDetail> | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update notebook")
op updateNotebook(notebookId: string, @body body: UpdateNotebookRequest): Ok<Notebook> | ResourceErrorResponses;

@route("/notebooks/{notebookId}")
@delete
@tag("Notebooks")
@summary("Delete notebook")
op deleteNotebook(notebookId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}/cells")
@post
@tag("Notebooks")
@summary("Create cell")
op createCell(notebookId: string, @body body: CreateCellRequest): Created<Cell> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/cells/reorder")
@post
@tag("Notebooks")
@summary("Reorder cells")
op reorderCells(notebookId: string, @body body: ReorderCellsRequest): Ok<CellList> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
@tag("Notebooks")
@summary("Update cell")
op updateCell(notebookId: string, cellId: string, @body body: UpdateCellRequest): Ok<Cell> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
@tag("Notebooks")
@summary("Delete cell")
op deleteCell(notebookId: string, cellId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions")
@post
@tag("Notebooks")
@summary("Create notebook session")
op createNotebookSession(notebookId: string): Created<NotebookSession> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
@tag("Notebooks")
@summary("Close notebook session")
op closeNotebookSession(notebookId: string, sessionId: string): NoContent | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
@tag("Notebooks")
@summary("Execute cell")
op executeCell(notebookId: string, sessionId: string, cellId: string): Ok<CellExecutionResult> | ValidationResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
@tag("Notebooks")
@summary("Run all cells")
op runAllCells(notebookId: string, sessionId: string): Ok<RunAllResult> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
@tag("Notebooks")
@summary("Run all cells asynchronously")
op runAllCellsAsync(notebookId: string, sessionId: string): Accepted<NotebookJob> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs")
@get
@tag("Notebooks")
@summary("List notebook jobs")
op listNotebookJobs(notebookId: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedNotebookJobs> | ResourceErrorResponses;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
@tag("Notebooks")
@summary("Get notebook job")
op getNotebookJob(notebookId: string, jobId: string): Ok<NotebookJob> | ResourceErrorResponses;

@route("/dashboards")
@get
@tag("Dashboards")
@summary("List dashboards")
op listDashboards(@query owner?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedDashboards> | StandardErrorResponses;

@route("/dashboards")
@post
@tag("Dashboards")
@summary("Create dashboard")
op createDashboard(@body body: CreateDashboardRequest): Created<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}")
@get
@tag("Dashboards")
@summary("Get dashboard")
op getDashboard(dashboardId: string): Ok<DashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboardId}/resolved")
@get
@tag("Dashboards")
@summary("Get resolved dashboard")
op getResolvedDashboard(dashboardId: string): Ok<ResolvedDashboardDetail> | ResourceErrorResponses;

@route("/dashboards/{dashboardId}")
@patch(#{ implicitOptionality: true })
@tag("Dashboards")
@summary("Update dashboard")
op updateDashboard(dashboardId: string, @body body: UpdateDashboardRequest): Ok<Dashboard> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}")
@delete
@tag("Dashboards")
@summary("Delete dashboard")
op deleteDashboard(dashboardId: string): NoContent | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets")
@post
@tag("Dashboards")
@summary("Create dashboard widget")
op createDashboardWidget(dashboardId: string, @body body: CreateDashboardWidgetRequest): Created<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets/{widgetId}")
@patch(#{ implicitOptionality: true })
@tag("Dashboards")
@summary("Update dashboard widget")
op updateDashboardWidget(dashboardId: string, widgetId: string, @body body: UpdateDashboardWidgetRequest): Ok<DashboardWidget> | MutatingErrorResponses;

@route("/dashboards/{dashboardId}/widgets/{widgetId}")
@delete
@tag("Dashboards")
@summary("Delete dashboard widget")
op deleteDashboardWidget(dashboardId: string, widgetId: string): NoContent | MutatingErrorResponses;

@route("/git-repos")
@get
@tag("Integrations")
@summary("List git repos")
op listGitRepos(@query max_results?: int32, @query page_token?: string): Ok<PaginatedGitRepos> | StandardErrorResponses;

@route("/git-repos")
@post
@tag("Integrations")
@summary("Create git repo")
op createGitRepo(@body body: CreateGitRepoRequest): Created<GitRepo> | MutatingErrorResponses;

@route("/git-repos/{gitRepoId}")
@get
@tag("Integrations")
@summary("Get git repo")
op getGitRepo(gitRepoId: string): Ok<GitRepo> | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}")
@delete
@tag("Integrations")
@summary("Delete git repo")
op deleteGitRepo(gitRepoId: string): NoContent | ResourceErrorResponses;

@route("/git-repos/{gitRepoId}/sync")
@post
@tag("Integrations")
@summary("Sync git repo")
op syncGitRepo(gitRepoId: string): Ok<GitSyncResult> | ResourceErrorResponses | NotImplementedResponse;

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

@route("/pipelines/runs/{runId}/cancel")
@post
@tag("Pipelines")
@summary("Cancel pipeline run")
op cancelPipelineRun(runId: string): Ok<PipelineRun> | MutatingErrorResponses;

@route("/pipelines/runs/{runId}/jobs")
@get
@tag("Pipelines")
@summary("List pipeline job runs")
op listPipelineJobRuns(runId: string): Ok<PipelineJobRunList> | ResourceErrorResponses;

@route("/models")
@get
@tag("Models")
@summary("List models")
op listModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedModels> | StandardErrorResponses;

@route("/models")
@post
@tag("Models")
@summary("Create model")
op createModel(@body body: CreateModelRequest): Created<Model> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@get
@tag("Models")
@summary("Get model")
op getModel(projectName: string, modelName: string): Ok<Model> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
@tag("Models")
@summary("Update model")
op updateModel(projectName: string, modelName: string, @body body: UpdateModelRequest): Ok<Model> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@delete
@tag("Models")
@summary("Delete model")
op deleteModel(projectName: string, modelName: string): NoContent | MutatingErrorResponses;

@route("/models/dag")
@get
@tag("Models")
@summary("Get model DAG")
op getModelDAG(@query project_name?: string): Ok<ModelDAG> | StandardErrorResponses;

@route("/model-runs")
@post
@tag("Models")
@summary("Trigger model run")
op triggerModelRun(@body body: TriggerModelRunRequest): Created<ModelRun> | MutatingErrorResponses;

@route("/model-runs")
@get
@tag("Models")
@summary("List model runs")
op listModelRuns(@query status?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedModelRuns> | StandardErrorResponses;

@route("/model-runs/{runId}")
@get
@tag("Models")
@summary("Get model run")
op getModelRun(runId: string): Ok<ModelRun> | ResourceErrorResponses;

@route("/model-runs/{runId}/steps")
@get
@tag("Models")
@summary("List model run steps")
op listModelRunSteps(runId: string): Ok<ModelRunStepList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@post
@tag("Models")
@summary("Create model test")
op createModelTest(projectName: string, modelName: string, @body body: CreateModelTestRequest): Created<ModelTest> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@get
@tag("Models")
@summary("List model tests")
op listModelTests(projectName: string, modelName: string): Ok<ModelTestList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
@tag("Models")
@summary("Delete model test")
op deleteModelTest(projectName: string, modelName: string, testId: string): NoContent | MutatingErrorResponses;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
@tag("Models")
@summary("List model test results")
op listModelTestResults(runId: string, stepId: string): Ok<ModelTestResultList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/freshness")
@get
@tag("Models")
@summary("Check model freshness")
op checkModelFreshness(projectName: string, modelName: string): Ok<FreshnessStatus> | ResourceErrorResponses;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
@tag("Semantic Layer")
@summary("Check source freshness")
op checkSourceFreshness(sourceSchema: string, sourceTable: string, @query timestamp_column?: string, @query max_lag_seconds?: int64): Ok<SourceFreshnessStatus> | ResourceErrorResponses;

@route("/models/from-notebook")
@post
@tag("Models")
@summary("Promote notebook to model")
op promoteNotebookToModel(@body body: PromoteNotebookRequest): Created<Model> | MutatingErrorResponses;

@route("/model-runs/{runId}/cancel")
@post
@tag("Models")
@summary("Cancel model run")
op cancelModelRun(runId: string): Ok<ModelRun> | MutatingErrorResponses;

@route("/macros")
@get
@tag("Models")
@summary("List macros")
op listMacros(@query max_results?: int32, @query page_token?: string): Ok<PaginatedMacros> | StandardErrorResponses;

@route("/macros")
@post
@tag("Models")
@summary("Create macro")
op createMacro(@body body: CreateMacroRequest): Created<Macro> | MutatingErrorResponses;

@route("/macros/{macroName}")
@get
@tag("Models")
@summary("Get macro")
op getMacro(macroName: string): Ok<Macro> | ResourceErrorResponses;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
@tag("Models")
@summary("Update macro")
op updateMacro(macroName: string, @body body: UpdateMacroRequest): Ok<Macro> | MutatingErrorResponses;

@route("/macros/{macroName}")
@delete
@tag("Models")
@summary("Delete macro")
op deleteMacro(macroName: string): NoContent | MutatingErrorResponses;

@route("/macros/{macroName}/revisions")
@get
@tag("Models")
@summary("List macro revisions")
op listMacroRevisions(macroName: string): Ok<MacroRevisionList> | ResourceErrorResponses;

@route("/macros/{macroName}/impact")
@get
@tag("Models")
@summary("Get macro impact")
op getMacroImpact(macroName: string, @query max_results?: int32, @query page_token?: string): Ok<MacroImpactList> | ResourceErrorResponses;

@route("/macros/{macroName}/diff")
@get
@tag("Models")
@summary("Diff macro revisions")
op diffMacroRevisions(macroName: string, @query from_version: int32, @query to_version: int32): Ok<MacroRevisionDiff> | ResourceErrorResponses;

@route("/semantic-models")
@get
@tag("Semantic Layer")
@summary("List semantic models")
op listSemanticModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedSemanticModels> | StandardErrorResponses;

@route("/semantic-models")
@post
@tag("Semantic Layer")
@summary("Create semantic model")
op createSemanticModel(@body body: CreateSemanticModelRequest): Created<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@get
@tag("Semantic Layer")
@summary("Get semantic model")
op getSemanticModel(projectName: string, semanticModelName: string): Ok<SemanticModel> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic model")
op updateSemanticModel(projectName: string, semanticModelName: string, @body body: UpdateSemanticModelRequest): Ok<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic model")
op deleteSemanticModel(projectName: string, semanticModelName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@get
@tag("Semantic Layer")
@summary("List semantic metrics")
op listSemanticMetrics(projectName: string, semanticModelName: string): Ok<SemanticMetricList> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@post
@tag("Semantic Layer")
@summary("Create semantic metric")
op createSemanticMetric(projectName: string, semanticModelName: string, @body body: CreateSemanticMetricRequest): Created<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic metric")
op updateSemanticMetric(projectName: string, semanticModelName: string, metricName: string, @body body: UpdateSemanticMetricRequest): Ok<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic metric")
op deleteSemanticMetric(projectName: string, semanticModelName: string, metricName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@get
@tag("Semantic Layer")
@summary("List semantic pre aggregations")
op listSemanticPreAggregations(projectName: string, semanticModelName: string): Ok<SemanticPreAggregationList> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@post
@tag("Semantic Layer")
@summary("Create semantic pre aggregation")
op createSemanticPreAggregation(projectName: string, semanticModelName: string, @body body: CreateSemanticPreAggregationRequest): Created<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic pre aggregation")
op updateSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string, @body body: UpdateSemanticPreAggregationRequest): Ok<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic pre aggregation")
op deleteSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string): NoContent | MutatingErrorResponses;

@route("/semantic-relationships")
@get
@tag("Semantic Layer")
@summary("List semantic relationships")
op listSemanticRelationships(@query max_results?: int32, @query page_token?: string): Ok<PaginatedSemanticRelationships> | StandardErrorResponses;

@route("/semantic-relationships")
@post
@tag("Semantic Layer")
@summary("Create semantic relationship")
op createSemanticRelationship(@body body: CreateSemanticRelationshipRequest): Created<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic relationship")
op updateSemanticRelationship(relationshipName: string, @body body: UpdateSemanticRelationshipRequest): Ok<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-relationships/{relationshipName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic relationship")
op deleteSemanticRelationship(relationshipName: string): NoContent | MutatingErrorResponses;

@route("/metric-queries:explain")
@post
@tag("Semantic Layer")
@summary("Explain metric query")
op explainMetricQuery(@body body: MetricQueryRequest): Ok<MetricQueryExplainResponse> | MutatingErrorResponses;

@route("/metric-queries:run")
@post
@tag("Semantic Layer")
@summary("Run metric query")
op runMetricQuery(@body body: MetricQueryRequest): Ok<MetricQueryRunResponse> | MutatingErrorResponses;

@route("/metrics/{metricName}/freshness")
@get
@tag("Semantic Layer")
@summary("Check metric freshness")
op checkMetricFreshness(metricName: string, @query project_name?: string, @query semantic_model_name?: string): Ok<MetricFreshnessStatus> | ResourceErrorResponses;
