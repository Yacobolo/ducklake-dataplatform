// Code generated for operation parity scaffolding.

using TypeSpec.Http;

namespace Duck;

model GenericRequest {
  payload?: Record<string>;
}

model GenericResponse {
  data?: Record<string>;
}

@route("/queries")
@post
op submitQuery(@body body: GenericRequest): GenericResponse;

@route("/queries/{queryId}")
@get
op getQuery(queryId: string): GenericResponse;

@route("/queries/{queryId}")
@delete
op deleteQuery(queryId: string): void;

@route("/queries/{queryId}/results")
@get
op getQueryResults(queryId: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/queries/{queryId}/cancel")
@post
op cancelQuery(queryId: string): GenericResponse;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/upload-url")
@post
op createUploadUrl(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): GenericResponse;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/commit")
@post
op commitTableIngestion(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): GenericResponse;

@route("/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/ingestion/load")
@post
op loadTableExternalFiles(catalogName: string, schemaName: string, tableName: string, @body body: GenericRequest): GenericResponse;

@route("/search")
@get
op searchCatalog(query: string, type?: string, catalog?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/tables/{schemaName}/{tableName}")
@get
op getTableLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/tables/{schemaName}/{tableName}/upstream")
@get
op getUpstreamLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/tables/{schemaName}/{tableName}/downstream")
@get
op getDownstreamLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/edges/{edgeId}")
@delete
op deleteLineageEdge(edgeId: string): void;

@route("/lineage/columns/{schemaName}/{tableName}")
@get
op getColumnLineage(schemaName: string, tableName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/columns/{schemaName}/{tableName}/{columnName}/impact")
@get
op getColumnImpact(schemaName: string, tableName: string, columnName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/lineage/purge")
@post
op purgeLineage(@body body: GenericRequest): GenericResponse;

@route("/tags")
@get
op listTags(max_results?: int32, page_token?: string): GenericResponse;

@route("/tags")
@post
op createTag(@body body: GenericRequest): GenericResponse;

@route("/tags/{tagId}")
@delete
op deleteTag(tagId: string): void;

@route("/tags/{tagId}/assignments")
@post
op createTagAssignment(tagId: string, @body body: GenericRequest): GenericResponse;

@route("/tag-assignments/{assignmentId}")
@delete
op deleteTagAssignment(assignmentId: string): void;

@route("/classifications")
@get
op listClassifications(max_results?: int32, page_token?: string): GenericResponse;

@route("/storage-credentials")
@get
op listStorageCredentials(max_results?: int32, page_token?: string): GenericResponse;

@route("/storage-credentials")
@post
op createStorageCredential(@body body: GenericRequest): GenericResponse;

@route("/storage-credentials/{credentialName}")
@get
op getStorageCredential(credentialName: string): GenericResponse;

@route("/storage-credentials/{credentialName}")
@patch(#{ implicitOptionality: true })
op updateStorageCredential(credentialName: string, @body body: GenericRequest): GenericResponse;

@route("/storage-credentials/{credentialName}")
@delete
op deleteStorageCredential(credentialName: string): void;

@route("/external-locations")
@get
op listExternalLocations(max_results?: int32, page_token?: string): GenericResponse;

@route("/external-locations")
@post
op createExternalLocation(@body body: GenericRequest): GenericResponse;

@route("/external-locations/{locationName}")
@get
op getExternalLocation(locationName: string): GenericResponse;

@route("/external-locations/{locationName}")
@patch(#{ implicitOptionality: true })
op updateExternalLocation(locationName: string, @body body: GenericRequest): GenericResponse;

@route("/external-locations/{locationName}")
@delete
op deleteExternalLocation(locationName: string): void;

@route("/compute-endpoints")
@get
op listComputeEndpoints(max_results?: int32, page_token?: string): GenericResponse;

@route("/compute-endpoints")
@post
op createComputeEndpoint(@body body: GenericRequest): GenericResponse;

@route("/compute-endpoints/{endpointName}")
@get
op getComputeEndpoint(endpointName: string): GenericResponse;

@route("/compute-endpoints/{endpointName}")
@patch(#{ implicitOptionality: true })
op updateComputeEndpoint(endpointName: string, @body body: GenericRequest): GenericResponse;

@route("/compute-endpoints/{endpointName}")
@delete
op deleteComputeEndpoint(endpointName: string): void;

@route("/compute-endpoints/{endpointName}/assignments")
@get
op listComputeAssignments(endpointName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/compute-endpoints/{endpointName}/assignments")
@post
op createComputeAssignment(endpointName: string, @body body: GenericRequest): GenericResponse;

@route("/compute-endpoints/{endpointName}/health")
@get
op getComputeEndpointHealth(endpointName: string): GenericResponse;

@route("/compute-endpoints/{endpointName}/assignments/{assignmentId}")
@delete
op deleteComputeAssignment(endpointName: string, assignmentId: string): void;

@route("/notebooks")
@get
op listNotebooks(owner?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/notebooks")
@post
op createNotebook(@body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}")
@get
op getNotebook(notebookId: string): GenericResponse;

@route("/notebooks/{notebookId}")
@patch(#{ implicitOptionality: true })
op updateNotebook(notebookId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}")
@delete
op deleteNotebook(notebookId: string): void;

@route("/notebooks/{notebookId}/cells")
@post
op createCell(notebookId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}/cells/reorder")
@post
op reorderCells(notebookId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}/cells/{cellId}")
@patch(#{ implicitOptionality: true })
op updateCell(notebookId: string, cellId: string, @body body: GenericRequest): GenericResponse;

@route("/notebooks/{notebookId}/cells/{cellId}")
@delete
op deleteCell(notebookId: string, cellId: string): void;

@route("/notebooks/{notebookId}/sessions")
@post
op createNotebookSession(notebookId: string): GenericResponse;

@route("/notebooks/{notebookId}/sessions/{sessionId}")
@delete
op closeNotebookSession(notebookId: string, sessionId: string): void;

@route("/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}")
@post
op executeCell(notebookId: string, sessionId: string, cellId: string): GenericResponse;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all")
@post
op runAllCells(notebookId: string, sessionId: string): GenericResponse;

@route("/notebooks/{notebookId}/sessions/{sessionId}/run-all-async")
@post
op runAllCellsAsync(notebookId: string, sessionId: string): GenericResponse;

@route("/notebooks/{notebookId}/jobs")
@get
op listNotebookJobs(notebookId: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/notebooks/{notebookId}/jobs/{jobId}")
@get
op getNotebookJob(notebookId: string, jobId: string): GenericResponse;

@route("/git-repos")
@get
op listGitRepos(max_results?: int32, page_token?: string): GenericResponse;

@route("/git-repos")
@post
op createGitRepo(@body body: GenericRequest): GenericResponse;

@route("/git-repos/{gitRepoId}")
@get
op getGitRepo(gitRepoId: string): GenericResponse;

@route("/git-repos/{gitRepoId}")
@delete
op deleteGitRepo(gitRepoId: string): void;

@route("/git-repos/{gitRepoId}/sync")
@post
op syncGitRepo(gitRepoId: string): GenericResponse;

@route("/pipelines")
@get
op listPipelines(max_results?: int32, page_token?: string): GenericResponse;

@route("/pipelines")
@post
op createPipeline(@body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}")
@get
op getPipeline(pipelineName: string): GenericResponse;

@route("/pipelines/{pipelineName}")
@patch(#{ implicitOptionality: true })
op updatePipeline(pipelineName: string, @body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}")
@delete
op deletePipeline(pipelineName: string): void;

@route("/pipelines/{pipelineName}/jobs")
@get
op listPipelineJobs(pipelineName: string): GenericResponse;

@route("/pipelines/{pipelineName}/jobs")
@post
op createPipelineJob(pipelineName: string, @body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}/jobs/{jobId}")
@delete
op deletePipelineJob(pipelineName: string, jobId: string): void;

@route("/pipelines/{pipelineName}/runs")
@post
op triggerPipelineRun(pipelineName: string, @body body: GenericRequest): GenericResponse;

@route("/pipelines/{pipelineName}/runs")
@get
op listPipelineRuns(pipelineName: string, max_results?: int32, page_token?: string, status?: string): GenericResponse;

@route("/pipelines/runs/{runId}")
@get
op getPipelineRun(runId: string): GenericResponse;

@route("/pipelines/runs/{runId}/cancel")
@post
op cancelPipelineRun(runId: string): GenericResponse;

@route("/pipelines/runs/{runId}/jobs")
@get
op listPipelineJobRuns(runId: string): GenericResponse;

@route("/models")
@get
op listModels(project_name?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/models")
@post
op createModel(@body body: GenericRequest): GenericResponse;

@route("/models/{projectName}/{modelName}")
@get
op getModel(projectName: string, modelName: string): GenericResponse;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
op updateModel(projectName: string, modelName: string, @body body: GenericRequest): GenericResponse;

@route("/models/{projectName}/{modelName}")
@delete
op deleteModel(projectName: string, modelName: string): void;

@route("/models/dag")
@get
op getModelDAG(project_name?: string): GenericResponse;

@route("/model-runs")
@post
op triggerModelRun(@body body: GenericRequest): GenericResponse;

@route("/model-runs")
@get
op listModelRuns(status?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/model-runs/{runId}")
@get
op getModelRun(runId: string): GenericResponse;

@route("/model-runs/{runId}/steps")
@get
op listModelRunSteps(runId: string): GenericResponse;

@route("/models/{projectName}/{modelName}/tests")
@post
op createModelTest(projectName: string, modelName: string, @body body: GenericRequest): GenericResponse;

@route("/models/{projectName}/{modelName}/tests")
@get
op listModelTests(projectName: string, modelName: string): GenericResponse;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
op deleteModelTest(projectName: string, modelName: string, testId: string): void;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
op listModelTestResults(runId: string, stepId: string): GenericResponse;

@route("/models/{projectName}/{modelName}/freshness")
@get
op checkModelFreshness(projectName: string, modelName: string): GenericResponse;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
op checkSourceFreshness(sourceSchema: string, sourceTable: string, timestamp_column?: string, max_lag_seconds?: int64): GenericResponse;

@route("/models/from-notebook")
@post
op promoteNotebookToModel(@body body: GenericRequest): GenericResponse;

@route("/model-runs/{runId}/cancel")
@post
op cancelModelRun(runId: string): GenericResponse;

@route("/macros")
@get
op listMacros(max_results?: int32, page_token?: string): GenericResponse;

@route("/macros")
@post
op createMacro(@body body: GenericRequest): GenericResponse;

@route("/macros/{macroName}")
@get
op getMacro(macroName: string): GenericResponse;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
op updateMacro(macroName: string, @body body: GenericRequest): GenericResponse;

@route("/macros/{macroName}")
@delete
op deleteMacro(macroName: string): void;

@route("/macros/{macroName}/revisions")
@get
op listMacroRevisions(macroName: string): GenericResponse;

@route("/macros/{macroName}/impact")
@get
op getMacroImpact(macroName: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/macros/{macroName}/diff")
@get
op diffMacroRevisions(macroName: string, from_version: int32, to_version: int32): GenericResponse;

@route("/semantic-models")
@get
op listSemanticModels(project_name?: string, max_results?: int32, page_token?: string): GenericResponse;

@route("/semantic-models")
@post
op createSemanticModel(@body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}")
@get
op getSemanticModel(projectName: string, semanticModelName: string): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}")
@patch(#{ implicitOptionality: true })
op updateSemanticModel(projectName: string, semanticModelName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}")
@delete
op deleteSemanticModel(projectName: string, semanticModelName: string): void;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@get
op listSemanticMetrics(projectName: string, semanticModelName: string): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics")
@post
op createSemanticMetric(projectName: string, semanticModelName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
op updateSemanticMetric(projectName: string, semanticModelName: string, metricName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/metrics/{metricName}")
@delete
op deleteSemanticMetric(projectName: string, semanticModelName: string, metricName: string): void;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@get
op listSemanticPreAggregations(projectName: string, semanticModelName: string): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations")
@post
op createSemanticPreAggregation(projectName: string, semanticModelName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
op updateSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-models/{projectName}/{semanticModelName}/pre-aggregations/{preAggregationName}")
@delete
op deleteSemanticPreAggregation(projectName: string, semanticModelName: string, preAggregationName: string): void;

@route("/semantic-relationships")
@get
op listSemanticRelationships(max_results?: int32, page_token?: string): GenericResponse;

@route("/semantic-relationships")
@post
op createSemanticRelationship(@body body: GenericRequest): GenericResponse;

@route("/semantic-relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
op updateSemanticRelationship(relationshipName: string, @body body: GenericRequest): GenericResponse;

@route("/semantic-relationships/{relationshipName}")
@delete
op deleteSemanticRelationship(relationshipName: string): void;

@route("/metric-queries:explain")
@post
op explainMetricQuery(@body body: GenericRequest): GenericResponse;

@route("/metric-queries:run")
@post
op runMetricQuery(@body body: GenericRequest): GenericResponse;

@route("/metrics/{metricName}/freshness")
@get
op checkMetricFreshness(metricName: string, project_name?: string, semantic_model_name?: string): GenericResponse;

