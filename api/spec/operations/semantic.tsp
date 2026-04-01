using TypeSpec.Http;

namespace Duck;

@route("/sources/{sourceSchema}/{sourceTable}/freshness")
@get
@tag("Semantic Layer")
@summary("Check source freshness")
op checkSourceFreshness(sourceSchema: string, sourceTable: string, @query timestamp_column?: string, @query max_lag_seconds?: int64): Ok<SourceFreshnessStatus> | ResourceErrorResponses;

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

@route("/semantic-models/{projectName}/{semanticModelName}/relationships")
@get
@tag("Semantic Layer")
@summary("List semantic relationships for a semantic model")
op listSemanticModelRelationships(projectName: string, semanticModelName: string): Ok<SemanticRelationshipList> | ResourceErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/relationships")
@post
@tag("Semantic Layer")
@summary("Create semantic relationship for a semantic model")
op createSemanticModelRelationship(projectName: string, semanticModelName: string, @body body: CreateSemanticRelationshipRequest): Created<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic relationship for a semantic model")
op updateSemanticModelRelationship(projectName: string, semanticModelName: string, relationshipName: string, @body body: UpdateSemanticRelationshipRequest): Ok<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-models/{projectName}/{semanticModelName}/relationships/{relationshipName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic relationship for a semantic model")
op deleteSemanticModelRelationship(projectName: string, semanticModelName: string, relationshipName: string): NoContent | MutatingErrorResponses;

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
