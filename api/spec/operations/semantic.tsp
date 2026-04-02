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
op listSemanticModels(@query max_results?: int32, @query page_token?: string): Ok<PaginatedSemanticModels> | StandardErrorResponses;

@route("/semantic-models")
@post
@tag("Semantic Layer")
@summary("Create semantic model")
op createSemanticModel(@body body: CreateSemanticModelRequest): Created<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}")
@get
@tag("Semantic Layer")
@summary("Get semantic model")
op getSemanticModel(semanticModelId: string): Ok<SemanticModel> | ResourceErrorResponses;

@route("/semantic-models/{semanticModelId}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic model")
op updateSemanticModel(semanticModelId: string, @body body: UpdateSemanticModelRequest): Ok<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic model")
op deleteSemanticModel(semanticModelId: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/metrics")
@get
@tag("Semantic Layer")
@summary("List semantic metrics")
op listSemanticMetrics(semanticModelId: string): Ok<SemanticMetricList> | ResourceErrorResponses;

@route("/semantic-models/{semanticModelId}/metrics")
@post
@tag("Semantic Layer")
@summary("Create semantic metric")
op createSemanticMetric(semanticModelId: string, @body body: CreateSemanticMetricRequest): Created<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/metrics/{metricName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic metric")
op updateSemanticMetric(semanticModelId: string, metricName: string, @body body: UpdateSemanticMetricRequest): Ok<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/metrics/{metricName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic metric")
op deleteSemanticMetric(semanticModelId: string, metricName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/pre-aggregations")
@get
@tag("Semantic Layer")
@summary("List semantic pre aggregations")
op listSemanticPreAggregations(semanticModelId: string): Ok<SemanticPreAggregationList> | ResourceErrorResponses;

@route("/semantic-models/{semanticModelId}/pre-aggregations")
@post
@tag("Semantic Layer")
@summary("Create semantic pre aggregation")
op createSemanticPreAggregation(semanticModelId: string, @body body: CreateSemanticPreAggregationRequest): Created<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/pre-aggregations/{preAggregationName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic pre aggregation")
op updateSemanticPreAggregation(semanticModelId: string, preAggregationName: string, @body body: UpdateSemanticPreAggregationRequest): Ok<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/pre-aggregations/{preAggregationName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic pre aggregation")
op deleteSemanticPreAggregation(semanticModelId: string, preAggregationName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/relationships")
@get
@tag("Semantic Layer")
@summary("List semantic relationships for a semantic model")
op listSemanticModelRelationships(semanticModelId: string): Ok<SemanticRelationshipList> | ResourceErrorResponses;

@route("/semantic-models/{semanticModelId}/relationships")
@post
@tag("Semantic Layer")
@summary("Create semantic relationship for a semantic model")
op createSemanticModelRelationship(semanticModelId: string, @body body: CreateSemanticRelationshipRequest): Created<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/relationships/{relationshipName}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic relationship for a semantic model")
op updateSemanticModelRelationship(semanticModelId: string, relationshipName: string, @body body: UpdateSemanticRelationshipRequest): Ok<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/relationships/{relationshipName}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic relationship for a semantic model")
op deleteSemanticModelRelationship(semanticModelId: string, relationshipName: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/queries:explain")
@post
@tag("Semantic Layer")
@summary("Explain metric query")
op explainMetricQuery(semanticModelId: string, @body body: MetricQueryRequest): Ok<MetricQueryExplainResponse> | MutatingErrorResponses;

@route("/semantic-models/{semanticModelId}/queries:run")
@post
@tag("Semantic Layer")
@summary("Run metric query")
op runMetricQuery(semanticModelId: string, @body body: MetricQueryRequest): Ok<MetricQueryRunResponse> | MutatingErrorResponses;

@route("/metrics/{metricName}/freshness")
@get
@tag("Semantic Layer")
@summary("Check metric freshness")
op checkMetricFreshness(metricName: string, @query semantic_model_id?: string): Ok<MetricFreshnessStatus> | ResourceErrorResponses;
