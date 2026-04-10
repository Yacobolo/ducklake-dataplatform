using TypeSpec.Http;

namespace Duck;

@route("/semantic-sources/{source_schema}/{source_table}/freshness")
@get
@tag("Semantic Layer")
@summary("Check source freshness")
op checkSourceFreshness(source_schema: string, source_table: string, @query timestamp_column?: string, @query max_lag_seconds?: int64): Ok<SourceFreshnessStatus> | ResourceErrorResponses;

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

@route("/semantic-models/{semantic_model_id}")
@get
@tag("Semantic Layer")
@summary("Get semantic model")
op getSemanticModel(semantic_model_id: string): Ok<SemanticModel> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic model")
op updateSemanticModel(semantic_model_id: string, @body body: UpdateSemanticModelRequest): Ok<SemanticModel> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic model")
op deleteSemanticModel(semantic_model_id: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/metrics")
@get
@tag("Semantic Layer")
@summary("List semantic metrics")
op listSemanticMetrics(semantic_model_id: string): Ok<SemanticMetricList> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}/metrics/{metric_name}")
@get
@tag("Semantic Layer")
@summary("Get semantic metric")
op getSemanticMetric(semantic_model_id: string, metric_name: string): Ok<SemanticMetric> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}/metrics")
@post
@tag("Semantic Layer")
@summary("Create semantic metric")
op createSemanticMetric(semantic_model_id: string, @body body: CreateSemanticMetricRequest): Created<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/metrics/{metric_name}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic metric")
op updateSemanticMetric(semantic_model_id: string, metric_name: string, @body body: UpdateSemanticMetricRequest): Ok<SemanticMetric> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/metrics/{metric_name}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic metric")
op deleteSemanticMetric(semantic_model_id: string, metric_name: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/pre-aggregations")
@get
@tag("Semantic Layer")
@summary("List semantic pre aggregations")
op listSemanticPreAggregations(semantic_model_id: string): Ok<SemanticPreAggregationList> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}")
@get
@tag("Semantic Layer")
@summary("Get semantic pre aggregation")
op getSemanticPreAggregation(semantic_model_id: string, pre_aggregation_name: string): Ok<SemanticPreAggregation> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}/pre-aggregations")
@post
@tag("Semantic Layer")
@summary("Create semantic pre aggregation")
op createSemanticPreAggregation(semantic_model_id: string, @body body: CreateSemanticPreAggregationRequest): Created<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic pre aggregation")
op updateSemanticPreAggregation(semantic_model_id: string, pre_aggregation_name: string, @body body: UpdateSemanticPreAggregationRequest): Ok<SemanticPreAggregation> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/pre-aggregations/{pre_aggregation_name}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic pre aggregation")
op deleteSemanticPreAggregation(semantic_model_id: string, pre_aggregation_name: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/relationships")
@get
@tag("Semantic Layer")
@summary("List semantic relationships for a semantic model")
op listSemanticModelRelationships(semantic_model_id: string): Ok<SemanticRelationshipList> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}/relationships/{relationship_name}")
@get
@tag("Semantic Layer")
@summary("Get semantic relationship for a semantic model")
op getSemanticModelRelationship(semantic_model_id: string, relationship_name: string): Ok<SemanticRelationship> | ResourceErrorResponses;

@route("/semantic-models/{semantic_model_id}/relationships")
@post
@tag("Semantic Layer")
@summary("Create semantic relationship for a semantic model")
op createSemanticModelRelationship(semantic_model_id: string, @body body: CreateSemanticRelationshipRequest): Created<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/relationships/{relationship_name}")
@patch(#{ implicitOptionality: true })
@tag("Semantic Layer")
@summary("Update semantic relationship for a semantic model")
op updateSemanticModelRelationship(semantic_model_id: string, relationship_name: string, @body body: UpdateSemanticRelationshipRequest): Ok<SemanticRelationship> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/relationships/{relationship_name}")
@delete
@tag("Semantic Layer")
@summary("Delete semantic relationship for a semantic model")
op deleteSemanticModelRelationship(semantic_model_id: string, relationship_name: string): NoContent | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/query-explanations")
@post
@tag("Semantic Layer")
@summary("Explain metric query")
op explainMetricQuery(semantic_model_id: string, @body body: MetricQueryRequest): Ok<MetricQueryExplainResponse> | MutatingErrorResponses;

@route("/semantic-models/{semantic_model_id}/query-runs")
@post
@tag("Semantic Layer")
@summary("Run metric query")
op runMetricQuery(semantic_model_id: string, @body body: MetricQueryRequest): Ok<MetricQueryRunResponse> | MutatingErrorResponses;

@route("/semantic-metrics/{metric_name}/freshness")
@get
@tag("Semantic Layer")
@summary("Check metric freshness")
op checkMetricFreshness(metric_name: string, @query semantic_model_id?: string): Ok<MetricFreshnessStatus> | ResourceErrorResponses;
