package semantic

import "duck-demo/internal/service/query"

// MetricQueryRequest is the runtime request contract for semantic query planning and execution.
type MetricQueryRequest struct {
	ProjectName       string
	SemanticModelName string
	Metrics           []string
	Dimensions        []string
	Filters           []string
	OrderBy           []string
	Limit             *int
}

// JoinStep describes one relationship step selected by the planner.
type JoinStep struct {
	RelationshipName string
	FromModel        string
	ToModel          string
	JoinSQL          string
}

// MetricQueryPlan captures the semantic planner output.
type MetricQueryPlan struct {
	BaseModelName          string
	BaseRelation           string
	Metrics                []string
	Dimensions             []string
	JoinPath               []JoinStep
	SelectedPreAggregation *string
	GeneratedSQL           string
	FreshnessStatus        string
	FreshnessBasis         []string
}

// MetricQueryResult wraps execution output and the generated plan.
type MetricQueryResult struct {
	Plan   MetricQueryPlan
	Result *query.QueryResult
}
