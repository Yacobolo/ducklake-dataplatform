package semantic

import "duck-demo/internal/service/query"

// MetricQueryRequest is the runtime request contract for semantic query planning and execution.
type MetricQueryRequest struct {
	SemanticModelID   string
	Metrics           []string
	RelationshipNames []string
	Dimensions        []string
	Filters           []string
	OrderBy           []string
	Limit             *int
	Offset            *int
	TimeGrain         *string
}

// JoinStep describes one relationship step selected by the planner.
type JoinStep struct {
	RelationshipName string
	FromModel        string
	ToModel          string
	RelationshipType string
	JoinSQL          string
}

// MetricQueryPlan captures the semantic planner output.
type MetricQueryPlan struct {
	BaseModelName          string
	BaseRelation           string
	Metrics                []string
	Dimensions             []string
	TimeGrain              *string
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
