package semantic

import (
	"context"
	"fmt"

	"duck-demo/internal/service/query"
)

type queryExecutor interface {
	Execute(ctx context.Context, principalName, sqlQuery string) (*query.QueryResult, error)
}

// SetQueryExecutor wires the secured query execution dependency.
func (s *Service) SetQueryExecutor(exec queryExecutor) {
	s.queryExec = exec
}

// RunMetricQuery plans and executes a semantic query through the secure query path.
func (s *Service) RunMetricQuery(ctx context.Context, principal string, req MetricQueryRequest) (*MetricQueryResult, error) {
	if s.queryExec == nil {
		return nil, fmt.Errorf("semantic query executor is not configured")
	}

	plan, err := s.ExplainMetricQuery(ctx, req)
	if err != nil {
		return nil, err
	}

	result, err := s.queryExec.Execute(ctx, principal, plan.GeneratedSQL)
	if err != nil {
		return nil, err
	}

	return &MetricQueryResult{Plan: *plan, Result: result}, nil
}
