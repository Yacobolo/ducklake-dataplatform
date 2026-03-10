package semantic

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
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
		return nil, s.normalizeExecutionError(ctx, req, plan, err)
	}

	return &MetricQueryResult{Plan: *plan, Result: result}, nil
}

func (s *Service) normalizeExecutionError(ctx context.Context, req MetricQueryRequest, plan *MetricQueryPlan, err error) error {
	if plan == nil || err == nil {
		return err
	}

	msg := err.Error()
	if !strings.Contains(msg, "catalog lookup for") {
		return err
	}

	if plan.SelectedPreAggregation != nil {
		return domain.ErrValidation(
			"semantic pre-aggregation %q targets relation %q, but that relation is not materialized yet",
			*plan.SelectedPreAggregation, plan.BaseRelation,
		)
	}

	if s.modelRepo != nil {
		if model, modelErr := s.resolveTransformationModel(ctx, req.ProjectName, plan.BaseRelation); modelErr == nil && model != nil {
			return domain.ErrValidation(
				"semantic model %q references transformation model %q, but its relation is not materialized; run the model first or point base_model_ref at a queryable relation",
				req.SemanticModelName, model.QualifiedName(),
			)
		}
	}

	return err
}

func (s *Service) resolveTransformationModel(ctx context.Context, defaultProjectName, relation string) (*domain.Model, error) {
	relation = strings.TrimSpace(relation)
	if relation == "" || s.modelRepo == nil {
		return nil, fmt.Errorf("model repository not configured")
	}

	parts := strings.SplitN(relation, ".", 2)
	projectName := defaultProjectName
	modelName := relation
	if len(parts) == 2 {
		projectName = parts[0]
		modelName = parts[1]
	}

	model, err := s.modelRepo.GetByName(ctx, projectName, modelName)
	if err != nil {
		return nil, err
	}
	return model, nil
}
