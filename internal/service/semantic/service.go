package semantic

import (
	"context"

	"duck-demo/internal/domain"
)

// Service provides business logic for semantic layer resource management.
type Service struct {
	models        domain.SemanticModelRepository
	metrics       domain.SemanticMetricRepository
	relationships domain.SemanticRelationshipRepository
	preAggs       domain.SemanticPreAggregationRepository
	queryExec     queryExecutor
}

// NewService creates a new semantic Service.
func NewService(
	models domain.SemanticModelRepository,
	metrics domain.SemanticMetricRepository,
	relationships domain.SemanticRelationshipRepository,
	preAggs domain.SemanticPreAggregationRepository,
) *Service {
	return &Service{
		models:        models,
		metrics:       metrics,
		relationships: relationships,
		preAggs:       preAggs,
	}
}

// CreateSemanticModel creates a semantic model.
func (s *Service) CreateSemanticModel(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	return s.models.Create(ctx, &domain.SemanticModel{
		ProjectName:          req.ProjectName,
		Name:                 req.Name,
		Description:          req.Description,
		BaseModelRef:         req.BaseModelRef,
		DefaultTimeDimension: req.DefaultTimeDimension,
		Tags:                 req.Tags,
		CreatedBy:            principal,
	})
}

// GetSemanticModel retrieves a semantic model by project and name.
func (s *Service) GetSemanticModel(ctx context.Context, projectName, name string) (*domain.SemanticModel, error) {
	return s.models.GetByName(ctx, projectName, name)
}

// ListSemanticModels lists semantic models with optional project filter.
func (s *Service) ListSemanticModels(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
	return s.models.List(ctx, projectName, page)
}

// UpdateSemanticModel updates an existing semantic model.
func (s *Service) UpdateSemanticModel(ctx context.Context, projectName, name string, req domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error) {
	existing, err := s.models.GetByName(ctx, projectName, name)
	if err != nil {
		return nil, err
	}
	return s.models.Update(ctx, existing.ID, req)
}

// DeleteSemanticModel deletes an existing semantic model.
func (s *Service) DeleteSemanticModel(ctx context.Context, projectName, name string) error {
	existing, err := s.models.GetByName(ctx, projectName, name)
	if err != nil {
		return err
	}
	return s.models.Delete(ctx, existing.ID)
}

// CreateMetric creates a metric inside a semantic model.
func (s *Service) CreateMetric(ctx context.Context, principal, projectName, semanticModelName string, req domain.CreateSemanticMetricRequest) (*domain.SemanticMetric, error) {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return nil, err
	}
	req.SemanticModelID = semanticModel.ID

	if err := req.Validate(); err != nil {
		return nil, err
	}

	return s.metrics.Create(ctx, &domain.SemanticMetric{
		SemanticModelID:    semanticModel.ID,
		Name:               req.Name,
		Description:        req.Description,
		MetricType:         req.MetricType,
		ExpressionMode:     req.ExpressionMode,
		Expression:         req.Expression,
		DefaultTimeGrain:   req.DefaultTimeGrain,
		Format:             req.Format,
		CertificationState: req.CertificationState,
		CreatedBy:          principal,
	})
}

// ListMetrics lists metrics for a semantic model.
func (s *Service) ListMetrics(ctx context.Context, projectName, semanticModelName string) ([]domain.SemanticMetric, error) {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return nil, err
	}
	return s.metrics.ListByModel(ctx, semanticModel.ID)
}

// UpdateMetric updates an existing metric by name.
func (s *Service) UpdateMetric(ctx context.Context, projectName, semanticModelName, metricName string, req domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error) {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return nil, err
	}
	existing, err := s.metrics.GetByName(ctx, semanticModel.ID, metricName)
	if err != nil {
		return nil, err
	}
	return s.metrics.Update(ctx, existing.ID, req)
}

// DeleteMetric deletes an existing metric by name.
func (s *Service) DeleteMetric(ctx context.Context, projectName, semanticModelName, metricName string) error {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return err
	}
	existing, err := s.metrics.GetByName(ctx, semanticModel.ID, metricName)
	if err != nil {
		return err
	}
	return s.metrics.Delete(ctx, existing.ID)
}

// CreateRelationship creates a semantic relationship.
func (s *Service) CreateRelationship(ctx context.Context, principal string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	return s.relationships.Create(ctx, &domain.SemanticRelationship{
		Name:             req.Name,
		FromSemanticID:   req.FromSemanticID,
		ToSemanticID:     req.ToSemanticID,
		RelationshipType: req.RelationshipType,
		JoinSQL:          req.JoinSQL,
		IsDefault:        req.IsDefault,
		Cost:             req.Cost,
		MaxHops:          req.MaxHops,
		CreatedBy:        principal,
	})
}

// ListRelationships lists semantic relationships.
func (s *Service) ListRelationships(ctx context.Context, page domain.PageRequest) ([]domain.SemanticRelationship, int64, error) {
	return s.relationships.List(ctx, page)
}

// UpdateRelationship updates an existing relationship by name.
func (s *Service) UpdateRelationship(ctx context.Context, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	existing, err := s.relationships.GetByName(ctx, relationshipName)
	if err != nil {
		return nil, err
	}
	return s.relationships.Update(ctx, existing.ID, req)
}

// DeleteRelationship deletes an existing relationship by name.
func (s *Service) DeleteRelationship(ctx context.Context, relationshipName string) error {
	existing, err := s.relationships.GetByName(ctx, relationshipName)
	if err != nil {
		return err
	}
	return s.relationships.Delete(ctx, existing.ID)
}

// CreatePreAggregation creates a semantic pre-aggregation under a semantic model.
func (s *Service) CreatePreAggregation(ctx context.Context, principal, projectName, semanticModelName string, req domain.CreateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error) {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return nil, err
	}
	req.SemanticModelID = semanticModel.ID

	if err := req.Validate(); err != nil {
		return nil, err
	}

	return s.preAggs.Create(ctx, &domain.SemanticPreAggregation{
		SemanticModelID: semanticModel.ID,
		Name:            req.Name,
		MetricSet:       req.MetricSet,
		DimensionSet:    req.DimensionSet,
		Grain:           req.Grain,
		TargetRelation:  req.TargetRelation,
		RefreshPolicy:   req.RefreshPolicy,
		CreatedBy:       principal,
	})
}

// ListPreAggregations lists pre-aggregations for a semantic model.
func (s *Service) ListPreAggregations(ctx context.Context, projectName, semanticModelName string) ([]domain.SemanticPreAggregation, error) {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return nil, err
	}
	return s.preAggs.ListByModel(ctx, semanticModel.ID)
}

// UpdatePreAggregation updates an existing pre-aggregation by name under a semantic model.
func (s *Service) UpdatePreAggregation(ctx context.Context, projectName, semanticModelName, preAggName string, req domain.UpdateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error) {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return nil, err
	}
	existing, err := s.preAggs.GetByName(ctx, semanticModel.ID, preAggName)
	if err != nil {
		return nil, err
	}
	return s.preAggs.Update(ctx, existing.ID, req)
}

// DeletePreAggregation deletes an existing pre-aggregation by name under a semantic model.
func (s *Service) DeletePreAggregation(ctx context.Context, projectName, semanticModelName, preAggName string) error {
	semanticModel, err := s.models.GetByName(ctx, projectName, semanticModelName)
	if err != nil {
		return err
	}
	existing, err := s.preAggs.GetByName(ctx, semanticModel.ID, preAggName)
	if err != nil {
		return err
	}
	return s.preAggs.Delete(ctx, existing.ID)
}
