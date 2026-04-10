package semantic

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// Service provides business logic for semantic layer resource management.
type Service struct {
	models        domain.SemanticModelRepository
	metrics       domain.SemanticMetricRepository
	relationships domain.SemanticRelationshipRepository
	preAggs       domain.SemanticPreAggregationRepository
	modelRepo     domain.ModelRepository
	queryExec     queryExecutor
	ddlExec       domain.DuckDBExecutor
}

// NewService creates a new semantic Service.
func NewService(
	models domain.SemanticModelRepository,
	metrics domain.SemanticMetricRepository,
	relationships domain.SemanticRelationshipRepository,
	preAggs domain.SemanticPreAggregationRepository,
	modelDefs ...domain.ModelRepository,
) *Service {
	var modelRepo domain.ModelRepository
	if len(modelDefs) > 0 {
		modelRepo = modelDefs[0]
	}
	return &Service{
		models:        models,
		metrics:       metrics,
		relationships: relationships,
		preAggs:       preAggs,
		modelRepo:     modelRepo,
	}
}

// SetModelRepository wires transformation model lookup for semantic run error normalization.
func (s *Service) SetModelRepository(repo domain.ModelRepository) {
	s.modelRepo = repo
}

// SetDDLExecutor wires a trusted DuckDB executor for internal pre-aggregation materialization.
func (s *Service) SetDDLExecutor(exec domain.DuckDBExecutor) {
	s.ddlExec = exec
}

func (s *Service) resolveSemanticModel(ctx context.Context, primary string, secondary ...string) (*domain.SemanticModel, error) {
	if len(secondary) == 0 {
		return s.models.GetByID(ctx, primary)
	}
	return s.models.GetByName(ctx, secondary[len(secondary)-1])
}

// CreateSemanticModel creates a semantic model.
func (s *Service) CreateSemanticModel(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	item, err := s.models.Create(ctx, &domain.SemanticModel{
		ProjectName:          req.ProjectName,
		Name:                 req.Name,
		Description:          req.Description,
		BaseModelRef:         req.BaseModelRef,
		DefaultTimeDimension: req.DefaultTimeDimension,
		Tags:                 req.Tags,
		CreatedBy:            principal,
	})
	if err != nil {
		return nil, err
	}
	item.ProjectName = req.ProjectName
	return item, nil
}

// GetSemanticModel retrieves a semantic model by ID or by legacy project/name pair.
func (s *Service) GetSemanticModel(ctx context.Context, identifiers ...string) (*domain.SemanticModel, error) {
	switch len(identifiers) {
	case 1:
		return s.models.GetByID(ctx, identifiers[0])
	case 2:
		return s.models.GetByName(ctx, identifiers[1])
	default:
		return nil, domain.ErrValidation("semantic model lookup requires an id or project/name pair")
	}
}

// ListSemanticModels lists semantic models and accepts an optional legacy project filter.
func (s *Service) ListSemanticModels(ctx context.Context, args ...any) ([]domain.SemanticModel, int64, error) {
	switch len(args) {
	case 1:
		page, ok := args[0].(domain.PageRequest)
		if !ok {
			return nil, 0, domain.ErrValidation("semantic model list requires a page request")
		}
		return s.models.List(ctx, page)
	case 2:
		page, ok := args[1].(domain.PageRequest)
		if !ok {
			return nil, 0, domain.ErrValidation("semantic model list requires a page request")
		}
		return s.models.List(ctx, page)
	default:
		return nil, 0, domain.ErrValidation("semantic model list requires a page request")
	}
}

// UpdateSemanticModel updates an existing semantic model by ID or by legacy project/name pair.
func (s *Service) UpdateSemanticModel(ctx context.Context, primary string, args ...any) (*domain.SemanticModel, error) {
	switch len(args) {
	case 1:
		req, ok := args[0].(domain.UpdateSemanticModelRequest)
		if !ok {
			return nil, domain.ErrValidation("semantic model update requires an update request")
		}
		return s.models.Update(ctx, primary, req)
	case 2:
		name, ok := args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("semantic model update requires a model name")
		}
		req, ok := args[1].(domain.UpdateSemanticModelRequest)
		if !ok {
			return nil, domain.ErrValidation("semantic model update requires an update request")
		}
		model, err := s.resolveSemanticModel(ctx, primary, name)
		if err != nil {
			return nil, err
		}
		return s.models.Update(ctx, model.ID, req)
	default:
		return nil, domain.ErrValidation("semantic model update requires an id or project/name pair")
	}
}

// DeleteSemanticModel deletes an existing semantic model.
func (s *Service) DeleteSemanticModel(ctx context.Context, semanticModelID string) error {
	return s.models.Delete(ctx, semanticModelID)
}

// CreateMetric creates a metric inside a semantic model.
func (s *Service) CreateMetric(ctx context.Context, principal, primary string, args ...any) (*domain.SemanticMetric, error) {
	var (
		semanticModel *domain.SemanticModel
		req           domain.CreateSemanticMetricRequest
		err           error
	)
	switch len(args) {
	case 1:
		var ok bool
		req, ok = args[0].(domain.CreateSemanticMetricRequest)
		if !ok {
			return nil, domain.ErrValidation("create metric requires a metric request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary)
	case 2:
		modelName, ok := args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("create metric requires a semantic model name")
		}
		req, ok = args[1].(domain.CreateSemanticMetricRequest)
		if !ok {
			return nil, domain.ErrValidation("create metric requires a metric request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary, modelName)
	default:
		return nil, domain.ErrValidation("create metric requires a semantic model reference and metric request")
	}
	if err != nil {
		return nil, err
	}
	req.SemanticModelID = semanticModel.ID

	if err := req.Validate(); err != nil {
		return nil, err
	}
	if err := validateMetricFilterSQL(req.FilterSQL); err != nil {
		return nil, err
	}

	return s.metrics.Create(ctx, &domain.SemanticMetric{
		SemanticModelID:    semanticModel.ID,
		Name:               req.Name,
		Description:        req.Description,
		Label:              req.Label,
		MetricType:         req.MetricType,
		ExpressionMode:     req.ExpressionMode,
		Expression:         req.Expression,
		RelationshipNames:  req.RelationshipNames,
		FilterSQL:          req.FilterSQL,
		DefaultTimeGrain:   req.DefaultTimeGrain,
		Format:             req.Format,
		CertificationState: req.CertificationState,
		CreatedBy:          principal,
	})
}

// ListMetrics lists metrics for a semantic model.
func (s *Service) ListMetrics(ctx context.Context, primary string, secondary ...string) ([]domain.SemanticMetric, error) {
	semanticModel, err := s.resolveSemanticModel(ctx, primary, secondary...)
	if err != nil {
		return nil, err
	}
	return s.metrics.ListByModel(ctx, semanticModel.ID)
}

// GetMetric gets a metric for a semantic model by name.
func (s *Service) GetMetric(ctx context.Context, semanticModelID, metricName string) (*domain.SemanticMetric, error) {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}
	return s.metrics.GetByName(ctx, semanticModel.ID, metricName)
}

// UpdateMetric updates an existing metric by name.
func (s *Service) UpdateMetric(ctx context.Context, primary string, args ...any) (*domain.SemanticMetric, error) {
	var (
		semanticModel *domain.SemanticModel
		metricName    string
		req           domain.UpdateSemanticMetricRequest
		err           error
	)
	switch len(args) {
	case 2:
		var ok bool
		metricName, ok = args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("update metric requires a metric name")
		}
		req, ok = args[1].(domain.UpdateSemanticMetricRequest)
		if !ok {
			return nil, domain.ErrValidation("update metric requires an update request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary)
	case 3:
		modelName, ok := args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("update metric requires a semantic model name")
		}
		metricName, ok = args[1].(string)
		if !ok {
			return nil, domain.ErrValidation("update metric requires a metric name")
		}
		req, ok = args[2].(domain.UpdateSemanticMetricRequest)
		if !ok {
			return nil, domain.ErrValidation("update metric requires an update request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary, modelName)
	default:
		return nil, domain.ErrValidation("update metric requires a semantic model reference, metric name, and update request")
	}
	if err != nil {
		return nil, err
	}
	if req.FilterSQL != nil {
		if err := validateMetricFilterSQL(*req.FilterSQL); err != nil {
			return nil, err
		}
	}
	existing, err := s.metrics.GetByName(ctx, semanticModel.ID, metricName)
	if err != nil {
		return nil, err
	}
	return s.metrics.Update(ctx, existing.ID, req)
}

// DeleteMetric deletes an existing metric by name.
func (s *Service) DeleteMetric(ctx context.Context, primary string, args ...string) error {
	if len(args) == 0 || len(args) > 2 {
		return domain.ErrValidation("delete metric requires a semantic model reference and metric name")
	}
	metricName := args[len(args)-1]
	semanticModel, err := s.resolveSemanticModel(ctx, primary, args[:len(args)-1]...)
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
		Cost:             req.Cost,
		MaxHops:          req.MaxHops,
		CreatedBy:        principal,
	})
}

// ListRelationships lists semantic relationships.
func (s *Service) ListRelationships(ctx context.Context, page domain.PageRequest) ([]domain.SemanticRelationship, int64, error) {
	return s.relationships.List(ctx, page)
}

// CreateRelationshipForModel creates a relationship scoped to a semantic model.
func (s *Service) CreateRelationshipForModel(ctx context.Context, principal, semanticModelID string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}
	if req.FromSemanticID != semanticModel.ID {
		return nil, domain.ErrValidation("join path source must match the current semantic model")
	}
	return s.CreateRelationship(ctx, principal, req)
}

// ListRelationshipsForModel lists semantic relationships owned by a semantic model.
func (s *Service) ListRelationshipsForModel(ctx context.Context, semanticModelID string) ([]domain.SemanticRelationship, error) {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}
	return s.relationships.ListByModel(ctx, semanticModel.ID)
}

// GetRelationshipForModel gets a semantic relationship owned by a semantic model by name.
func (s *Service) GetRelationshipForModel(ctx context.Context, semanticModelID, relationshipName string) (*domain.SemanticRelationship, error) {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}
	return s.relationships.GetByName(ctx, semanticModel.ID, relationshipName)
}

// UpdateRelationship updates an existing relationship by name.
func (s *Service) UpdateRelationship(_ context.Context, _ string, _ domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	return nil, domain.ErrValidation("global relationship updates are not supported")
}

// DeleteRelationship deletes an existing relationship by name.
func (s *Service) DeleteRelationship(_ context.Context, _ string) error {
	return domain.ErrValidation("global relationship deletes are not supported")
}

// UpdateRelationshipForModel updates a relationship that belongs to a semantic model.
func (s *Service) UpdateRelationshipForModel(ctx context.Context, semanticModelID, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error) {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}
	existing, err := s.relationships.GetByName(ctx, semanticModel.ID, relationshipName)
	if err != nil {
		return nil, err
	}
	return s.relationships.Update(ctx, existing.ID, req)
}

// DeleteRelationshipForModel deletes a relationship that belongs to a semantic model.
func (s *Service) DeleteRelationshipForModel(ctx context.Context, semanticModelID, relationshipName string) error {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return err
	}
	existing, err := s.relationships.GetByName(ctx, semanticModel.ID, relationshipName)
	if err != nil {
		return err
	}
	return s.relationships.Delete(ctx, existing.ID)
}

// CreatePreAggregation creates a semantic pre-aggregation under a semantic model.
func (s *Service) CreatePreAggregation(ctx context.Context, principal, primary string, args ...any) (*domain.SemanticPreAggregation, error) {
	var (
		semanticModel *domain.SemanticModel
		req           domain.CreateSemanticPreAggregationRequest
		err           error
	)
	switch len(args) {
	case 1:
		var ok bool
		req, ok = args[0].(domain.CreateSemanticPreAggregationRequest)
		if !ok {
			return nil, domain.ErrValidation("create pre-aggregation requires a request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary)
	case 2:
		modelName, ok := args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("create pre-aggregation requires a semantic model name")
		}
		req, ok = args[1].(domain.CreateSemanticPreAggregationRequest)
		if !ok {
			return nil, domain.ErrValidation("create pre-aggregation requires a request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary, modelName)
	default:
		return nil, domain.ErrValidation("create pre-aggregation requires a semantic model reference and request")
	}
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
func (s *Service) ListPreAggregations(ctx context.Context, primary string, secondary ...string) ([]domain.SemanticPreAggregation, error) {
	semanticModel, err := s.resolveSemanticModel(ctx, primary, secondary...)
	if err != nil {
		return nil, err
	}
	return s.preAggs.ListByModel(ctx, semanticModel.ID)
}

// GetPreAggregation gets a pre-aggregation for a semantic model by name.
func (s *Service) GetPreAggregation(ctx context.Context, semanticModelID, preAggName string) (*domain.SemanticPreAggregation, error) {
	semanticModel, err := s.models.GetByID(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}
	return s.preAggs.GetByName(ctx, semanticModel.ID, preAggName)
}

// UpdatePreAggregation updates an existing pre-aggregation by name under a semantic model.
func (s *Service) UpdatePreAggregation(ctx context.Context, primary string, args ...any) (*domain.SemanticPreAggregation, error) {
	var (
		semanticModel *domain.SemanticModel
		preAggName    string
		req           domain.UpdateSemanticPreAggregationRequest
		err           error
	)
	switch len(args) {
	case 2:
		var ok bool
		preAggName, ok = args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("update pre-aggregation requires a name")
		}
		req, ok = args[1].(domain.UpdateSemanticPreAggregationRequest)
		if !ok {
			return nil, domain.ErrValidation("update pre-aggregation requires an update request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary)
	case 3:
		modelName, ok := args[0].(string)
		if !ok {
			return nil, domain.ErrValidation("update pre-aggregation requires a semantic model name")
		}
		preAggName, ok = args[1].(string)
		if !ok {
			return nil, domain.ErrValidation("update pre-aggregation requires a name")
		}
		req, ok = args[2].(domain.UpdateSemanticPreAggregationRequest)
		if !ok {
			return nil, domain.ErrValidation("update pre-aggregation requires an update request")
		}
		semanticModel, err = s.resolveSemanticModel(ctx, primary, modelName)
	default:
		return nil, domain.ErrValidation("update pre-aggregation requires a semantic model reference, name, and update request")
	}
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
func (s *Service) DeletePreAggregation(ctx context.Context, primary string, args ...string) error {
	if len(args) == 0 || len(args) > 2 {
		return domain.ErrValidation("delete pre-aggregation requires a semantic model reference and name")
	}
	preAggName := args[len(args)-1]
	semanticModel, err := s.resolveSemanticModel(ctx, primary, args[:len(args)-1]...)
	if err != nil {
		return err
	}
	existing, err := s.preAggs.GetByName(ctx, semanticModel.ID, preAggName)
	if err != nil {
		return err
	}
	return s.preAggs.Delete(ctx, existing.ID)
}

// MaterializePreAggregation rebuilds the target relation for a semantic pre-aggregation from semantic metadata.
func (s *Service) MaterializePreAggregation(ctx context.Context, principal, preAggregationID string) (*domain.SemanticPreAggregation, map[string]any, error) {
	if s.ddlExec == nil {
		return nil, nil, domain.ErrValidation("semantic pre-aggregation materializer is not configured")
	}

	preAgg, err := s.preAggs.GetByID(ctx, preAggregationID)
	if err != nil {
		return nil, nil, err
	}
	model, err := s.models.GetByID(ctx, preAgg.SemanticModelID)
	if err != nil {
		return nil, nil, err
	}

	req := MetricQueryRequest{
		SemanticModelID:   model.ID,
		Metrics:           append([]string(nil), preAgg.MetricSet...),
		Dimensions:        append([]string(nil), preAgg.DimensionSet...),
	}
	if grain := strings.TrimSpace(preAgg.Grain); grain != "" {
		req.TimeGrain = &grain
	}

	plan, err := s.explainMetricQuery(ctx, req, explainMetricQueryOptions{DisablePreAggregationID: preAgg.ID})
	if err != nil {
		return nil, nil, err
	}

	targetRelation, err := normalizeTargetRelation(preAgg.TargetRelation)
	if err != nil {
		return nil, nil, err
	}
	materializeSQL := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS %s", targetRelation, plan.GeneratedSQL)
	if err := s.ddlExec.ExecContext(ctx, materializeSQL); err != nil {
		return nil, nil, fmt.Errorf("materialize semantic pre-aggregation %q: %w", preAgg.Name, err)
	}

	return preAgg, map[string]any{
		"asset_type":      domain.AssetTypeSemanticPreAggregation,
		"principal":       principal,
		"pre_aggregation": preAgg.Name,
		"target_relation": preAgg.TargetRelation,
		"generated_sql":   plan.GeneratedSQL,
		"base_relation":   plan.BaseRelation,
	}, nil
}
