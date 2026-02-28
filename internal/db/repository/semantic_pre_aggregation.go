package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.SemanticPreAggregationRepository = (*SemanticPreAggregationRepo)(nil)

// SemanticPreAggregationRepo implements SemanticPreAggregationRepository using SQLite.
type SemanticPreAggregationRepo struct {
	q *dbstore.Queries
}

// NewSemanticPreAggregationRepo creates a new SemanticPreAggregationRepo.
func NewSemanticPreAggregationRepo(db *sql.DB) *SemanticPreAggregationRepo {
	return &SemanticPreAggregationRepo{q: dbstore.New(db)}
}

// Create inserts a new semantic pre-aggregation.
func (r *SemanticPreAggregationRepo) Create(ctx context.Context, p *domain.SemanticPreAggregation) (*domain.SemanticPreAggregation, error) {
	metricSetJSON, err := json.Marshal(p.MetricSet)
	if err != nil {
		return nil, fmt.Errorf("marshal metric_set: %w", err)
	}
	dimensionSetJSON, err := json.Marshal(p.DimensionSet)
	if err != nil {
		return nil, fmt.Errorf("marshal dimension_set: %w", err)
	}

	row, err := r.q.CreateSemanticPreAggregation(ctx, dbstore.CreateSemanticPreAggregationParams{
		ID:              newID(),
		SemanticModelID: p.SemanticModelID,
		Name:            p.Name,
		MetricSet:       string(metricSetJSON),
		DimensionSet:    string(dimensionSetJSON),
		Grain:           p.Grain,
		TargetRelation:  p.TargetRelation,
		RefreshPolicy:   p.RefreshPolicy,
		CreatedBy:       p.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticPreAggregationFromDB(row), nil
}

// GetByID returns a semantic pre-aggregation by ID.
func (r *SemanticPreAggregationRepo) GetByID(ctx context.Context, id string) (*domain.SemanticPreAggregation, error) {
	row, err := r.q.GetSemanticPreAggregationByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticPreAggregationFromDB(row), nil
}

// GetByName returns a semantic pre-aggregation by semantic model and name.
func (r *SemanticPreAggregationRepo) GetByName(ctx context.Context, semanticModelID, name string) (*domain.SemanticPreAggregation, error) {
	row, err := r.q.GetSemanticPreAggregationByName(ctx, dbstore.GetSemanticPreAggregationByNameParams{
		SemanticModelID: semanticModelID,
		Name:            name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticPreAggregationFromDB(row), nil
}

// ListByModel lists all semantic pre-aggregations for a semantic model.
func (r *SemanticPreAggregationRepo) ListByModel(ctx context.Context, semanticModelID string) ([]domain.SemanticPreAggregation, error) {
	rows, err := r.q.ListSemanticPreAggregationsByModel(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}

	preAggs := make([]domain.SemanticPreAggregation, 0, len(rows))
	for _, row := range rows {
		preAggs = append(preAggs, *semanticPreAggregationFromDB(row))
	}
	return preAggs, nil
}

// Update applies partial updates to a semantic pre-aggregation using read-modify-write.
func (r *SemanticPreAggregationRepo) Update(ctx context.Context, id string, req domain.UpdateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	metricSet := current.MetricSet
	if req.MetricSet != nil {
		metricSet = req.MetricSet
	}
	dimensionSet := current.DimensionSet
	if req.DimensionSet != nil {
		dimensionSet = req.DimensionSet
	}
	grain := current.Grain
	if req.Grain != nil {
		grain = *req.Grain
	}
	targetRelation := current.TargetRelation
	if req.TargetRelation != nil {
		targetRelation = *req.TargetRelation
	}
	refreshPolicy := current.RefreshPolicy
	if req.RefreshPolicy != nil {
		refreshPolicy = *req.RefreshPolicy
	}

	metricSetJSON, err := json.Marshal(metricSet)
	if err != nil {
		return nil, fmt.Errorf("marshal metric_set: %w", err)
	}
	dimensionSetJSON, err := json.Marshal(dimensionSet)
	if err != nil {
		return nil, fmt.Errorf("marshal dimension_set: %w", err)
	}

	err = r.q.UpdateSemanticPreAggregation(ctx, dbstore.UpdateSemanticPreAggregationParams{
		MetricSet:      string(metricSetJSON),
		DimensionSet:   string(dimensionSetJSON),
		Grain:          grain,
		TargetRelation: targetRelation,
		RefreshPolicy:  refreshPolicy,
		ID:             id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a semantic pre-aggregation by ID.
func (r *SemanticPreAggregationRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteSemanticPreAggregation(ctx, id))
}
