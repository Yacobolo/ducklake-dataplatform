package repository

import (
	"context"
	"database/sql"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.SemanticMetricRepository = (*SemanticMetricRepo)(nil)

// SemanticMetricRepo implements SemanticMetricRepository using SQLite.
type SemanticMetricRepo struct {
	q *dbstore.Queries
}

// NewSemanticMetricRepo creates a new SemanticMetricRepo.
func NewSemanticMetricRepo(db *sql.DB) *SemanticMetricRepo {
	return &SemanticMetricRepo{q: dbstore.New(db)}
}

// Create inserts a new semantic metric.
func (r *SemanticMetricRepo) Create(ctx context.Context, m *domain.SemanticMetric) (*domain.SemanticMetric, error) {
	row, err := r.q.CreateSemanticMetric(ctx, dbstore.CreateSemanticMetricParams{
		ID:                 newID(),
		SemanticModelID:    m.SemanticModelID,
		Name:               m.Name,
		Description:        m.Description,
		MetricType:         m.MetricType,
		ExpressionMode:     m.ExpressionMode,
		Expression:         m.Expression,
		DefaultTimeGrain:   m.DefaultTimeGrain,
		Format:             m.Format,
		Owner:              m.Owner,
		CertificationState: m.CertificationState,
		CreatedBy:          m.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticMetricFromDB(row), nil
}

// GetByID returns a semantic metric by ID.
func (r *SemanticMetricRepo) GetByID(ctx context.Context, id string) (*domain.SemanticMetric, error) {
	row, err := r.q.GetSemanticMetricByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticMetricFromDB(row), nil
}

// GetByName returns a semantic metric by semantic model and name.
func (r *SemanticMetricRepo) GetByName(ctx context.Context, semanticModelID, name string) (*domain.SemanticMetric, error) {
	row, err := r.q.GetSemanticMetricByName(ctx, dbstore.GetSemanticMetricByNameParams{
		SemanticModelID: semanticModelID,
		Name:            name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticMetricFromDB(row), nil
}

// ListByModel lists all semantic metrics for a semantic model.
func (r *SemanticMetricRepo) ListByModel(ctx context.Context, semanticModelID string) ([]domain.SemanticMetric, error) {
	rows, err := r.q.ListSemanticMetricsByModel(ctx, semanticModelID)
	if err != nil {
		return nil, err
	}

	metrics := make([]domain.SemanticMetric, 0, len(rows))
	for _, row := range rows {
		metrics = append(metrics, *semanticMetricFromDB(row))
	}
	return metrics, nil
}

// Update applies partial updates to a semantic metric using read-modify-write.
func (r *SemanticMetricRepo) Update(ctx context.Context, id string, req domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	metricType := current.MetricType
	if req.MetricType != nil {
		metricType = *req.MetricType
	}
	expressionMode := current.ExpressionMode
	if req.ExpressionMode != nil {
		expressionMode = *req.ExpressionMode
	}
	expression := current.Expression
	if req.Expression != nil {
		expression = *req.Expression
	}
	defaultGrain := current.DefaultTimeGrain
	if req.DefaultTimeGrain != nil {
		defaultGrain = *req.DefaultTimeGrain
	}
	format := current.Format
	if req.Format != nil {
		format = *req.Format
	}
	owner := current.Owner
	if req.Owner != nil {
		owner = *req.Owner
	}
	certificationState := current.CertificationState
	if req.CertificationState != nil {
		certificationState = *req.CertificationState
	}

	err = r.q.UpdateSemanticMetric(ctx, dbstore.UpdateSemanticMetricParams{
		Description:        description,
		MetricType:         metricType,
		ExpressionMode:     expressionMode,
		Expression:         expression,
		DefaultTimeGrain:   defaultGrain,
		Format:             format,
		Owner:              owner,
		CertificationState: certificationState,
		ID:                 id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a semantic metric by ID.
func (r *SemanticMetricRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteSemanticMetric(ctx, id))
}
