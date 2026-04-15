package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/domain"
)

// Compile-time check.
var _ domain.SemanticModelRepository = (*SemanticModelRepo)(nil)

// SemanticModelRepo implements SemanticModelRepository using SQLite.
type SemanticModelRepo struct {
	q *dbstore.Queries
}

// NewSemanticModelRepo creates a new SemanticModelRepo.
func NewSemanticModelRepo(db *sql.DB) *SemanticModelRepo {
	return &SemanticModelRepo{q: dbstore.New(db)}
}

// Create inserts a new semantic model.
func (r *SemanticModelRepo) Create(ctx context.Context, m *domain.SemanticModel) (*domain.SemanticModel, error) {
	tagsJSON, err := json.Marshal(m.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}

	row, err := r.q.CreateSemanticModel(ctx, dbstore.CreateSemanticModelParams{
		ID:                   newID(),
		WorkspaceID:          m.WorkspaceID,
		Name:                 m.Name,
		Description:          m.Description,
		Owner:                m.Owner,
		BaseRelationRef:      m.BaseRelationRef,
		DefaultTimeDimension: m.DefaultTimeDimension,
		Tags:                 string(tagsJSON),
		CreatedBy:            m.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticModelFromDB(row), nil
}

// GetByID returns a semantic model by ID.
func (r *SemanticModelRepo) GetByID(ctx context.Context, id string) (*domain.SemanticModel, error) {
	row, err := r.q.GetSemanticModelByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticModelFromDB(row), nil
}

// GetByWorkspaceAndName returns a semantic model by workspace/name.
func (r *SemanticModelRepo) GetByWorkspaceAndName(ctx context.Context, workspaceID, name string) (*domain.SemanticModel, error) {
	row, err := r.q.GetSemanticModelByWorkspaceAndName(ctx, dbstore.GetSemanticModelByWorkspaceAndNameParams{
		WorkspaceID: workspaceID,
		Name:        name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticModelFromDB(row), nil
}

// ListByWorkspace returns a paginated list of semantic models for a workspace.
func (r *SemanticModelRepo) ListByWorkspace(ctx context.Context, workspaceID string, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
	total, err := r.q.CountSemanticModelsByWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListSemanticModelsByWorkspace(ctx, dbstore.ListSemanticModelsByWorkspaceParams{
		WorkspaceID: workspaceID,
		Limit:       int64(page.Limit()),
		Offset:      int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	models := make([]domain.SemanticModel, 0, len(rows))
	for _, row := range rows {
		models = append(models, *semanticModelFromDB(row))
	}
	return models, total, nil
}

// Update applies partial updates to a semantic model using read-modify-write.
func (r *SemanticModelRepo) Update(ctx context.Context, id string, req domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	owner := current.Owner
	if req.Owner != nil {
		owner = *req.Owner
	}
	baseRelationRef := current.BaseRelationRef
	if req.BaseRelationRef != nil {
		baseRelationRef = *req.BaseRelationRef
	}
	defaultTimeDim := current.DefaultTimeDimension
	if req.DefaultTimeDimension != nil {
		defaultTimeDim = *req.DefaultTimeDimension
	}
	tags := current.Tags
	if req.Tags != nil {
		tags = req.Tags
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("marshal tags: %w", err)
	}

	err = r.q.UpdateSemanticModel(ctx, dbstore.UpdateSemanticModelParams{
		Description:          description,
		Owner:                owner,
		BaseRelationRef:      baseRelationRef,
		DefaultTimeDimension: defaultTimeDim,
		Tags:                 string(tagsJSON),
		ID:                   id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a semantic model by ID.
func (r *SemanticModelRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteSemanticModel(ctx, id))
}

// ListAllByWorkspace returns all semantic models ordered by name for a workspace.
func (r *SemanticModelRepo) ListAllByWorkspace(ctx context.Context, workspaceID string) ([]domain.SemanticModel, error) {
	rows, err := r.q.ListAllSemanticModelsByWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, err
	}

	models := make([]domain.SemanticModel, 0, len(rows))
	for _, row := range rows {
		models = append(models, *semanticModelFromDB(row))
	}
	return models, nil
}

// ListAll returns all semantic models ordered by workspace and name.
func (r *SemanticModelRepo) ListAll(ctx context.Context) ([]domain.SemanticModel, error) {
	rows, err := r.q.ListAllSemanticModels(ctx)
	if err != nil {
		return nil, err
	}

	models := make([]domain.SemanticModel, 0, len(rows))
	for _, row := range rows {
		models = append(models, *semanticModelFromDB(row))
	}
	return models, nil
}
