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
		ProjectName:          m.ProjectName,
		Name:                 m.Name,
		Description:          m.Description,
		Owner:                m.Owner,
		BaseModelRef:         m.BaseModelRef,
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

// GetByName returns a semantic model by project and name.
func (r *SemanticModelRepo) GetByName(ctx context.Context, projectName, name string) (*domain.SemanticModel, error) {
	row, err := r.q.GetSemanticModelByName(ctx, dbstore.GetSemanticModelByNameParams{
		ProjectName: projectName,
		Name:        name,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return semanticModelFromDB(row), nil
}

// List returns a paginated list of semantic models, optionally filtered by project.
func (r *SemanticModelRepo) List(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.SemanticModel, int64, error) {
	projectFilter := ""
	if projectName != nil {
		projectFilter = *projectName
	}

	total, err := r.q.CountSemanticModels(ctx, dbstore.CountSemanticModelsParams{
		Column1:     projectFilter,
		ProjectName: projectFilter,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListSemanticModels(ctx, dbstore.ListSemanticModelsParams{
		Column1:     projectFilter,
		ProjectName: projectFilter,
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
	baseModelRef := current.BaseModelRef
	if req.BaseModelRef != nil {
		baseModelRef = *req.BaseModelRef
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
		BaseModelRef:         baseModelRef,
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

// ListAll returns all semantic models ordered by project and name.
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
