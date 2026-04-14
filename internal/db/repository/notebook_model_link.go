package repository

import (
	"context"
	"database/sql"

	"github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/db/mapper"
	"github.com/Yacobolo/quackstack/internal/domain"
)

var _ domain.NotebookModelLinkRepository = (*NotebookModelLinkRepo)(nil)

// NotebookModelLinkRepo implements domain.NotebookModelLinkRepository using sqlc queries.
type NotebookModelLinkRepo struct {
	q *dbstore.Queries
}

// NewNotebookModelLinkRepo creates a new NotebookModelLinkRepo.
func NewNotebookModelLinkRepo(db *sql.DB) *NotebookModelLinkRepo {
	return &NotebookModelLinkRepo{q: dbstore.New(db)}
}

// Upsert creates or updates a notebook-model link for a notebook.
func (r *NotebookModelLinkRepo) Upsert(ctx context.Context, link *domain.NotebookModelLink) error {
	return mapDBError(r.q.UpsertNotebookModelLink(ctx, dbstore.UpsertNotebookModelLinkParams{
		ID:           newID(),
		NotebookID:   link.NotebookID,
		ModelID:      link.ModelID,
		OutputCellID: link.OutputCellID,
	}))
}

// GetByNotebookID returns a link by notebook ID.
func (r *NotebookModelLinkRepo) GetByNotebookID(ctx context.Context, notebookID string) (*domain.NotebookModelLink, error) {
	row, err := r.q.GetNotebookModelLinkByNotebookID(ctx, notebookID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookModelLinkFromDB(row), nil
}

// GetByModelID returns a link by model ID.
func (r *NotebookModelLinkRepo) GetByModelID(ctx context.Context, modelID string) (*domain.NotebookModelLink, error) {
	row, err := r.q.GetNotebookModelLinkByModelID(ctx, modelID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookModelLinkFromDB(row), nil
}

// DeleteByNotebookID deletes a link by notebook ID.
func (r *NotebookModelLinkRepo) DeleteByNotebookID(ctx context.Context, notebookID string) error {
	return mapDBError(r.q.DeleteNotebookModelLinkByNotebookID(ctx, notebookID))
}
