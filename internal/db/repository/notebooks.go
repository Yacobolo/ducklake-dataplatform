package repository

import (
	"context"
	"database/sql"
	"fmt"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

var _ domain.NotebookRepository = (*NotebookRepo)(nil)

// NotebookRepo implements domain.NotebookRepository using sqlc-generated queries.
type NotebookRepo struct {
	q *dbstore.Queries
}

// NewNotebookRepo creates a new NotebookRepo.
func NewNotebookRepo(db *sql.DB) *NotebookRepo {
	return &NotebookRepo{q: dbstore.New(db)}
}

// CreateNotebook inserts a new notebook.
func (r *NotebookRepo) CreateNotebook(ctx context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
	row, err := r.q.CreateNotebook(ctx, dbstore.CreateNotebookParams{
		ID:          domain.NewID(),
		Name:        nb.Name,
		Description: mapper.NullStrFromPtr(nb.Description),
		Owner:       nb.Owner,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookFromDB(row), nil
}

// GetNotebook returns a notebook by its ID.
func (r *NotebookRepo) GetNotebook(ctx context.Context, id string) (*domain.Notebook, error) {
	row, err := r.q.GetNotebook(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookFromDB(row), nil
}

// ListNotebooks returns a paginated list of notebooks, optionally filtered by owner.
func (r *NotebookRepo) ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	total, err := r.q.CountNotebooks(ctx, mapper.InterfaceFromPtr(owner))
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListNotebooks(ctx, dbstore.ListNotebooksParams{
		Owner:  mapper.InterfaceFromPtr(owner),
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.NotebooksFromDB(rows), total, nil
}

// UpdateNotebook applies partial updates to an existing notebook.
func (r *NotebookRepo) UpdateNotebook(ctx context.Context, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	existing, err := r.q.GetNotebook(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}

	name := existing.Name
	if req.Name != nil {
		name = *req.Name
	}

	description := existing.Description
	if req.Description != nil {
		description = mapper.NullStrFromPtr(req.Description)
	}

	row, err := r.q.UpdateNotebook(ctx, dbstore.UpdateNotebookParams{
		Name:        name,
		Description: description,
		ID:          id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookFromDB(row), nil
}

// DeleteNotebook removes a notebook by ID.
func (r *NotebookRepo) DeleteNotebook(ctx context.Context, id string) error {
	return r.q.DeleteNotebook(ctx, id)
}

// CreateCell inserts a new cell into a notebook.
// A position of -1 means "auto-assign to end". Any other value (including 0)
// is treated as an explicit position.
func (r *NotebookRepo) CreateCell(ctx context.Context, cell *domain.Cell) (*domain.Cell, error) {
	position := int64(cell.Position)
	if cell.Position < 0 {
		maxPos, err := r.GetMaxPosition(ctx, cell.NotebookID)
		if err != nil {
			return nil, fmt.Errorf("get max position: %w", err)
		}
		position = int64(maxPos + 1)
	}

	row, err := r.q.CreateCell(ctx, dbstore.CreateCellParams{
		ID:         domain.NewID(),
		NotebookID: cell.NotebookID,
		CellType:   string(cell.CellType),
		Content:    cell.Content,
		Position:   position,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CellFromDB(row), nil
}

// GetCell returns a cell by its ID.
func (r *NotebookRepo) GetCell(ctx context.Context, id string) (*domain.Cell, error) {
	row, err := r.q.GetCell(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CellFromDB(row), nil
}

// ListCells returns all cells for a notebook, ordered by position.
func (r *NotebookRepo) ListCells(ctx context.Context, notebookID string) ([]domain.Cell, error) {
	rows, err := r.q.ListCells(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	return mapper.CellsFromDB(rows), nil
}

// UpdateCell applies partial updates to an existing cell.
func (r *NotebookRepo) UpdateCell(ctx context.Context, id string, req domain.UpdateCellRequest) (*domain.Cell, error) {
	existing, err := r.q.GetCell(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}

	content := existing.Content
	if req.Content != nil {
		content = *req.Content
	}

	position := existing.Position
	if req.Position != nil {
		position = int64(*req.Position)
	}

	row, err := r.q.UpdateCell(ctx, dbstore.UpdateCellParams{
		Content:  content,
		Position: position,
		ID:       id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.CellFromDB(row), nil
}

// DeleteCell removes a cell by ID.
func (r *NotebookRepo) DeleteCell(ctx context.Context, id string) error {
	return r.q.DeleteCell(ctx, id)
}

// UpdateCellResult updates the last_result field of a cell.
func (r *NotebookRepo) UpdateCellResult(ctx context.Context, cellID string, result *string) error {
	return r.q.UpdateCellResult(ctx, dbstore.UpdateCellResultParams{
		LastResult: mapper.NullStrFromPtr(result),
		ID:         cellID,
	})
}

// ReorderCells reorders cells within a notebook by assigning new positions
// based on the order of the provided cell IDs.
func (r *NotebookRepo) ReorderCells(ctx context.Context, notebookID string, cellIDs []string) error {
	// Validate all cell IDs belong to the notebook
	cells, err := r.q.ListCells(ctx, notebookID)
	if err != nil {
		return fmt.Errorf("list cells: %w", err)
	}

	cellSet := make(map[string]bool, len(cells))
	for _, c := range cells {
		cellSet[c.ID] = true
	}

	for _, id := range cellIDs {
		if !cellSet[id] {
			return &domain.ValidationError{Message: fmt.Sprintf("cell %q does not belong to notebook %q", id, notebookID)}
		}
	}

	// Update positions in order
	for i, id := range cellIDs {
		if err := r.q.UpdateCellPosition(ctx, dbstore.UpdateCellPositionParams{
			Position: int64(i),
			ID:       id,
		}); err != nil {
			return fmt.Errorf("update cell position: %w", err)
		}
	}
	return nil
}

// GetMaxPosition returns the maximum cell position in a notebook.
// Returns -1 if the notebook has no cells.
func (r *NotebookRepo) GetMaxPosition(ctx context.Context, notebookID string) (int, error) {
	result, err := r.q.GetMaxCellPosition(ctx, notebookID)
	if err != nil {
		return 0, err
	}
	// The COALESCE returns an interface{}, handle type assertion
	switch v := result.(type) {
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	default:
		return -1, nil
	}
}
