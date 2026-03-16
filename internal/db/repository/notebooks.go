package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

var _ domain.NotebookRepository = (*NotebookRepo)(nil)

// NotebookRepo implements domain.NotebookRepository using sqlc-generated queries.
type NotebookRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewNotebookRepo creates a new NotebookRepo.
func NewNotebookRepo(db *sql.DB) *NotebookRepo {
	return &NotebookRepo{db: db, q: dbstore.New(db)}
}

// CreateNotebook inserts a new notebook.
func (r *NotebookRepo) CreateNotebook(ctx context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
	notebookID := nb.ID
	if notebookID == "" {
		notebookID = domain.NewID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO notebooks (id, name, description, owner, git_repo_id, git_path)
		VALUES (?, ?, ?, ?, ?, ?)
	`, notebookID, nb.Name, mapper.NullStrFromPtr(nb.Description), nb.Owner, mapper.NullStrFromPtr(nb.GitRepoID), mapper.NullStrFromPtr(nb.GitPath))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetNotebook(ctx, notebookID)
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

// UpdateNotebookSync applies Git-backed notebook metadata updates.
func (r *NotebookRepo) UpdateNotebookSync(ctx context.Context, nb *domain.Notebook) (*domain.Notebook, error) {
	if nb == nil {
		return nil, domain.ErrValidation("notebook is required")
	}
	if nb.ID == "" {
		return nil, domain.ErrValidation("notebook id is required")
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE notebooks
		SET name = ?, description = ?, owner = ?, git_repo_id = ?, git_path = ?, updated_at = datetime('now')
		WHERE id = ?
	`, nb.Name, mapper.NullStrFromPtr(nb.Description), nb.Owner, mapper.NullStrFromPtr(nb.GitRepoID), mapper.NullStrFromPtr(nb.GitPath), nb.ID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetNotebook(ctx, nb.ID)
}

// DeleteNotebook removes a notebook by ID.
func (r *NotebookRepo) DeleteNotebook(ctx context.Context, id string) error {
	return r.q.DeleteNotebook(ctx, id)
}

// CreateCell inserts a new cell into a notebook.
// A position of -1 means "auto-assign to end". Any other value (including 0)
// is treated as an explicit position.
func (r *NotebookRepo) CreateCell(ctx context.Context, cell *domain.Cell) (*domain.Cell, error) {
	role := cell.Role
	if role == "" {
		if cell.CellType == domain.CellTypeMarkdown {
			role = domain.CellRoleMarkdown
		} else {
			role = domain.CellRoleTransform
		}
	}
	testCfg := "{}"
	if cell.Test != nil {
		b, err := json.Marshal(cell.Test)
		if err != nil {
			return nil, fmt.Errorf("marshal test config: %w", err)
		}
		testCfg = string(b)
	}
	visualSpec := ""
	if cell.VisualSpec != nil {
		b, err := json.Marshal(cell.VisualSpec)
		if err != nil {
			return nil, fmt.Errorf("marshal visual spec: %w", err)
		}
		visualSpec = string(b)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := r.q.WithTx(tx)
	position := int64(cell.Position)
	if cell.Position < 0 {
		maxPos, err := r.getMaxPositionWithQuerier(ctx, qtx, cell.NotebookID)
		if err != nil {
			return nil, fmt.Errorf("get max position: %w", err)
		}
		position = int64(maxPos + 1)
	} else {
		if err := shiftPositionsForInsert(ctx, tx, cell.NotebookID, position); err != nil {
			return nil, err
		}
	}

	row, err := qtx.CreateCell(ctx, dbstore.CreateCellParams{
		ID:         domain.NewID(),
		NotebookID: cell.NotebookID,
		CellType:   string(cell.CellType),
		Name:       mapper.NullStrFromPtr(cell.Name),
		Role:       string(role),
		Disabled:   boolToInt64(cell.Disabled),
		TestConfig: testCfg,
		VisualSpec: visualSpec,
		Content:    cell.Content,
		Position:   position,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit create cell: %w", err)
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
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := r.q.WithTx(tx)
	existing, err := qtx.GetCell(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}

	content := existing.Content
	if req.Content != nil {
		content = *req.Content
	}

	name := existing.Name
	if req.Name != nil {
		name = mapper.NullStrFromPtr(req.Name)
	}

	role := existing.Role
	if req.Role != nil {
		role = string(*req.Role)
	}

	disabled := existing.Disabled
	if req.Disabled != nil {
		disabled = boolToInt64(*req.Disabled)
	}

	testConfig := existing.TestConfig
	if req.Test != nil {
		b, err := json.Marshal(req.Test)
		if err != nil {
			return nil, fmt.Errorf("marshal test config: %w", err)
		}
		testConfig = string(b)
	} else if req.Role != nil && *req.Role != domain.CellRoleTest {
		testConfig = "{}"
	}
	visualSpec := existing.VisualSpec
	if req.VisualSpec != nil {
		b, err := json.Marshal(req.VisualSpec)
		if err != nil {
			return nil, fmt.Errorf("marshal visual spec: %w", err)
		}
		visualSpec = string(b)
	}

	position := existing.Position
	if req.Position != nil {
		nextPosition := int64(*req.Position)
		if nextPosition != existing.Position {
			if err := shiftPositionsForMove(ctx, tx, existing.NotebookID, existing.ID, existing.Position, nextPosition); err != nil {
				return nil, err
			}
			position = nextPosition
		}
	}

	row, err := qtx.UpdateCell(ctx, dbstore.UpdateCellParams{
		Name:       name,
		Role:       role,
		Disabled:   disabled,
		TestConfig: testConfig,
		VisualSpec: visualSpec,
		Content:    content,
		Position:   position,
		ID:         id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit update cell: %w", err)
	}
	return mapper.CellFromDB(row), nil
}

// UpdateCellSync replaces persisted cell state for Git-backed sync while preserving the cell ID.
func (r *NotebookRepo) UpdateCellSync(ctx context.Context, cell *domain.Cell) (*domain.Cell, error) {
	if cell == nil {
		return nil, domain.ErrValidation("cell is required")
	}
	if cell.ID == "" {
		return nil, domain.ErrValidation("cell id is required")
	}
	role := cell.Role
	if role == "" {
		if cell.CellType == domain.CellTypeMarkdown {
			role = domain.CellRoleMarkdown
		} else {
			role = domain.CellRoleTransform
		}
	}
	testCfg := "{}"
	if cell.Test != nil {
		b, err := json.Marshal(cell.Test)
		if err != nil {
			return nil, fmt.Errorf("marshal test config: %w", err)
		}
		testCfg = string(b)
	}
	visualSpec := ""
	if cell.VisualSpec != nil {
		b, err := json.Marshal(cell.VisualSpec)
		if err != nil {
			return nil, fmt.Errorf("marshal visual spec: %w", err)
		}
		visualSpec = string(b)
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE cells
		SET cell_type = ?, name = ?, role = ?, disabled = ?, test_config = ?, visual_spec = ?, content = ?, position = ?, updated_at = datetime('now')
		WHERE id = ?
	`, string(cell.CellType), mapper.NullStrFromPtr(cell.Name), string(role), boolToInt64(cell.Disabled), testCfg, visualSpec, cell.Content, cell.Position, cell.ID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetCell(ctx, cell.ID)
}

// DeleteCell removes a cell by ID.
func (r *NotebookRepo) DeleteCell(ctx context.Context, id string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	qtx := r.q.WithTx(tx)
	cell, err := qtx.GetCell(ctx, id)
	if err != nil {
		return mapDBError(err)
	}
	if linked, err := isProtectedNotebookOutputCell(ctx, tx, id); err != nil {
		return err
	} else if linked {
		return domain.ErrValidation("cannot delete notebook output cell %q while it is published to a model", id)
	}

	result, err := tx.ExecContext(ctx, "DELETE FROM cells WHERE id = ?", id)
	if err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
			return domain.ErrValidation("cannot delete notebook cell %q because it is still referenced", id)
		}
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete cell rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return domain.ErrNotFound("cell %s not found", id)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE cells SET position = position - 1, updated_at = datetime('now') WHERE notebook_id = ? AND position > ?", cell.NotebookID, cell.Position); err != nil {
		return fmt.Errorf("normalize positions after delete: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete cell: %w", err)
	}
	return nil
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
	return r.getMaxPositionWithQuerier(ctx, r.q, notebookID)
}

func (r *NotebookRepo) getMaxPositionWithQuerier(ctx context.Context, q maxCellPositionQuerier, notebookID string) (int, error) {
	result, err := q.GetMaxCellPosition(ctx, notebookID)
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

type maxCellPositionQuerier interface {
	GetMaxCellPosition(ctx context.Context, notebookID string) (interface{}, error)
}

func shiftPositionsForInsert(ctx context.Context, tx *sql.Tx, notebookID string, position int64) error {
	if _, err := tx.ExecContext(ctx, "UPDATE cells SET position = position + 1, updated_at = datetime('now') WHERE notebook_id = ? AND position >= ?", notebookID, position); err != nil {
		return fmt.Errorf("shift positions for insert: %w", err)
	}
	return nil
}

func shiftPositionsForMove(ctx context.Context, tx *sql.Tx, notebookID, cellID string, fromPosition, toPosition int64) error {
	if toPosition < fromPosition {
		if _, err := tx.ExecContext(ctx, "UPDATE cells SET position = position + 1, updated_at = datetime('now') WHERE notebook_id = ? AND id <> ? AND position >= ? AND position < ?", notebookID, cellID, toPosition, fromPosition); err != nil {
			return fmt.Errorf("shift positions up for move: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, "UPDATE cells SET position = position - 1, updated_at = datetime('now') WHERE notebook_id = ? AND id <> ? AND position > ? AND position <= ?", notebookID, cellID, fromPosition, toPosition); err != nil {
		return fmt.Errorf("shift positions down for move: %w", err)
	}
	return nil
}

func isProtectedNotebookOutputCell(ctx context.Context, tx *sql.Tx, cellID string) (bool, error) {
	row := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM notebook_model_links WHERE output_cell_id = ?)", cellID)
	var exists bool
	if err := row.Scan(&exists); err != nil {
		return false, fmt.Errorf("check notebook output links: %w", err)
	}
	return exists, nil
}
