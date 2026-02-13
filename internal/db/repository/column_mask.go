package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// ColumnMaskRepo implements domain.ColumnMaskRepository using SQLite.
type ColumnMaskRepo struct {
	q *dbstore.Queries
}

// NewColumnMaskRepo creates a new ColumnMaskRepo.
func NewColumnMaskRepo(db *sql.DB) *ColumnMaskRepo {
	return &ColumnMaskRepo{q: dbstore.New(db)}
}

// Create inserts a new column mask into the database.
func (r *ColumnMaskRepo) Create(ctx context.Context, m *domain.ColumnMask) (*domain.ColumnMask, error) {
	row, err := r.q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID:             newID(),
		TableID:        m.TableID,
		ColumnName:     m.ColumnName,
		MaskExpression: m.MaskExpression,
		Description:    sql.NullString{String: m.Description, Valid: m.Description != ""},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.ColumnMaskFromDB(row), nil
}

// GetForTable returns a paginated list of column masks for a table.
func (r *ColumnMaskRepo) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	total, err := r.q.CountColumnMasksForTable(ctx, tableID)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListColumnMasksForTablePaginated(ctx, dbstore.ListColumnMasksForTablePaginatedParams{
		TableID: tableID,
		Limit:   int64(page.Limit()),
		Offset:  int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.ColumnMasksFromDB(rows), total, nil
}

// Delete removes a column mask by ID.
func (r *ColumnMaskRepo) Delete(ctx context.Context, id string) error {
	return r.q.DeleteColumnMask(ctx, id)
}

// Bind associates a column mask with a principal or group.
func (r *ColumnMaskRepo) Bind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return r.q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID:            newID(),
		ColumnMaskID:  b.ColumnMaskID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
		SeeOriginal:   boolToInt(b.SeeOriginal),
	})
}

// Unbind removes a column mask binding from a principal or group.
func (r *ColumnMaskRepo) Unbind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return r.q.UnbindColumnMask(ctx, dbstore.UnbindColumnMaskParams{
		ColumnMaskID:  b.ColumnMaskID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	})
}

// ListBindings returns all bindings for a column mask.
func (r *ColumnMaskRepo) ListBindings(ctx context.Context, maskID string) ([]domain.ColumnMaskBinding, error) {
	rows, err := r.q.GetColumnMaskBindingsForMask(ctx, maskID)
	if err != nil {
		return nil, err
	}
	return mapper.ColumnMaskBindingsFromDB(rows), nil
}

// GetForTableAndPrincipal returns column masks with bindings for a specific table and principal.
func (r *ColumnMaskRepo) GetForTableAndPrincipal(ctx context.Context, tableID, principalID string, principalType string) ([]domain.ColumnMaskWithBinding, error) {
	rows, err := r.q.GetColumnMaskForTableAndPrincipal(ctx, dbstore.GetColumnMaskForTableAndPrincipalParams{
		TableID:       tableID,
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, err
	}
	result := make([]domain.ColumnMaskWithBinding, len(rows))
	for i, row := range rows {
		result[i] = domain.ColumnMaskWithBinding{
			ColumnName:     row.ColumnName,
			MaskExpression: row.MaskExpression,
			SeeOriginal:    row.SeeOriginal != 0,
		}
	}
	return result, nil
}
