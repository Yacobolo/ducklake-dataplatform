package repository

import (
	"context"
	"database/sql"
	"fmt"

	dbstore "github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/db/mapper"
	"github.com/Yacobolo/quackstack/internal/domain"
)

// ColumnMaskRepo implements domain.ColumnMaskRepository using SQLite.
type ColumnMaskRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewColumnMaskRepo creates a new ColumnMaskRepo.
func NewColumnMaskRepo(db *sql.DB) *ColumnMaskRepo {
	return &ColumnMaskRepo{db: db, q: dbstore.New(db)}
}

// Create inserts a new column mask into the database.
func (r *ColumnMaskRepo) Create(ctx context.Context, m *domain.ColumnMask) (*domain.ColumnMask, error) {
	row, err := r.q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID:             newID(),
		TableID:        m.TableID,
		Name:           sql.NullString{String: m.Name, Valid: m.Name != ""},
		ColumnName:     m.ColumnName,
		MaskExpression: m.MaskExpression,
		Description:    sql.NullString{String: m.Description, Valid: m.Description != ""},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.ColumnMaskFromDB(row), nil
}

// GetByID returns a column mask by ID.
func (r *ColumnMaskRepo) GetByID(ctx context.Context, id string) (*domain.ColumnMask, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, table_id, column_name, mask_expression, description, created_at, name
		FROM column_masks
		WHERE id = ?
	`, id)
	item, err := scanColumnMask(row)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// List returns a paginated list of all column masks.
func (r *ColumnMaskRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM column_masks`).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, table_id, column_name, mask_expression, description, created_at, name
		FROM column_masks
		ORDER BY created_at DESC, id ASC
		LIMIT ? OFFSET ?
	`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer rows.Close() //nolint:errcheck

	items := make([]domain.ColumnMask, 0)
	for rows.Next() {
		item, err := scanColumnMask(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, mapDBError(err)
	}
	return items, total, nil
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

// Update applies partial changes to a column mask.
func (r *ColumnMaskRepo) Update(ctx context.Context, id string, req domain.UpdateColumnMaskRequest) (*domain.ColumnMask, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	name := current.Name
	if req.Name != nil {
		name = *req.Name
	}
	columnName := current.ColumnName
	if req.ColumnName != nil {
		columnName = *req.ColumnName
	}
	maskExpression := current.MaskExpression
	if req.MaskExpression != nil {
		maskExpression = *req.MaskExpression
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE column_masks
		SET name = ?, column_name = ?, mask_expression = ?, description = ?
		WHERE id = ?
	`, sql.NullString{String: name, Valid: name != ""}, columnName, maskExpression, sql.NullString{String: description, Valid: description != ""}, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return nil, domain.ErrNotFound("column mask %s not found", id)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a column mask by ID.
func (r *ColumnMaskRepo) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, "DELETE FROM column_masks WHERE id = ?", id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return domain.ErrNotFound("column mask %s not found", id)
	}
	return nil
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

func scanColumnMask(row interface{ Scan(dest ...any) error }) (*domain.ColumnMask, error) {
	var item dbstore.ColumnMask
	if err := row.Scan(
		&item.ID,
		&item.TableID,
		&item.ColumnName,
		&item.MaskExpression,
		&item.Description,
		&item.CreatedAt,
		&item.Name,
	); err != nil {
		return nil, mapDBError(err)
	}
	return mapper.ColumnMaskFromDB(item), nil
}
