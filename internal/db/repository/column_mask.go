package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type ColumnMaskRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewColumnMaskRepo(db *sql.DB) *ColumnMaskRepo {
	return &ColumnMaskRepo{q: dbstore.New(db), db: db}
}

func (r *ColumnMaskRepo) Create(ctx context.Context, m *domain.ColumnMask) (*domain.ColumnMask, error) {
	row, err := r.q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
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

func (r *ColumnMaskRepo) GetForTable(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.ColumnMask, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM column_masks WHERE table_id = ?`, tableID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, table_id, column_name, mask_expression, description, created_at FROM column_masks WHERE table_id = ? ORDER BY id LIMIT ? OFFSET ?`,
		tableID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var masks []domain.ColumnMask
	for rows.Next() {
		var m dbstore.ColumnMask
		if err := rows.Scan(&m.ID, &m.TableID, &m.ColumnName, &m.MaskExpression, &m.Description, &m.CreatedAt); err != nil {
			return nil, 0, err
		}
		masks = append(masks, *mapper.ColumnMaskFromDB(m))
	}
	return masks, total, rows.Err()
}

func (r *ColumnMaskRepo) Delete(ctx context.Context, id int64) error {
	return r.q.DeleteColumnMask(ctx, id)
}

func (r *ColumnMaskRepo) Bind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return r.q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID:  b.ColumnMaskID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
		SeeOriginal:   boolToInt(b.SeeOriginal),
	})
}

func (r *ColumnMaskRepo) Unbind(ctx context.Context, b *domain.ColumnMaskBinding) error {
	return r.q.UnbindColumnMask(ctx, dbstore.UnbindColumnMaskParams{
		ColumnMaskID:  b.ColumnMaskID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	})
}

func (r *ColumnMaskRepo) ListBindings(ctx context.Context, maskID int64) ([]domain.ColumnMaskBinding, error) {
	rows, err := r.q.GetColumnMaskBindingsForMask(ctx, maskID)
	if err != nil {
		return nil, err
	}
	return mapper.ColumnMaskBindingsFromDB(rows), nil
}

func (r *ColumnMaskRepo) GetForTableAndPrincipal(ctx context.Context, tableID, principalID int64, principalType string) ([]domain.ColumnMaskWithBinding, error) {
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
