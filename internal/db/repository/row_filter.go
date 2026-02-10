package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type RowFilterRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewRowFilterRepo(db *sql.DB) *RowFilterRepo {
	return &RowFilterRepo{q: dbstore.New(db), db: db}
}

func (r *RowFilterRepo) Create(ctx context.Context, f *domain.RowFilter) (*domain.RowFilter, error) {
	row, err := r.q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:     f.TableID,
		FilterSql:   f.FilterSQL,
		Description: sql.NullString{String: f.Description, Valid: f.Description != ""},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.RowFilterFromDB(row), nil
}

func (r *RowFilterRepo) GetForTable(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM row_filters WHERE table_id = ?`, tableID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.db.QueryContext(ctx,
		`SELECT id, table_id, filter_sql, description, created_at FROM row_filters WHERE table_id = ? ORDER BY id LIMIT ? OFFSET ?`,
		tableID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var filters []domain.RowFilter
	for rows.Next() {
		var f dbstore.RowFilter
		if err := rows.Scan(&f.ID, &f.TableID, &f.FilterSql, &f.Description, &f.CreatedAt); err != nil {
			return nil, 0, err
		}
		filters = append(filters, *mapper.RowFilterFromDB(f))
	}
	return filters, total, rows.Err()
}

func (r *RowFilterRepo) Delete(ctx context.Context, id int64) error {
	return r.q.DeleteRowFilter(ctx, id)
}

func (r *RowFilterRepo) Bind(ctx context.Context, b *domain.RowFilterBinding) error {
	return r.q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID:   b.RowFilterID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	})
}

func (r *RowFilterRepo) Unbind(ctx context.Context, b *domain.RowFilterBinding) error {
	return r.q.UnbindRowFilter(ctx, dbstore.UnbindRowFilterParams{
		RowFilterID:   b.RowFilterID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	})
}

func (r *RowFilterRepo) ListBindings(ctx context.Context, filterID int64) ([]domain.RowFilterBinding, error) {
	rows, err := r.q.GetRowFilterBindingsForFilter(ctx, filterID)
	if err != nil {
		return nil, err
	}
	return mapper.RowFilterBindingsFromDB(rows), nil
}

func (r *RowFilterRepo) GetForTableAndPrincipal(ctx context.Context, tableID, principalID int64, principalType string) ([]domain.RowFilter, error) {
	rows, err := r.q.GetRowFiltersForTableAndPrincipal(ctx, dbstore.GetRowFiltersForTableAndPrincipalParams{
		TableID:       tableID,
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, err
	}
	return mapper.RowFiltersFromDB(rows), nil
}
