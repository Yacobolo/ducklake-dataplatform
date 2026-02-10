package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/db/catalog"
	"duck-demo/domain"
	"duck-demo/internal/mapper"
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

func (r *RowFilterRepo) GetForTable(ctx context.Context, tableID int64) ([]domain.RowFilter, error) {
	rows, err := r.q.GetRowFiltersForTable(ctx, tableID)
	if err != nil {
		return nil, err
	}
	return mapper.RowFiltersFromDB(rows), nil
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
