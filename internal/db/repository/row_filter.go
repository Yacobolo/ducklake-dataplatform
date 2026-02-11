package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type RowFilterRepo struct {
	q *dbstore.Queries
}

func NewRowFilterRepo(db *sql.DB) *RowFilterRepo {
	return &RowFilterRepo{q: dbstore.New(db)}
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
	total, err := r.q.CountRowFiltersForTable(ctx, tableID)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListRowFiltersForTablePaginated(ctx, dbstore.ListRowFiltersForTablePaginatedParams{
		TableID: tableID,
		Limit:   int64(page.Limit()),
		Offset:  int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.RowFiltersFromDB(rows), total, nil
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
