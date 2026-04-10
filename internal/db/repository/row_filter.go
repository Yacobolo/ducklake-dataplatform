package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// RowFilterRepo implements domain.RowFilterRepository using SQLite.
type RowFilterRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewRowFilterRepo creates a new RowFilterRepo.
func NewRowFilterRepo(db *sql.DB) *RowFilterRepo {
	return &RowFilterRepo{db: db, q: dbstore.New(db)}
}

// Create inserts a new row filter into the database.
func (r *RowFilterRepo) Create(ctx context.Context, f *domain.RowFilter) (*domain.RowFilter, error) {
	row, err := r.q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID:          newID(),
		TableID:     f.TableID,
		Name:        sql.NullString{String: f.Name, Valid: f.Name != ""},
		FilterSql:   f.FilterSQL,
		Description: sql.NullString{String: f.Description, Valid: f.Description != ""},
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.RowFilterFromDB(row), nil
}

// GetByID returns a row filter by ID.
func (r *RowFilterRepo) GetByID(ctx context.Context, id string) (*domain.RowFilter, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, table_id, filter_sql, description, created_at, name
		FROM row_filters
		WHERE id = ?
	`, id)
	item, err := scanRowFilter(row)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// List returns a paginated list of all row filters.
func (r *RowFilterRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM row_filters`).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT id, table_id, filter_sql, description, created_at, name
		FROM row_filters
		ORDER BY created_at DESC, id ASC
		LIMIT ? OFFSET ?
	`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer rows.Close() //nolint:errcheck

	items := make([]domain.RowFilter, 0)
	for rows.Next() {
		item, err := scanRowFilter(rows)
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

// GetForTable returns a paginated list of row filters for a table.
func (r *RowFilterRepo) GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.RowFilter, int64, error) {
	total, err := r.q.CountRowFiltersForTable(ctx, tableID)
	if err != nil {
		return nil, 0, mapDBError(err)
	}

	rows, err := r.q.ListRowFiltersForTablePaginated(ctx, dbstore.ListRowFiltersForTablePaginatedParams{
		TableID: tableID,
		Limit:   int64(page.Limit()),
		Offset:  int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, mapDBError(err)
	}

	return mapper.RowFiltersFromDB(rows), total, nil
}

// Update applies partial changes to a row filter.
func (r *RowFilterRepo) Update(ctx context.Context, id string, req domain.UpdateRowFilterRequest) (*domain.RowFilter, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	name := current.Name
	if req.Name != nil {
		name = *req.Name
	}
	filterSQL := current.FilterSQL
	if req.FilterSQL != nil {
		filterSQL = *req.FilterSQL
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE row_filters
		SET name = ?, filter_sql = ?, description = ?
		WHERE id = ?
	`, sql.NullString{String: name, Valid: name != ""}, filterSQL, sql.NullString{String: description, Valid: description != ""}, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, domain.ErrNotFound("row filter %s not found", id)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a row filter by ID. Returns NotFoundError if the filter does not exist.
func (r *RowFilterRepo) Delete(ctx context.Context, id string) error {
	result, err := r.q.DeleteRowFilter(ctx, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("row filter %s not found", id)
	}
	return nil
}

// Bind associates a row filter with a principal or group.
func (r *RowFilterRepo) Bind(ctx context.Context, b *domain.RowFilterBinding) error {
	err := r.q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID:            newID(),
		RowFilterID:   b.RowFilterID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	})
	return mapDBError(err)
}

// Unbind removes a row filter binding from a principal or group.
// Returns NotFoundError if the binding does not exist.
func (r *RowFilterRepo) Unbind(ctx context.Context, b *domain.RowFilterBinding) error {
	result, err := r.q.UnbindRowFilter(ctx, dbstore.UnbindRowFilterParams{
		RowFilterID:   b.RowFilterID,
		PrincipalID:   b.PrincipalID,
		PrincipalType: b.PrincipalType,
	})
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("row filter binding not found")
	}
	return nil
}

// ListBindings returns all bindings for a row filter.
func (r *RowFilterRepo) ListBindings(ctx context.Context, filterID string) ([]domain.RowFilterBinding, error) {
	rows, err := r.q.GetRowFilterBindingsForFilter(ctx, filterID)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.RowFilterBindingsFromDB(rows), nil
}

// GetForTableAndPrincipal returns row filters bound to a specific table and principal.
func (r *RowFilterRepo) GetForTableAndPrincipal(ctx context.Context, tableID, principalID string, principalType string) ([]domain.RowFilter, error) {
	rows, err := r.q.GetRowFiltersForTableAndPrincipal(ctx, dbstore.GetRowFiltersForTableAndPrincipalParams{
		TableID:       tableID,
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.RowFiltersFromDB(rows), nil
}

func scanRowFilter(row interface{ Scan(dest ...any) error }) (*domain.RowFilter, error) {
	var item dbstore.RowFilter
	if err := row.Scan(
		&item.ID,
		&item.TableID,
		&item.FilterSql,
		&item.Description,
		&item.CreatedAt,
		&item.Name,
	); err != nil {
		return nil, mapDBError(err)
	}
	return mapper.RowFilterFromDB(item), nil
}
