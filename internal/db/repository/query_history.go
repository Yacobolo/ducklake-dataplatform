package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/cuestore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// QueryHistoryRepo implements domain.QueryHistoryRepository using SQLite.
type QueryHistoryRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewQueryHistoryRepo creates a new QueryHistoryRepo.
func NewQueryHistoryRepo(db *sql.DB) *QueryHistoryRepo {
	return &QueryHistoryRepo{q: dbstore.New(db), db: db}
}

// List returns a filtered, paginated list of query history entries.
func (r *QueryHistoryRepo) List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
	var fromStr *string
	if filter.From != nil {
		value := filter.From.Format("2006-01-02 15:04:05")
		fromStr = &value
	}

	var toStr *string
	if filter.To != nil {
		value := filter.To.Format("2006-01-02 15:04:05")
		toStr = &value
	}

	limit := int64(filter.Page.Limit())
	offset := int64(filter.Page.Offset())

	total, err := r.q.CountQueryHistory(ctx, dbstore.CountQueryHistoryParams{
		PrincipalName: mapper.NullStrFromPtr(filter.PrincipalName),
		Status:        mapper.NullStrFromPtr(filter.Status),
		CreatedAtFrom: mapper.NullStrFromPtr(fromStr),
		CreatedAtTo:   mapper.NullStrFromPtr(toStr),
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListQueryHistory(ctx, dbstore.ListQueryHistoryParams{
		PrincipalName: mapper.NullStrFromPtr(filter.PrincipalName),
		Status:        mapper.NullStrFromPtr(filter.Status),
		CreatedAtFrom: mapper.NullStrFromPtr(fromStr),
		CreatedAtTo:   mapper.NullStrFromPtr(toStr),
		Limit:         limit,
		Offset:        offset,
	})
	if err != nil {
		return nil, 0, err
	}

	entries := make([]domain.QueryHistoryEntry, len(rows))
	for i, row := range rows {
		entries[i] = *mapper.QueryHistoryEntryFromDB(row)
	}

	return entries, total, nil
}
