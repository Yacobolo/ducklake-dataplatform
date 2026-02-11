package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type QueryHistoryRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewQueryHistoryRepo(db *sql.DB) *QueryHistoryRepo {
	return &QueryHistoryRepo{q: dbstore.New(db), db: db}
}

func (r *QueryHistoryRepo) List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
	var principalFilter interface{}
	var principalName string
	if filter.PrincipalName != nil {
		principalFilter = *filter.PrincipalName
		principalName = *filter.PrincipalName
	}

	var statusFilter interface{}
	var status string
	if filter.Status != nil {
		statusFilter = *filter.Status
		status = *filter.Status
	}

	var fromFilter interface{}
	var fromStr string
	if filter.From != nil {
		fromFilter = filter.From.Format("2006-01-02 15:04:05")
		fromStr = filter.From.Format("2006-01-02 15:04:05")
	}

	var toFilter interface{}
	var toStr string
	if filter.To != nil {
		toFilter = filter.To.Format("2006-01-02 15:04:05")
		toStr = filter.To.Format("2006-01-02 15:04:05")
	}

	limit := int64(filter.Page.Limit())
	offset := int64(filter.Page.Offset())

	total, err := r.q.CountQueryHistory(ctx, dbstore.CountQueryHistoryParams{
		Column1:       principalFilter,
		PrincipalName: principalName,
		Column3:       statusFilter,
		Status:        status,
		Column5:       fromFilter,
		CreatedAt:     fromStr,
		Column7:       toFilter,
		CreatedAt_2:   toStr,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListQueryHistory(ctx, dbstore.ListQueryHistoryParams{
		Column1:       principalFilter,
		PrincipalName: principalName,
		Column3:       statusFilter,
		Status:        status,
		Column5:       fromFilter,
		CreatedAt:     fromStr,
		Column7:       toFilter,
		CreatedAt_2:   toStr,
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
