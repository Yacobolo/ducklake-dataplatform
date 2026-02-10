package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type AuditRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewAuditRepo(db *sql.DB) *AuditRepo {
	return &AuditRepo{q: dbstore.New(db), db: db}
}

func (r *AuditRepo) Insert(ctx context.Context, e *domain.AuditEntry) error {
	params := mapper.AuditEntriesToDBParams(e)
	return r.q.InsertAuditLog(ctx, params)
}

func (r *AuditRepo) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	// Build filter params — use nil for "no filter" column
	var principalFilter interface{}
	var principalName string
	if filter.PrincipalName != nil {
		principalFilter = *filter.PrincipalName
		principalName = *filter.PrincipalName
	}

	var actionFilter interface{}
	var action string
	if filter.Action != nil {
		actionFilter = *filter.Action
		action = *filter.Action
	}

	var statusFilter interface{}
	var status string
	if filter.Status != nil {
		statusFilter = *filter.Status
		status = *filter.Status
	}

	limit := int64(filter.Page.Limit())
	offset := int64(filter.Page.Offset())

	// Count
	total, err := r.q.CountAuditLogs(ctx, dbstore.CountAuditLogsParams{
		Column1:       principalFilter,
		PrincipalName: principalName,
		Column3:       actionFilter,
		Action:        action,
		Column5:       statusFilter,
		Status:        status,
	})
	if err != nil {
		return nil, 0, err
	}

	// List
	rows, err := r.q.ListAuditLogs(ctx, dbstore.ListAuditLogsParams{
		Column1:       principalFilter,
		PrincipalName: principalName,
		Column3:       actionFilter,
		Action:        action,
		Column5:       statusFilter,
		Status:        status,
		Limit:         limit,
		Offset:        offset,
	})
	if err != nil {
		return nil, 0, err
	}

	entries := make([]domain.AuditEntry, len(rows))
	for i, row := range rows {
		entries[i] = *mapper.AuditEntryFromDB(row)
	}

	return entries, total, nil
}
