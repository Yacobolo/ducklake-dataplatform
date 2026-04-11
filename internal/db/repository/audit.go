package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/cuestore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// AuditRepo implements domain.AuditRepository using SQLite.
type AuditRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewAuditRepo creates a new AuditRepo.
func NewAuditRepo(db *sql.DB) *AuditRepo {
	return &AuditRepo{q: dbstore.New(db), db: db}
}

// Insert persists a new audit log entry.
func (r *AuditRepo) Insert(ctx context.Context, e *domain.AuditEntry) error {
	params := mapper.AuditEntriesToDBParams(e)
	return r.q.InsertAuditLog(ctx, params)
}

// List returns a filtered, paginated list of audit log entries.
func (r *AuditRepo) List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	limit := int64(filter.Page.Limit())
	offset := int64(filter.Page.Offset())

	total, err := r.q.CountAuditLogs(ctx, dbstore.CountAuditLogsParams{
		PrincipalName: mapper.NullStrFromPtr(filter.PrincipalName),
		Action:        mapper.NullStrFromPtr(filter.Action),
		Status:        mapper.NullStrFromPtr(filter.Status),
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListAuditLogs(ctx, dbstore.ListAuditLogsParams{
		PrincipalName: mapper.NullStrFromPtr(filter.PrincipalName),
		Action:        mapper.NullStrFromPtr(filter.Action),
		Status:        mapper.NullStrFromPtr(filter.Status),
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
