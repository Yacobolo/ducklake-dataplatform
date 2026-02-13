package repository

import (
	"context"
	"database/sql"
	"time"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

// LineageRepo implements domain.LineageRepository using SQLite.
type LineageRepo struct {
	q *dbstore.Queries
}

// NewLineageRepo creates a new LineageRepo.
func NewLineageRepo(db *sql.DB) *LineageRepo {
	return &LineageRepo{q: dbstore.New(db)}
}

// InsertEdge records a new lineage edge between tables.
func (r *LineageRepo) InsertEdge(ctx context.Context, edge *domain.LineageEdge) error {
	return r.q.InsertLineageEdge(ctx, dbstore.InsertLineageEdgeParams{
		ID:            newID(),
		SourceTable:   edge.SourceTable,
		TargetTable:   mapper.NullStrFromPtr(edge.TargetTable),
		EdgeType:      edge.EdgeType,
		PrincipalName: edge.PrincipalName,
		QueryHash:     mapper.NullStrFromPtr(edge.QueryHash),
		SourceSchema:  mapper.NullStrFromStr(edge.SourceSchema),
		TargetSchema:  mapper.NullStrFromStr(edge.TargetSchema),
	})
}

// GetUpstream returns a paginated list of upstream lineage edges for a table.
func (r *LineageRepo) GetUpstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	total, err := r.q.CountUpstreamLineage(ctx, sql.NullString{String: tableName, Valid: true})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.GetUpstreamLineage(ctx, dbstore.GetUpstreamLineageParams{
		TargetTable: sql.NullString{String: tableName, Valid: true},
		Limit:       int64(page.Limit()),
		Offset:      int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	edges := make([]domain.LineageEdge, len(rows))
	for i, row := range rows {
		edges[i] = *mapper.LineageEdgeFromDB(row)
	}
	return edges, total, nil
}

// GetDownstream returns a paginated list of downstream lineage edges for a table.
func (r *LineageRepo) GetDownstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	total, err := r.q.CountDownstreamLineage(ctx, tableName)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.GetDownstreamLineage(ctx, dbstore.GetDownstreamLineageParams{
		SourceTable: tableName,
		Limit:       int64(page.Limit()),
		Offset:      int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	edges := make([]domain.LineageEdge, len(rows))
	for i, row := range rows {
		edges[i] = *mapper.LineageEdgeFromDownstreamDB(row)
	}
	return edges, total, nil
}

// DeleteEdge removes a lineage edge by ID.
func (r *LineageRepo) DeleteEdge(ctx context.Context, id string) error {
	// sqlc DeleteLineageEdge doesn't return rows affected, so we need to check existence
	return r.q.DeleteLineageEdge(ctx, id)
}

// PurgeOlderThan removes lineage edges created before the given time.
func (r *LineageRepo) PurgeOlderThan(ctx context.Context, before time.Time) (int64, error) {
	return r.q.PurgeLineageOlderThan(ctx, before.Format("2006-01-02 15:04:05"))
}
