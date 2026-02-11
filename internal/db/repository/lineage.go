package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

type LineageRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

func NewLineageRepo(db *sql.DB) *LineageRepo {
	return &LineageRepo{q: dbstore.New(db), db: db}
}

func (r *LineageRepo) InsertEdge(ctx context.Context, edge *domain.LineageEdge) error {
	return r.q.InsertLineageEdge(ctx, dbstore.InsertLineageEdgeParams{
		SourceTable:   edge.SourceTable,
		TargetTable:   mapper.NullStrFromPtr(edge.TargetTable),
		EdgeType:      edge.EdgeType,
		PrincipalName: edge.PrincipalName,
		QueryHash:     mapper.NullStrFromPtr(edge.QueryHash),
	})
}

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
