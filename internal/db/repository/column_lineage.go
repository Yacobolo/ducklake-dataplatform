package repository

import (
	"context"
	"database/sql"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// ColumnLineageRepo implements domain.ColumnLineageRepository using SQLite.
type ColumnLineageRepo struct {
	db *sql.DB
	q  *dbstore.Queries
}

// NewColumnLineageRepo creates a new ColumnLineageRepo.
func NewColumnLineageRepo(db *sql.DB) *ColumnLineageRepo {
	return &ColumnLineageRepo{db: db, q: dbstore.New(db)}
}

// InsertBatch inserts all column lineage edges for a given table-level edge.
func (r *ColumnLineageRepo) InsertBatch(ctx context.Context, edgeID string, edges []domain.ColumnLineageEdge) error {
	if len(edges) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	qtx := r.q.WithTx(tx)
	for _, edge := range edges {
		if err := qtx.InsertColumnLineageEdge(ctx, dbstore.InsertColumnLineageEdgeParams{
			LineageEdgeID: edgeID,
			TargetColumn:  edge.TargetColumn,
			SourceSchema:  edge.SourceSchema,
			SourceTable:   edge.SourceTable,
			SourceColumn:  edge.SourceColumn,
			TransformType: string(edge.TransformType),
			FunctionName:  edge.Function,
		}); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetByEdgeID returns all column lineage edges for a table-level lineage edge.
func (r *ColumnLineageRepo) GetByEdgeID(ctx context.Context, edgeID string) ([]domain.ColumnLineageEdge, error) {
	rows, err := r.q.GetColumnLineageByEdgeID(ctx, edgeID)
	if err != nil {
		return nil, err
	}
	return mapColumnLineageEdges(rows), nil
}

// GetForTable returns all column lineage for a target table.
func (r *ColumnLineageRepo) GetForTable(ctx context.Context, schema, table string) ([]domain.ColumnLineageEdge, error) {
	rows, err := r.q.GetColumnLineageForTable(ctx, dbstore.GetColumnLineageForTableParams{
		TargetSchema: sql.NullString{String: schema, Valid: true},
		TargetTable:  sql.NullString{String: table, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	return mapColumnLineageEdges(rows), nil
}

// GetForSourceColumn returns all column lineage edges sourced from a specific column.
func (r *ColumnLineageRepo) GetForSourceColumn(ctx context.Context, schema, table, column string) ([]domain.ColumnLineageEdge, error) {
	rows, err := r.q.GetColumnLineageForSourceColumn(ctx, dbstore.GetColumnLineageForSourceColumnParams{
		SourceSchema: schema,
		SourceTable:  table,
		SourceColumn: column,
	})
	if err != nil {
		return nil, err
	}
	return mapColumnLineageEdges(rows), nil
}

// DeleteByEdgeID removes all column lineage for a table-level edge.
func (r *ColumnLineageRepo) DeleteByEdgeID(ctx context.Context, edgeID string) error {
	return r.q.DeleteColumnLineageByEdgeID(ctx, edgeID)
}

// mapColumnLineageEdges converts dbstore rows to domain types.
func mapColumnLineageEdges(rows []dbstore.ColumnLineageEdge) []domain.ColumnLineageEdge {
	edges := make([]domain.ColumnLineageEdge, len(rows))
	for i, row := range rows {
		edges[i] = domain.ColumnLineageEdge{
			ID:            row.ID,
			LineageEdgeID: row.LineageEdgeID,
			TargetColumn:  row.TargetColumn,
			SourceSchema:  row.SourceSchema,
			SourceTable:   row.SourceTable,
			SourceColumn:  row.SourceColumn,
			TransformType: domain.TransformType(row.TransformType),
			Function:      row.FunctionName,
		}
	}
	return edges
}
