package governance

import (
	"context"
	"time"

	"duck-demo/internal/domain"
)

// LineageService provides lineage operations.
type LineageService struct {
	repo    domain.LineageRepository
	colRepo domain.ColumnLineageRepository
}

// NewLineageService creates a new LineageService.
func NewLineageService(repo domain.LineageRepository, colRepo domain.ColumnLineageRepository) *LineageService {
	return &LineageService{repo: repo, colRepo: colRepo}
}

// InsertEdge records a new lineage edge.
func (s *LineageService) InsertEdge(ctx context.Context, edge *domain.LineageEdge) error {
	return s.repo.InsertEdge(ctx, edge)
}

// GetUpstream returns upstream lineage edges for a table.
func (s *LineageService) GetUpstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	return s.repo.GetUpstream(ctx, tableName, page)
}

// GetDownstream returns downstream lineage edges for a table.
func (s *LineageService) GetDownstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error) {
	return s.repo.GetDownstream(ctx, tableName, page)
}

// GetFullLineage returns both upstream and downstream lineage for a table.
func (s *LineageService) GetFullLineage(ctx context.Context, tableName string, page domain.PageRequest) (*domain.LineageNode, error) {
	upstream, _, err := s.repo.GetUpstream(ctx, tableName, page)
	if err != nil {
		return nil, err
	}
	downstream, _, err := s.repo.GetDownstream(ctx, tableName, page)
	if err != nil {
		return nil, err
	}
	return &domain.LineageNode{
		TableName:  tableName,
		Upstream:   upstream,
		Downstream: downstream,
	}, nil
}

// DeleteEdge removes a lineage edge by ID. Requires admin privileges.
func (s *LineageService) DeleteEdge(ctx context.Context, id string) error {
	if err := requireAdmin(ctx); err != nil {
		return err
	}
	return s.repo.DeleteEdge(ctx, id)
}

// PurgeOlderThan removes lineage edges older than the given duration. Requires admin privileges.
func (s *LineageService) PurgeOlderThan(ctx context.Context, olderThanDays int) (int64, error) {
	if err := requireAdmin(ctx); err != nil {
		return 0, err
	}
	before := time.Now().AddDate(0, 0, -olderThanDays)
	return s.repo.PurgeOlderThan(ctx, before)
}

// === Column-Level Lineage ===

// InsertColumnLineage stores column lineage entries for a table-level edge.
// Best-effort: returns nil if column lineage repo is not configured.
func (s *LineageService) InsertColumnLineage(ctx context.Context, edgeID string, entries []domain.ColumnLineageEntry) error {
	if s.colRepo == nil {
		return nil
	}

	var edges []domain.ColumnLineageEdge
	for _, entry := range entries {
		for _, src := range entry.Sources {
			edges = append(edges, domain.ColumnLineageEdge{
				LineageEdgeID: edgeID,
				TargetColumn:  entry.TargetColumn,
				SourceSchema:  src.Schema,
				SourceTable:   src.Table,
				SourceColumn:  src.Column,
				TransformType: entry.TransformType,
				Function:      entry.Function,
			})
		}
	}

	if len(edges) == 0 {
		return nil
	}

	return s.colRepo.InsertBatch(ctx, edgeID, edges)
}

// GetColumnLineageByEdge returns column lineage for a specific table-level edge.
func (s *LineageService) GetColumnLineageByEdge(ctx context.Context, edgeID string) ([]domain.ColumnLineageEdge, error) {
	if s.colRepo == nil {
		return nil, nil
	}
	return s.colRepo.GetByEdgeID(ctx, edgeID)
}

// GetColumnLineageForTable returns all column lineage edges for a table.
func (s *LineageService) GetColumnLineageForTable(ctx context.Context, schema, table string) ([]domain.ColumnLineageEdge, error) {
	if s.colRepo == nil {
		return nil, nil
	}
	return s.colRepo.GetForTable(ctx, schema, table)
}

// GetColumnLineageForSourceColumn returns column lineage edges originating from
// a specific source column.
func (s *LineageService) GetColumnLineageForSourceColumn(ctx context.Context, schema, table, column string) ([]domain.ColumnLineageEdge, error) {
	if s.colRepo == nil {
		return nil, nil
	}
	return s.colRepo.GetForSourceColumn(ctx, schema, table, column)
}
