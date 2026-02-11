package service

import (
	"context"
	"time"

	"duck-demo/internal/domain"
)

// LineageService provides lineage operations.
type LineageService struct {
	repo domain.LineageRepository
}

// NewLineageService creates a new LineageService.
func NewLineageService(repo domain.LineageRepository) *LineageService {
	return &LineageService{repo: repo}
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

// DeleteEdge removes a lineage edge by ID.
func (s *LineageService) DeleteEdge(ctx context.Context, id int64) error {
	return s.repo.DeleteEdge(ctx, id)
}

// PurgeOlderThan removes lineage edges older than the given duration.
func (s *LineageService) PurgeOlderThan(ctx context.Context, olderThanDays int) (int64, error) {
	before := time.Now().AddDate(0, 0, -olderThanDays)
	return s.repo.PurgeOlderThan(ctx, before)
}
