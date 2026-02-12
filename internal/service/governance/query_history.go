package governance

import (
	"context"

	"duck-demo/internal/domain"
)

// QueryHistoryService provides query history operations.
type QueryHistoryService struct {
	repo domain.QueryHistoryRepository
}

// NewQueryHistoryService creates a new QueryHistoryService.
func NewQueryHistoryService(repo domain.QueryHistoryRepository) *QueryHistoryService {
	return &QueryHistoryService{repo: repo}
}

// List returns a paginated list of query history entries.
func (s *QueryHistoryService) List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
	return s.repo.List(ctx, filter)
}
