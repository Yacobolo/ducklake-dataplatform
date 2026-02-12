package catalog

import (
	"context"

	"duck-demo/internal/domain"
)

// SearchService provides catalog search operations.
type SearchService struct {
	repo domain.SearchRepository
}

// NewSearchService creates a new SearchService.
func NewSearchService(repo domain.SearchRepository) *SearchService {
	return &SearchService{repo: repo}
}

// Search performs a full-text search across schemas, tables, and columns.
func (s *SearchService) Search(ctx context.Context, query string, objectType *string, page domain.PageRequest) ([]domain.SearchResult, int64, error) {
	return s.repo.Search(ctx, query, objectType, page.Limit(), page.Offset())
}
