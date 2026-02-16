package catalog

import (
	"context"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

// SearchRepoFactory creates per-catalog SearchRepository instances.
type SearchRepoFactory interface {
	ForCatalog(ctx context.Context, catalogName string) (domain.SearchRepository, error)
	ForDefault(ctx context.Context) (domain.SearchRepository, error)
}

// SearchService provides catalog search operations.
// It uses a factory to route searches to the appropriate catalog metastore.
type SearchService struct {
	factory     SearchRepoFactory
	defaultRepo domain.SearchRepository // used when no catalog is specified
}

// NewSearchService creates a new SearchService.
// defaultRepo handles searches when no catalog name is specified.
func NewSearchService(defaultRepo domain.SearchRepository, factory SearchRepoFactory) *SearchService {
	return &SearchService{
		defaultRepo: defaultRepo,
		factory:     factory,
	}
}

// Search performs a full-text search across schemas, tables, and columns.
// When catalogName is nil, searches the default catalog's metastore.
// When catalogName is provided, searches that specific catalog's metastore.
func (s *SearchService) Search(ctx context.Context, query string, objectType *string, catalogName *string, page domain.PageRequest) ([]domain.SearchResult, int64, error) {
	repo, err := s.resolveRepo(ctx, catalogName)
	if err != nil {
		return nil, 0, err
	}
	results, total, err := repo.Search(ctx, query, objectType, page.Limit(), page.Offset())
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return nil, 0, domain.ErrValidation("search unavailable: no catalog is currently attached")
		}
		return nil, 0, fmt.Errorf("search query: %w", err)
	}
	return results, total, nil
}

// resolveRepo returns the appropriate SearchRepository for the given catalog name.
// When no catalog name is provided, it dynamically resolves the current default catalog.
func (s *SearchService) resolveRepo(ctx context.Context, catalogName *string) (domain.SearchRepository, error) {
	if catalogName == nil || *catalogName == "" {
		if s.factory == nil {
			return s.defaultRepo, nil
		}
		repo, err := s.factory.ForDefault(ctx)
		if err != nil {
			// Fall back to static default if no default catalog is configured.
			return s.defaultRepo, nil //nolint:nilerr // intentional fallback
		}
		return repo, nil
	}
	if s.factory == nil {
		return s.defaultRepo, nil
	}
	repo, err := s.factory.ForCatalog(ctx, *catalogName)
	if err != nil {
		return nil, fmt.Errorf("resolve search repo for catalog %q: %w", *catalogName, err)
	}
	return repo, nil
}
