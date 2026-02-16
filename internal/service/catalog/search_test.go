package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestSearchService_Search(t *testing.T) {
	t.Run("delegates_with_pagination", func(t *testing.T) {
		var capturedMaxResults, capturedOffset int
		repo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
				capturedMaxResults = maxResults
				capturedOffset = offset
				return []domain.SearchResult{
					{Type: "table", Name: "titanic", MatchField: "name"},
				}, 1, nil
			},
		}
		svc := NewSearchService(repo, nil)

		results, total, err := svc.Search(context.Background(), "titanic", nil, nil, domain.PageRequest{MaxResults: 50})

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, results, 1)
		assert.Equal(t, 50, capturedMaxResults)
		assert.Equal(t, 0, capturedOffset)
	})

	t.Run("default_pagination", func(t *testing.T) {
		var capturedMaxResults int
		repo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, maxResults int, _ int) ([]domain.SearchResult, int64, error) {
				capturedMaxResults = maxResults
				return []domain.SearchResult{}, 0, nil
			},
		}
		svc := NewSearchService(repo, nil)

		_, _, err := svc.Search(context.Background(), "q", nil, nil, domain.PageRequest{})

		require.NoError(t, err)
		assert.Equal(t, domain.DefaultMaxResults, capturedMaxResults, "should use default max results when zero")
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewSearchService(repo, nil)

		_, _, err := svc.Search(context.Background(), "q", nil, nil, domain.PageRequest{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})

	t.Run("catalog_param_uses_factory", func(t *testing.T) {
		catalogName := "analytics"
		var factoryCalled bool
		catalogRepo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				return []domain.SearchResult{
					{Type: "schema", Name: "raw", MatchField: "name"},
				}, 1, nil
			},
		}
		factory := &mockSearchRepoFactory{
			ForCatalogFn: func(_ context.Context, name string) (domain.SearchRepository, error) {
				factoryCalled = true
				assert.Equal(t, "analytics", name)
				return catalogRepo, nil
			},
		}
		defaultRepo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				t.Fatal("default repo should not be called when catalog is specified")
				return nil, 0, nil
			},
		}
		svc := NewSearchService(defaultRepo, factory)

		results, total, err := svc.Search(context.Background(), "raw", nil, &catalogName, domain.PageRequest{MaxResults: 50})

		require.NoError(t, err)
		assert.True(t, factoryCalled, "factory should have been called")
		assert.Equal(t, int64(1), total)
		assert.Len(t, results, 1)
		assert.Equal(t, "raw", results[0].Name)
	})
}

func TestSearchService_Search_DefaultCatalogResolution(t *testing.T) {
	t.Run("uses_default_catalog_when_no_name_provided", func(t *testing.T) {
		var defaultCalled bool
		catalogRepo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				return []domain.SearchResult{
					{Type: "table", Name: "orders", MatchField: "name"},
				}, 1, nil
			},
		}
		factory := &mockSearchRepoFactory{
			ForDefaultFn: func(_ context.Context) (domain.SearchRepository, error) {
				defaultCalled = true
				return catalogRepo, nil
			},
		}
		defaultRepo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				t.Fatal("static default repo should not be called when factory resolves a default catalog")
				return nil, 0, nil
			},
		}
		svc := NewSearchService(defaultRepo, factory)

		results, total, err := svc.Search(context.Background(), "orders", nil, nil, domain.PageRequest{MaxResults: 50})

		require.NoError(t, err)
		assert.True(t, defaultCalled, "factory.ForDefault should have been called")
		assert.Equal(t, int64(1), total)
		assert.Len(t, results, 1)
		assert.Equal(t, "orders", results[0].Name)
	})

	t.Run("falls_back_to_static_default_when_no_default_catalog", func(t *testing.T) {
		staticRepo := &mockSearchRepo{
			SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				return []domain.SearchResult{}, 0, nil
			},
		}
		factory := &mockSearchRepoFactory{
			ForDefaultFn: func(_ context.Context) (domain.SearchRepository, error) {
				return nil, fmt.Errorf("no default catalog configured")
			},
		}
		svc := NewSearchService(staticRepo, factory)

		_, _, err := svc.Search(context.Background(), "test", nil, nil, domain.PageRequest{})

		require.NoError(t, err, "should fall back to static default repo without error")
	})
}

func TestSearchService_Search_NoCatalogAttached(t *testing.T) {
	// Simulate the "no such table" error that occurs when no DuckLake catalog is attached.
	repo := &mockSearchRepo{
		SearchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
			return nil, 0, fmt.Errorf("no such table: ducklake_column")
		},
	}
	svc := NewSearchService(repo, nil)

	_, _, err := svc.Search(context.Background(), "test", nil, nil, domain.PageRequest{})

	require.Error(t, err)
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr, "should be a ValidationError when no catalog is attached")
	assert.Contains(t, err.Error(), "no catalog")
}

// mockSearchRepoFactory implements SearchRepoFactory for testing.
type mockSearchRepoFactory struct {
	ForCatalogFn func(ctx context.Context, catalogName string) (domain.SearchRepository, error)
	ForDefaultFn func(ctx context.Context) (domain.SearchRepository, error)
}

func (f *mockSearchRepoFactory) ForCatalog(ctx context.Context, catalogName string) (domain.SearchRepository, error) {
	if f.ForCatalogFn != nil {
		return f.ForCatalogFn(ctx, catalogName)
	}
	panic("unexpected call to mockSearchRepoFactory.ForCatalog")
}

func (f *mockSearchRepoFactory) ForDefault(ctx context.Context) (domain.SearchRepository, error) {
	if f.ForDefaultFn != nil {
		return f.ForDefaultFn(ctx)
	}
	return nil, fmt.Errorf("no default catalog configured")
}
