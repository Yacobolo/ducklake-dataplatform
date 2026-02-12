package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestSearchService_Search(t *testing.T) {
	t.Run("delegates_with_pagination", func(t *testing.T) {
		var capturedMaxResults, capturedOffset int
		repo := &mockSearchRepo{
			searchFn: func(_ context.Context, _ string, _ *string, maxResults int, offset int) ([]domain.SearchResult, int64, error) {
				capturedMaxResults = maxResults
				capturedOffset = offset
				return []domain.SearchResult{
					{Type: "table", Name: "titanic", MatchField: "name"},
				}, 1, nil
			},
		}
		svc := NewSearchService(repo)

		results, total, err := svc.Search(context.Background(), "titanic", nil, domain.PageRequest{MaxResults: 50})

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, results, 1)
		assert.Equal(t, 50, capturedMaxResults)
		assert.Equal(t, 0, capturedOffset)
	})

	t.Run("default_pagination", func(t *testing.T) {
		var capturedMaxResults int
		repo := &mockSearchRepo{
			searchFn: func(_ context.Context, _ string, _ *string, maxResults int, _ int) ([]domain.SearchResult, int64, error) {
				capturedMaxResults = maxResults
				return []domain.SearchResult{}, 0, nil
			},
		}
		svc := NewSearchService(repo)

		_, _, err := svc.Search(context.Background(), "q", nil, domain.PageRequest{})

		require.NoError(t, err)
		assert.Equal(t, domain.DefaultMaxResults, capturedMaxResults, "should use default max results when zero")
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockSearchRepo{
			searchFn: func(_ context.Context, _ string, _ *string, _ int, _ int) ([]domain.SearchResult, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewSearchService(repo)

		_, _, err := svc.Search(context.Background(), "q", nil, domain.PageRequest{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}
