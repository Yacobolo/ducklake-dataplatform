package governance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === InsertEdge ===

func TestLineageService_InsertEdge(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockLineageRepo{
			InsertEdgeFn: func(_ context.Context, _ *domain.LineageEdge) error {
				return nil
			},
		}
		svc := NewLineageService(repo)

		err := svc.InsertEdge(context.Background(), &domain.LineageEdge{
			SourceTable: "orders",
			EdgeType:    "READ",
		})

		require.NoError(t, err)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockLineageRepo{
			InsertEdgeFn: func(_ context.Context, _ *domain.LineageEdge) error {
				return errTest
			},
		}
		svc := NewLineageService(repo)

		err := svc.InsertEdge(context.Background(), &domain.LineageEdge{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

// === GetUpstream ===

func TestLineageService_GetUpstream(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockLineageRepo{
			GetUpstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{
					{ID: 1, SourceTable: "orders", EdgeType: "READ"},
				}, 1, nil
			},
		}
		svc := NewLineageService(repo)

		edges, total, err := svc.GetUpstream(context.Background(), "revenue_summary", domain.PageRequest{})

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, edges, 1)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockLineageRepo{
			GetUpstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewLineageService(repo)

		_, _, err := svc.GetUpstream(context.Background(), "t", domain.PageRequest{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

// === GetDownstream ===

func TestLineageService_GetDownstream(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockLineageRepo{
			GetDownstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{
					{ID: 2, SourceTable: "revenue_summary", EdgeType: "READ"},
				}, 1, nil
			},
		}
		svc := NewLineageService(repo)

		edges, total, err := svc.GetDownstream(context.Background(), "revenue_summary", domain.PageRequest{})

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, edges, 1)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockLineageRepo{
			GetDownstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewLineageService(repo)

		_, _, err := svc.GetDownstream(context.Background(), "t", domain.PageRequest{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

// === GetFullLineage ===

func TestLineageService_GetFullLineage(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		target := "main.revenue_summary"
		repo := &mockLineageRepo{
			GetUpstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{
					{ID: 1, SourceTable: "main.orders", EdgeType: "READ"},
					{ID: 2, SourceTable: "main.customers", EdgeType: "READ"},
				}, 2, nil
			},
			GetDownstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{
					{ID: 3, SourceTable: "main.revenue_summary", EdgeType: "READ"},
				}, 1, nil
			},
		}
		svc := NewLineageService(repo)

		node, err := svc.GetFullLineage(context.Background(), target, domain.PageRequest{})

		require.NoError(t, err)
		assert.Equal(t, target, node.TableName)
		assert.Len(t, node.Upstream, 2)
		assert.Len(t, node.Downstream, 1)
	})

	t.Run("upstream_error", func(t *testing.T) {
		downstreamCalled := false
		repo := &mockLineageRepo{
			GetUpstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return nil, 0, errTest
			},
			GetDownstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				downstreamCalled = true
				return nil, 0, nil
			},
		}
		svc := NewLineageService(repo)

		_, err := svc.GetFullLineage(context.Background(), "t", domain.PageRequest{})

		require.Error(t, err)
		require.ErrorIs(t, err, errTest)
		assert.False(t, downstreamCalled, "downstream should not be called when upstream fails")
	})

	t.Run("downstream_error", func(t *testing.T) {
		repo := &mockLineageRepo{
			GetUpstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{{ID: 1}}, 1, nil
			},
			GetDownstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewLineageService(repo)

		_, err := svc.GetFullLineage(context.Background(), "t", domain.PageRequest{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})

	t.Run("both_empty", func(t *testing.T) {
		repo := &mockLineageRepo{
			GetUpstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{}, 0, nil
			},
			GetDownstreamFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.LineageEdge, int64, error) {
				return []domain.LineageEdge{}, 0, nil
			},
		}
		svc := NewLineageService(repo)

		node, err := svc.GetFullLineage(context.Background(), "unknown_table", domain.PageRequest{})

		require.NoError(t, err)
		assert.Equal(t, "unknown_table", node.TableName)
		assert.Empty(t, node.Upstream)
		assert.Empty(t, node.Downstream)
	})
}

// === DeleteEdge ===

func TestLineageService_DeleteEdge(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockLineageRepo{
			DeleteEdgeFn: func(_ context.Context, id int64) error {
				assert.Equal(t, int64(42), id)
				return nil
			},
		}
		svc := NewLineageService(repo)

		err := svc.DeleteEdge(context.Background(), 42)

		require.NoError(t, err)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockLineageRepo{
			DeleteEdgeFn: func(_ context.Context, _ int64) error {
				return domain.ErrNotFound("edge not found")
			},
		}
		svc := NewLineageService(repo)

		err := svc.DeleteEdge(context.Background(), 999)

		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockLineageRepo{
			DeleteEdgeFn: func(_ context.Context, _ int64) error {
				return errTest
			},
		}
		svc := NewLineageService(repo)

		err := svc.DeleteEdge(context.Background(), 1)

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

// === PurgeOlderThan ===

func TestLineageService_PurgeOlderThan(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockLineageRepo{
			PurgeOlderThanFn: func(_ context.Context, before time.Time) (int64, error) {
				// Should be approximately 90 days ago
				assert.True(t, before.Before(time.Now().AddDate(0, 0, -89)))
				return 5, nil
			},
		}
		svc := NewLineageService(repo)

		deleted, err := svc.PurgeOlderThan(context.Background(), 90)

		require.NoError(t, err)
		assert.Equal(t, int64(5), deleted)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockLineageRepo{
			PurgeOlderThanFn: func(_ context.Context, _ time.Time) (int64, error) {
				return 0, errTest
			},
		}
		svc := NewLineageService(repo)

		_, err := svc.PurgeOlderThan(context.Background(), 30)

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})

	t.Run("zero_deleted", func(t *testing.T) {
		repo := &mockLineageRepo{
			PurgeOlderThanFn: func(_ context.Context, _ time.Time) (int64, error) {
				return 0, nil
			},
		}
		svc := NewLineageService(repo)

		deleted, err := svc.PurgeOlderThan(context.Background(), 7)

		require.NoError(t, err)
		assert.Equal(t, int64(0), deleted)
	})
}
