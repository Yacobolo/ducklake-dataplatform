package governance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestQueryHistoryService_List(t *testing.T) {
	t.Run("delegates_to_repo", func(t *testing.T) {
		principalName := "alice"
		filter := domain.QueryHistoryFilter{
			PrincipalName: &principalName,
			Page:          domain.PageRequest{MaxResults: 50},
		}
		expected := []domain.QueryHistoryEntry{
			{ID: "1", PrincipalName: "alice", Status: "ALLOWED", CreatedAt: time.Now()},
		}

		var capturedFilter domain.QueryHistoryFilter
		repo := &mockQueryHistoryRepo{
			ListFn: func(_ context.Context, f domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
				capturedFilter = f
				return expected, 1, nil
			},
		}
		svc := NewQueryHistoryService(repo)

		entries, total, err := svc.List(context.Background(), filter)

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, entries, 1)
		assert.Equal(t, "alice", *capturedFilter.PrincipalName)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockQueryHistoryRepo{
			ListFn: func(_ context.Context, _ domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewQueryHistoryService(repo)

		_, _, err := svc.List(context.Background(), domain.QueryHistoryFilter{})

		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

func TestQueryHistoryService_List_RequiresAdmin(t *testing.T) {
	repo := &mockQueryHistoryRepo{
		ListFn: func(_ context.Context, _ domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error) {
			return []domain.QueryHistoryEntry{{ID: "1", PrincipalName: "alice"}}, 1, nil
		},
	}
	svc := NewQueryHistoryService(repo)

	// Non-admin should NOT be able to view all query history.
	_, _, err := svc.List(nonAdminCtx(), domain.QueryHistoryFilter{})
	require.Error(t, err, "non-admin should not be able to list all query history")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}
