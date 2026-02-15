package governance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestAuditService_List(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		expected := []domain.AuditEntry{
			{ID: "ae-1", PrincipalName: "alice", Action: "QUERY", Status: "ALLOWED", CreatedAt: time.Now()},
			{ID: "ae-2", PrincipalName: "bob", Action: "GRANT", Status: "ALLOWED", CreatedAt: time.Now()},
		}
		repo := &mockAuditRepo{
			ListFn: func(_ context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
				return expected, 2, nil
			},
		}
		svc := NewAuditService(repo)

		entries, total, err := svc.List(context.Background(), domain.AuditFilter{})
		require.NoError(t, err)
		assert.Len(t, entries, 2)
		assert.Equal(t, int64(2), total)
		assert.Equal(t, "ae-1", entries[0].ID)
		assert.Equal(t, "ae-2", entries[1].ID)
	})

	t.Run("with_filters", func(t *testing.T) {
		principalName := "alice"
		action := "QUERY"
		repo := &mockAuditRepo{
			ListFn: func(_ context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
				assert.Equal(t, &principalName, filter.PrincipalName)
				assert.Equal(t, &action, filter.Action)
				return []domain.AuditEntry{
					{ID: "ae-1", PrincipalName: "alice", Action: "QUERY", Status: "ALLOWED"},
				}, 1, nil
			},
		}
		svc := NewAuditService(repo)

		entries, total, err := svc.List(context.Background(), domain.AuditFilter{
			PrincipalName: &principalName,
			Action:        &action,
		})
		require.NoError(t, err)
		assert.Len(t, entries, 1)
		assert.Equal(t, int64(1), total)
	})

	t.Run("empty_result", func(t *testing.T) {
		repo := &mockAuditRepo{
			ListFn: func(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
				return []domain.AuditEntry{}, 0, nil
			},
		}
		svc := NewAuditService(repo)

		entries, total, err := svc.List(context.Background(), domain.AuditFilter{})
		require.NoError(t, err)
		assert.Empty(t, entries)
		assert.Equal(t, int64(0), total)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockAuditRepo{
			ListFn: func(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
				return nil, 0, errTest
			},
		}
		svc := NewAuditService(repo)

		_, _, err := svc.List(context.Background(), domain.AuditFilter{})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

func TestAuditService_List_RequiresAdmin(t *testing.T) {
	repo := &mockAuditRepo{
		ListFn: func(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
			return []domain.AuditEntry{{ID: "ae-1", PrincipalName: "alice"}}, 1, nil
		},
	}
	svc := NewAuditService(repo)

	// Non-admin should NOT be able to view audit logs — they contain
	// sensitive information about all users' actions and SQL queries.
	_, _, err := svc.List(nonAdminCtx(), domain.AuditFilter{})
	require.Error(t, err, "non-admin should not be able to list audit logs")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}
