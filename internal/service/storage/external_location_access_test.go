package storage

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/testutil"
)

type readAccessExternalLocationRepo = testutil.MockExternalLocationRepo

func testDiscardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestExternalLocationService_ReadAccess(t *testing.T) {
	t.Run("get_by_name denies non_owner", func(t *testing.T) {
		repo := &readAccessExternalLocationRepo{
			GetByNameFn: func(_ context.Context, name string) (*domain.ExternalLocation, error) {
				return &domain.ExternalLocation{ID: "1", Name: name, Owner: "alice"}, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewExternalLocationService(repo, &mockStorageCredentialRepo{}, allowAllAuth(), audit, nil, testDiscardLogger())

		_, err := svc.GetByName(ctxWithPrincipal("bob"), "bob", "landing")

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
		require.NotNil(t, audit.LastEntry())
		assert.Equal(t, "DENIED", audit.LastEntry().Status)
		assert.Equal(t, "GET_EXTERNAL_LOCATION", audit.LastEntry().Action)
	})

	t.Run("list returns only owned locations for non_admin", func(t *testing.T) {
		repo := &readAccessExternalLocationRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
				return []domain.ExternalLocation{
					{ID: "1", Name: "landing", Owner: "alice"},
					{ID: "2", Name: "bronze", Owner: "bob"},
					{ID: "3", Name: "silver", Owner: "alice"},
				}, 3, nil
			},
		}
		svc := NewExternalLocationService(repo, &mockStorageCredentialRepo{}, allowAllAuth(), &mockAuditRepo{}, nil, testDiscardLogger())

		locations, total, err := svc.List(ctxWithPrincipal("alice"), "alice", domain.PageRequest{MaxResults: 10})

		require.NoError(t, err)
		require.Len(t, locations, 2)
		assert.Equal(t, int64(2), total)
		assert.Equal(t, "landing", locations[0].Name)
		assert.Equal(t, "silver", locations[1].Name)
	})

	t.Run("admin list returns full set", func(t *testing.T) {
		repo := &readAccessExternalLocationRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
				return []domain.ExternalLocation{
					{ID: "1", Name: "landing", Owner: "alice"},
					{ID: "2", Name: "bronze", Owner: "bob"},
				}, 2, nil
			},
		}
		svc := NewExternalLocationService(repo, &mockStorageCredentialRepo{}, allowAllAuth(), &mockAuditRepo{}, nil, testDiscardLogger())

		locations, total, err := svc.List(ctxWithPrincipal("admin_user"), "admin_user", domain.PageRequest{MaxResults: 10})

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, locations, 2)
	})
}
