package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestRequireAdmin(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		err := RequireAdmin(context.Background())
		require.Error(t, err)
		assert.EqualError(t, err, "authentication required")
	})

	t.Run("non admin denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireAdmin(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "admin privileges required")
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		require.NoError(t, RequireAdmin(ctx))
	})
}

func TestRequireAuthenticatedPrincipal(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		_, err := RequireAuthenticatedPrincipal(context.Background())
		require.Error(t, err)
		assert.EqualError(t, err, "authentication required")
	})

	t.Run("principal returned", func(t *testing.T) {
		t.Parallel()

		expected := domain.ContextPrincipal{ID: "alice-id", Name: "alice", IsAdmin: true}
		ctx := domain.WithPrincipal(context.Background(), expected)

		principal, err := RequireAuthenticatedPrincipal(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, principal)
	})
}

func TestRequirePrincipalName(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		_, err := RequirePrincipalName(context.Background())
		require.Error(t, err)
		assert.EqualError(t, err, "principal context is required")
	})

	t.Run("empty principal name denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{ID: "alice-id"})
		_, err := RequirePrincipalName(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "principal context is required")
	})

	t.Run("principal name returned", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{ID: "alice-id", Name: "alice"})
		name, err := RequirePrincipalName(ctx)
		require.NoError(t, err)
		assert.Equal(t, "alice", name)
	})
}

func TestRequireAdminForAction(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		err := RequireAdminForAction(context.Background(), "list catalog registrations")
		require.Error(t, err)
		assert.EqualError(t, err, "authentication required")
	})

	t.Run("non admin denied with action message", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireAdminForAction(ctx, "list catalog registrations")
		require.Error(t, err)
		assert.EqualError(t, err, "list catalog registrations requires admin privileges")
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		require.NoError(t, RequireAdminForAction(ctx, "list catalog registrations"))
	})
}

func TestRequireAdminIfPresentForAction(t *testing.T) {
	t.Parallel()

	t.Run("missing principal allowed", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, RequireAdminIfPresentForAction(context.Background(), "attach catalogs"))
	})

	t.Run("non admin denied with action message", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireAdminIfPresentForAction(ctx, "set default catalog")
		require.Error(t, err)
		assert.EqualError(t, err, "set default catalog requires admin privileges")
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		require.NoError(t, RequireAdminIfPresentForAction(ctx, "set default catalog"))
	})
}

func TestRequirePrincipalOrAdmin(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		_, err := RequirePrincipalOrAdmin(context.Background(), "owner-id", "can only list your own API keys")
		require.Error(t, err)
		assert.EqualError(t, err, "authentication required")
	})

	t.Run("owner allowed", func(t *testing.T) {
		t.Parallel()

		expected := domain.ContextPrincipal{ID: "owner-id", Name: "alice"}
		ctx := domain.WithPrincipal(context.Background(), expected)

		principal, err := RequirePrincipalOrAdmin(ctx, "owner-id", "can only list your own API keys")
		require.NoError(t, err)
		assert.Equal(t, expected, principal)
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		expected := domain.ContextPrincipal{ID: "admin-id", Name: "admin", IsAdmin: true}
		ctx := domain.WithPrincipal(context.Background(), expected)

		principal, err := RequirePrincipalOrAdmin(ctx, "owner-id", "can only list your own API keys")
		require.NoError(t, err)
		assert.Equal(t, expected, principal)
	})

	t.Run("other principal denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{ID: "other-id", Name: "other"})

		_, err := RequirePrincipalOrAdmin(ctx, "owner-id", "can only list your own API keys")
		require.Error(t, err)
		assert.EqualError(t, err, "can only list your own API keys")
	})
}

type stubAuthorizationService struct {
	checkPrivilegeFn func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error)
}

func (s stubAuthorizationService) LookupTableID(ctx context.Context, tableName string) (string, string, bool, error) {
	return "", "", false, errors.New("unexpected LookupTableID call")
}

func (s stubAuthorizationService) CheckPrivilege(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
	return s.checkPrivilegeFn(ctx, principal, securableType, securableID, privilege)
}

func (s stubAuthorizationService) GetEffectiveRowFilters(ctx context.Context, principalName string, tableID string) ([]string, error) {
	return nil, errors.New("unexpected GetEffectiveRowFilters call")
}

func (s stubAuthorizationService) GetEffectiveColumnMasks(ctx context.Context, principalName string, tableID string) (map[string]string, error) {
	return nil, errors.New("unexpected GetEffectiveColumnMasks call")
}

func (s stubAuthorizationService) GetTableColumnNames(ctx context.Context, tableID string) ([]string, error) {
	return nil, errors.New("unexpected GetTableColumnNames call")
}

func TestRequireCatalogPrivilege(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		err := RequireCatalogPrivilege(context.Background(), nil, "", domain.PrivManageAssetDefinitions, "asset orchestration requires %s on catalog")
		require.Error(t, err)
		assert.EqualError(t, err, "principal context is required")
	})

	t.Run("admin allowed when auth is unavailable", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true})
		require.NoError(t, RequireCatalogPrivilege(ctx, nil, "admin", domain.PrivManageAssetDefinitions, "asset orchestration requires %s on catalog"))
	})

	t.Run("non admin denied when auth is unavailable", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireCatalogPrivilege(ctx, nil, "alice", domain.PrivManageAssetDefinitions, "asset orchestration requires %s on catalog")
		require.Error(t, err)
		assert.EqualError(t, err, "asset orchestration requires MANAGE_ASSET_DEFINITIONS on catalog")
	})

	t.Run("authorization service denial returns access denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		auth := stubAuthorizationService{
			checkPrivilegeFn: func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
				assert.Equal(t, "alice", principal)
				assert.Equal(t, domain.SecurableCatalog, securableType)
				assert.Equal(t, domain.CatalogID, securableID)
				assert.Equal(t, domain.PrivManageAssetDefinitions, privilege)
				return false, nil
			},
		}

		err := RequireCatalogPrivilege(ctx, auth, "alice", domain.PrivManageAssetDefinitions, "asset orchestration requires %s on catalog")
		require.Error(t, err)
		assert.EqualError(t, err, "\"alice\" lacks MANAGE_ASSET_DEFINITIONS on catalog")
	})

	t.Run("authorization service errors are wrapped", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		auth := stubAuthorizationService{
			checkPrivilegeFn: func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
				return false, errors.New("boom")
			},
		}

		err := RequireCatalogPrivilege(ctx, auth, "alice", domain.PrivManageAssetDefinitions, "asset orchestration requires %s on catalog")
		require.Error(t, err)
		assert.EqualError(t, err, "check privilege: boom")
	})

	t.Run("authorization service allow passes", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		auth := stubAuthorizationService{
			checkPrivilegeFn: func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
				return true, nil
			},
		}

		require.NoError(t, RequireCatalogPrivilege(ctx, auth, "alice", domain.PrivManageAssetDefinitions, "asset orchestration requires %s on catalog"))
	})
}

func TestRequireSecurablePrivilege(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		err := RequireSecurablePrivilege(context.Background(), stubAuthorizationService{}, "", domain.SecurableComputeEndpoint, "endpoint-1", domain.PrivManageCompute)
		require.Error(t, err)
		assert.EqualError(t, err, "principal context is required")
	})

	t.Run("authorization service denial returns access denied", func(t *testing.T) {
		t.Parallel()

		auth := stubAuthorizationService{
			checkPrivilegeFn: func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
				assert.Equal(t, "alice", principal)
				assert.Equal(t, domain.SecurableComputeEndpoint, securableType)
				assert.Equal(t, "endpoint-1", securableID)
				assert.Equal(t, domain.PrivManageCompute, privilege)
				return false, nil
			},
		}

		err := RequireSecurablePrivilege(context.Background(), auth, "alice", domain.SecurableComputeEndpoint, "endpoint-1", domain.PrivManageCompute)
		require.Error(t, err)
		assert.EqualError(t, err, "\"alice\" lacks MANAGE_COMPUTE on compute_endpoint \"endpoint-1\"")
	})

	t.Run("authorization service errors are wrapped", func(t *testing.T) {
		t.Parallel()

		auth := stubAuthorizationService{
			checkPrivilegeFn: func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
				return false, errors.New("boom")
			},
		}

		err := RequireSecurablePrivilege(context.Background(), auth, "alice", domain.SecurableComputeEndpoint, "endpoint-1", domain.PrivManageCompute)
		require.Error(t, err)
		assert.EqualError(t, err, "check privilege: boom")
	})

	t.Run("authorization service allow passes", func(t *testing.T) {
		t.Parallel()

		auth := stubAuthorizationService{
			checkPrivilegeFn: func(ctx context.Context, principal, securableType, securableID, privilege string) (bool, error) {
				return true, nil
			},
		}

		require.NoError(t, RequireSecurablePrivilege(context.Background(), auth, "alice", domain.SecurableComputeEndpoint, "endpoint-1", domain.PrivManageCompute))
	})
}

func TestCanReadOwnedResource(t *testing.T) {
	t.Parallel()

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		assert.True(t, CanReadOwnedResource(ctx, "bob", "carol"))
	})

	t.Run("owner allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		assert.True(t, CanReadOwnedResource(ctx, "alice", "alice"))
	})

	t.Run("non owner denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		assert.False(t, CanReadOwnedResource(ctx, "alice", "bob"))
	})
}

func TestCallerName(t *testing.T) {
	t.Parallel()

	assert.Empty(t, CallerName(context.Background()))

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
	assert.Equal(t, "alice", CallerName(ctx))
}
