package security

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

func setupAPIKeyService(t *testing.T) (*APIKeyService, *PrincipalService) {
	t.Helper()
	db, _ := internaldb.OpenTestSQLite(t)
	apiKeyRepo := repository.NewAPIKeyRepo(db)
	principalRepo := repository.NewPrincipalRepo(db)
	auditRepo := repository.NewAuditRepo(db)
	return NewAPIKeyService(apiKeyRepo, auditRepo), NewPrincipalService(principalRepo, auditRepo)
}

func TestAPIKeyService_Create(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "keyowner", Type: "user"})
	require.NoError(t, err)

	rawKey, key, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: p.ID, Name: "my-api-key"})
	require.NoError(t, err)
	assert.NotEmpty(t, rawKey)
	assert.Len(t, rawKey, 64) // 32 bytes hex encoded
	assert.Equal(t, "my-api-key", key.Name)
	assert.NotEmpty(t, key.ID)
	assert.Nil(t, key.ExpiresAt)
}

func TestAPIKeyService_Create_WithExpiry(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "keyowner", Type: "user"})
	require.NoError(t, err)

	expiry := time.Now().Add(24 * time.Hour)
	_, key, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: p.ID, Name: "expiring-key", ExpiresAt: &expiry})
	require.NoError(t, err)
	assert.NotNil(t, key.ExpiresAt)
}

func TestAPIKeyService_Create_EmptyName(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	_, _, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: "1", Name: ""})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestAPIKeyService_Create_Unauthenticated(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	_, _, err := svc.Create(ctx, domain.CreateAPIKeyRequest{PrincipalID: "1", Name: "key"})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestAPIKeyService_List(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "listowner", Type: "user"})
	require.NoError(t, err)

	// Create two keys.
	for _, name := range []string{"key-1", "key-2"} {
		_, _, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: p.ID, Name: name})
		require.NoError(t, err)
	}

	t.Run("explicit_principal_id", func(t *testing.T) {
		ownerCtx := principalCtx(p.ID, "listowner", false)
		keys, total, err := svc.List(ownerCtx, &p.ID, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, keys, 2)
	})

	t.Run("default_to_caller_keys", func(t *testing.T) {
		ownerCtx := principalCtx(p.ID, "listowner", false)
		keys, total, err := svc.List(ownerCtx, nil, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, keys, 2)
	})

	t.Run("non_admin_cannot_list_others", func(t *testing.T) {
		otherCtx := principalCtx("other-id", "other-user", false)
		_, _, err := svc.List(otherCtx, &p.ID, domain.PageRequest{})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})

	t.Run("admin_can_list_others", func(t *testing.T) {
		keys, total, err := svc.List(adminCtx(), &p.ID, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, keys, 2)
	})
}

func TestAPIKeyService_Delete(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "delowner", Type: "user"})
	require.NoError(t, err)

	_, key, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: p.ID, Name: "to-delete"})
	require.NoError(t, err)

	err = svc.Delete(adminCtx(), key.ID)
	require.NoError(t, err)

	keys, total, err := svc.List(adminCtx(), &p.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, keys)
}

func TestAPIKeyService_Delete_OwnerCanDelete(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	// Create the principal that will own the key.
	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "key-owner", Type: "user"})
	require.NoError(t, err)

	// Create a key as admin.
	_, key, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: p.ID, Name: "owner-key"})
	require.NoError(t, err)

	// Delete as the owner — should succeed.
	ownerCtx := principalCtx(p.ID, p.Name, false)
	err = svc.Delete(ownerCtx, key.ID)
	require.NoError(t, err)

	keys, total, err := svc.List(ownerCtx, &p.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, keys)
}

func TestAPIKeyService_Delete_AdminCanDeleteOthers(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "other-owner", Type: "user"})
	require.NoError(t, err)

	_, key, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: p.ID, Name: "admin-deletable"})
	require.NoError(t, err)

	// Admin can delete any key.
	err = svc.Delete(adminCtx(), key.ID)
	require.NoError(t, err)
}

func TestAPIKeyService_Delete_NonOwnerDenied(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	owner, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "actual-owner", Type: "user"})
	require.NoError(t, err)

	_, key, err := svc.Create(adminCtx(), domain.CreateAPIKeyRequest{PrincipalID: owner.ID, Name: "protected-key"})
	require.NoError(t, err)

	// Another non-admin user tries to delete — should be denied.
	otherCtx := principalCtx("other-id", "other-user", false)
	err = svc.Delete(otherCtx, key.ID)
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestAPIKeyService_Delete_Unauthenticated(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	err := svc.Delete(ctx, "some-id")
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestAPIKeyService_CleanupExpired_AdminRequired(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	_, err := svc.CleanupExpired(nonAdminCtx())
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestAPIKeyService_CleanupExpired_AdminAllowed(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	count, err := svc.CleanupExpired(adminCtx())
	require.NoError(t, err)
	assert.Equal(t, int64(0), count) // no expired keys
}
