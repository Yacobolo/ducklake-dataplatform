package service

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

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "keyowner", Type: "user"})
	require.NoError(t, err)

	rawKey, key, err := svc.Create(adminCtx(), p.ID, "my-api-key", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, rawKey)
	assert.Len(t, rawKey, 64) // 32 bytes hex encoded
	assert.Equal(t, "my-api-key", key.Name)
	assert.Positive(t, key.ID)
	assert.Nil(t, key.ExpiresAt)
}

func TestAPIKeyService_Create_WithExpiry(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "keyowner", Type: "user"})
	require.NoError(t, err)

	expiry := time.Now().Add(24 * time.Hour)
	_, key, err := svc.Create(adminCtx(), p.ID, "expiring-key", &expiry)
	require.NoError(t, err)
	assert.NotNil(t, key.ExpiresAt)
}

func TestAPIKeyService_Create_EmptyName(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	_, _, err := svc.Create(adminCtx(), 1, "", nil)
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestAPIKeyService_Create_Unauthenticated(t *testing.T) {
	svc, _ := setupAPIKeyService(t)

	_, _, err := svc.Create(ctx, 1, "key", nil)
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestAPIKeyService_List(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "listowner", Type: "user"})
	require.NoError(t, err)

	// Create two keys.
	for _, name := range []string{"key-1", "key-2"} {
		_, _, err := svc.Create(adminCtx(), p.ID, name, nil)
		require.NoError(t, err)
	}

	keys, total, err := svc.List(ctx, p.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, keys, 2)
}

func TestAPIKeyService_Delete(t *testing.T) {
	svc, principalSvc := setupAPIKeyService(t)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "delowner", Type: "user"})
	require.NoError(t, err)

	_, key, err := svc.Create(adminCtx(), p.ID, "to-delete", nil)
	require.NoError(t, err)

	err = svc.Delete(adminCtx(), key.ID)
	require.NoError(t, err)

	keys, total, err := svc.List(ctx, p.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, keys)
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
