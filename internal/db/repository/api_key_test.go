package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupAPIKeyTest(t *testing.T) (*APIKeyRepo, *PrincipalRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewAPIKeyRepo(writeDB), NewPrincipalRepo(writeDB)
}

func hashTestKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

func TestAPIKeyRepo_CreateAndLookup(t *testing.T) {
	apiKeyRepo, principalRepo := setupAPIKeyTest(t)
	ctx := context.Background()

	// Create a principal first.
	p, err := principalRepo.Create(ctx, &domain.Principal{Name: "testuser", Type: "user"})
	require.NoError(t, err)

	rawKey := "raw-api-key-1234567890"
	keyHash := hashTestKey(rawKey)

	key := &domain.APIKey{
		PrincipalID: p.ID,
		Name:        "my-key",
		KeyHash:     keyHash,
		KeyPrefix:   rawKey[:8],
	}
	err = apiKeyRepo.Create(ctx, key)
	require.NoError(t, err)
	assert.Positive(t, key.ID)
	assert.False(t, key.CreatedAt.IsZero())

	// Lookup by hash.
	foundKey, foundPrincipal, err := apiKeyRepo.GetByHash(ctx, keyHash)
	require.NoError(t, err)
	assert.Equal(t, key.ID, foundKey.ID)
	assert.Equal(t, "my-key", foundKey.Name)
	assert.Equal(t, p.ID, foundPrincipal.ID)
	assert.Equal(t, "testuser", foundPrincipal.Name)

	// Lookup principal name via LookupPrincipalByAPIKeyHash.
	name, err := apiKeyRepo.LookupPrincipalByAPIKeyHash(ctx, keyHash)
	require.NoError(t, err)
	assert.Equal(t, "testuser", name)
}

func TestAPIKeyRepo_ListByPrincipal(t *testing.T) {
	apiKeyRepo, principalRepo := setupAPIKeyTest(t)
	ctx := context.Background()

	p1, err := principalRepo.Create(ctx, &domain.Principal{Name: "user1", Type: "user"})
	require.NoError(t, err)
	p2, err := principalRepo.Create(ctx, &domain.Principal{Name: "user2", Type: "user"})
	require.NoError(t, err)

	// Create 2 keys for p1, 1 for p2.
	for _, kn := range []string{"key-a", "key-b"} {
		err := apiKeyRepo.Create(ctx, &domain.APIKey{
			PrincipalID: p1.ID,
			Name:        kn,
			KeyHash:     hashTestKey(kn),
		})
		require.NoError(t, err)
	}
	err = apiKeyRepo.Create(ctx, &domain.APIKey{
		PrincipalID: p2.ID,
		Name:        "key-c",
		KeyHash:     hashTestKey("key-c"),
	})
	require.NoError(t, err)

	// List for p1.
	keys, total, err := apiKeyRepo.ListByPrincipal(ctx, p1.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, keys, 2)

	// List for p2.
	keys, total, err = apiKeyRepo.ListByPrincipal(ctx, p2.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, keys, 1)
	assert.Equal(t, "key-c", keys[0].Name)
}

func TestAPIKeyRepo_Delete(t *testing.T) {
	apiKeyRepo, principalRepo := setupAPIKeyTest(t)
	ctx := context.Background()

	p, err := principalRepo.Create(ctx, &domain.Principal{Name: "user1", Type: "user"})
	require.NoError(t, err)

	key := &domain.APIKey{
		PrincipalID: p.ID,
		Name:        "to-delete",
		KeyHash:     hashTestKey("to-delete-key"),
	}
	err = apiKeyRepo.Create(ctx, key)
	require.NoError(t, err)

	err = apiKeyRepo.Delete(ctx, key.ID)
	require.NoError(t, err)

	// Should no longer be listed.
	keys, total, err := apiKeyRepo.ListByPrincipal(ctx, p.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, keys)
}

func TestAPIKeyRepo_DeleteExpired(t *testing.T) {
	apiKeyRepo, principalRepo := setupAPIKeyTest(t)
	ctx := context.Background()

	p, err := principalRepo.Create(ctx, &domain.Principal{Name: "user1", Type: "user"})
	require.NoError(t, err)

	// Create one expired key.
	past := time.Now().Add(-time.Hour)
	err = apiKeyRepo.Create(ctx, &domain.APIKey{
		PrincipalID: p.ID,
		Name:        "expired-key",
		KeyHash:     hashTestKey("expired-key"),
		ExpiresAt:   &past,
	})
	require.NoError(t, err)

	// Create one valid key.
	future := time.Now().Add(24 * time.Hour)
	err = apiKeyRepo.Create(ctx, &domain.APIKey{
		PrincipalID: p.ID,
		Name:        "valid-key",
		KeyHash:     hashTestKey("valid-key"),
		ExpiresAt:   &future,
	})
	require.NoError(t, err)

	// Create one key with no expiry.
	err = apiKeyRepo.Create(ctx, &domain.APIKey{
		PrincipalID: p.ID,
		Name:        "no-expiry-key",
		KeyHash:     hashTestKey("no-expiry-key"),
	})
	require.NoError(t, err)

	// Delete expired.
	count, err := apiKeyRepo.DeleteExpired(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Only valid + no-expiry remain.
	keys, total, err := apiKeyRepo.ListByPrincipal(ctx, p.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, keys, 2)
}

func TestAPIKeyRepo_LookupNotFound(t *testing.T) {
	apiKeyRepo, _ := setupAPIKeyTest(t)
	ctx := context.Background()

	_, err := apiKeyRepo.LookupPrincipalByAPIKeyHash(ctx, hashTestKey("nonexistent"))
	require.Error(t, err)
}
