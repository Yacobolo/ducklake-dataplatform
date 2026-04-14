package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestWebSessionRepo_CreateGetTouchRevoke(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	principalRepo := NewPrincipalRepo(writeDB)
	repo := NewWebSessionRepo(writeDB)

	p, err := principalRepo.Create(context.Background(), &domain.Principal{Name: "web-user", Type: "user"})
	require.NoError(t, err)

	expires := time.Now().Add(2 * time.Hour)
	idleExpires := time.Now().Add(30 * time.Minute)
	created, err := repo.Create(context.Background(), &domain.AuthSession{
		PrincipalID:   p.ID,
		SessionHash:   "hash-1",
		AuthMethod:    "local",
		ExpiresAt:     expires,
		IdleExpiresAt: idleExpires,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, created.ID)

	got, err := repo.GetActiveByHash(context.Background(), "hash-1")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)

	newIdle := time.Now().Add(45 * time.Minute)
	require.NoError(t, repo.Touch(context.Background(), created.ID, newIdle))

	touched, err := repo.GetActiveByHash(context.Background(), "hash-1")
	require.NoError(t, err)
	assert.WithinDuration(t, newIdle, touched.IdleExpiresAt, 2*time.Second)

	require.NoError(t, repo.Revoke(context.Background(), created.ID))
	_, err = repo.GetActiveByHash(context.Background(), "hash-1")
	require.Error(t, err)
}

func TestWebSessionRepo_RevokeByHashAndReap(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	principalRepo := NewPrincipalRepo(writeDB)
	repo := NewWebSessionRepo(writeDB)

	p, err := principalRepo.Create(context.Background(), &domain.Principal{Name: "reap-user", Type: "user"})
	require.NoError(t, err)

	_, err = repo.Create(context.Background(), &domain.AuthSession{
		PrincipalID:   p.ID,
		SessionHash:   "hash-reap",
		AuthMethod:    "local",
		ExpiresAt:     time.Now().Add(time.Hour),
		IdleExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)

	require.NoError(t, repo.RevokeByHash(context.Background(), "hash-reap"))

	deleted, err := repo.DeleteExpiredOrRevoked(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
}

func TestWebSessionRepo_RevokeAllForPrincipal(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	principalRepo := NewPrincipalRepo(writeDB)
	repo := NewWebSessionRepo(writeDB)

	p, err := principalRepo.Create(context.Background(), &domain.Principal{Name: "bulk-user", Type: "user"})
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		_, createErr := repo.Create(context.Background(), &domain.AuthSession{
			PrincipalID:   p.ID,
			SessionHash:   fmt.Sprintf("hash-bulk-%d", i),
			AuthMethod:    "oidc",
			ExpiresAt:     time.Now().Add(2 * time.Hour),
			IdleExpiresAt: time.Now().Add(20 * time.Minute),
		})
		require.NoError(t, createErr)
	}

	require.NoError(t, repo.RevokeAllForPrincipal(context.Background(), p.ID))

	deleted, err := repo.DeleteExpiredOrRevoked(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
}
