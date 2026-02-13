package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupPrincipalRepo(t *testing.T) *PrincipalRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewPrincipalRepo(writeDB)
}

func TestPrincipalRepo_CRUD(t *testing.T) {
	repo := setupPrincipalRepo(t)
	ctx := context.Background()

	// Create
	p, err := repo.Create(ctx, &domain.Principal{
		Name:    "alice",
		Type:    "user",
		IsAdmin: false,
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.NotEmpty(t, p.ID)
	assert.Equal(t, "alice", p.Name)
	assert.Equal(t, "user", p.Type)
	assert.False(t, p.IsAdmin)

	// GetByID
	found, err := repo.GetByID(ctx, p.ID)
	require.NoError(t, err)
	assert.Equal(t, p.ID, found.ID)
	assert.Equal(t, "alice", found.Name)

	// GetByName
	found, err = repo.GetByName(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, p.ID, found.ID)

	// List
	ps, total, err := repo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, int64(1))
	var names []string
	for _, pp := range ps {
		names = append(names, pp.Name)
	}
	assert.Contains(t, names, "alice")

	// Delete
	err = repo.Delete(ctx, p.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, p.ID)
	require.Error(t, err)
}

func TestPrincipalRepo_GetByExternalID(t *testing.T) {
	repo := setupPrincipalRepo(t)
	ctx := context.Background()

	issuer := "https://login.microsoftonline.com/tenant/v2.0"
	extID := "ext-sub-12345"

	// Create with external ID.
	p, err := repo.Create(ctx, &domain.Principal{
		Name:           "ext-user",
		Type:           "user",
		ExternalID:     &extID,
		ExternalIssuer: &issuer,
	})
	require.NoError(t, err)

	// Lookup by external ID.
	found, err := repo.GetByExternalID(ctx, issuer, extID)
	require.NoError(t, err)
	assert.Equal(t, p.ID, found.ID)
	assert.Equal(t, "ext-user", found.Name)
	assert.Equal(t, &extID, found.ExternalID)
	assert.Equal(t, &issuer, found.ExternalIssuer)
}

func TestPrincipalRepo_ExternalIDUnique(t *testing.T) {
	repo := setupPrincipalRepo(t)
	ctx := context.Background()

	issuer := "https://issuer.example.com"
	extID := "unique-ext-id"

	_, err := repo.Create(ctx, &domain.Principal{
		Name:           "user-a",
		Type:           "user",
		ExternalID:     &extID,
		ExternalIssuer: &issuer,
	})
	require.NoError(t, err)

	// Second create with same external ID should conflict.
	_, err = repo.Create(ctx, &domain.Principal{
		Name:           "user-b",
		Type:           "user",
		ExternalID:     &extID,
		ExternalIssuer: &issuer,
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestPrincipalRepo_SetAdmin(t *testing.T) {
	repo := setupPrincipalRepo(t)
	ctx := context.Background()

	p, err := repo.Create(ctx, &domain.Principal{
		Name: "bob",
		Type: "user",
	})
	require.NoError(t, err)
	assert.False(t, p.IsAdmin)

	// Promote to admin.
	err = repo.SetAdmin(ctx, p.ID, true)
	require.NoError(t, err)

	found, err := repo.GetByID(ctx, p.ID)
	require.NoError(t, err)
	assert.True(t, found.IsAdmin)

	// Demote.
	err = repo.SetAdmin(ctx, p.ID, false)
	require.NoError(t, err)

	found, err = repo.GetByID(ctx, p.ID)
	require.NoError(t, err)
	assert.False(t, found.IsAdmin)
}

func TestPrincipalRepo_GetByName_NotFound(t *testing.T) {
	repo := setupPrincipalRepo(t)
	ctx := context.Background()

	_, err := repo.GetByName(ctx, "nonexistent")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestPrincipalRepo_GetByExternalID_NotFound(t *testing.T) {
	repo := setupPrincipalRepo(t)
	ctx := context.Background()

	_, err := repo.GetByExternalID(ctx, "https://issuer.example.com", "unknown-ext-id")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}
