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

func setupGrantRepo(t *testing.T) (*GrantRepo, *PrincipalRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewGrantRepo(writeDB), NewPrincipalRepo(writeDB)
}

// createTestPrincipal is a helper that creates a principal for use in grant tests.
func createTestPrincipal(t *testing.T, repo *PrincipalRepo, name string) *domain.Principal {
	t.Helper()
	p, err := repo.Create(context.Background(), &domain.Principal{
		Name: name,
		Type: "user",
	})
	require.NoError(t, err)
	return p
}

func TestGrantRepo_GrantAndRevoke(t *testing.T) {
	grantRepo, principalRepo := setupGrantRepo(t)
	ctx := context.Background()

	p := createTestPrincipal(t, principalRepo, "alice")
	admin := "admin"

	// Grant SELECT on a table.
	grant, err := grantRepo.Grant(ctx, &domain.PrivilegeGrant{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		SecurableType: "table",
		SecurableID:   "t-1",
		Privilege:     "SELECT",
		GrantedBy:     &admin,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, grant.ID)
	assert.Equal(t, p.ID, grant.PrincipalID)
	assert.Equal(t, "SELECT", grant.Privilege)
	require.NotNil(t, grant.GrantedBy)
	assert.Equal(t, "admin", *grant.GrantedBy)

	// Verify HasPrivilege.
	has, err := grantRepo.HasPrivilege(ctx, p.ID, "user", "table", "t-1", "SELECT")
	require.NoError(t, err)
	assert.True(t, has)

	// Revoke by compound key.
	err = grantRepo.Revoke(ctx, &domain.PrivilegeGrant{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		SecurableType: "table",
		SecurableID:   "t-1",
		Privilege:     "SELECT",
	})
	require.NoError(t, err)

	// Verify privilege is gone.
	has, err = grantRepo.HasPrivilege(ctx, p.ID, "user", "table", "t-1", "SELECT")
	require.NoError(t, err)
	assert.False(t, has)
}

func TestGrantRepo_RevokeByID(t *testing.T) {
	grantRepo, principalRepo := setupGrantRepo(t)
	ctx := context.Background()

	p := createTestPrincipal(t, principalRepo, "bob")

	grant, err := grantRepo.Grant(ctx, &domain.PrivilegeGrant{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		SecurableType: "schema",
		SecurableID:   "s-1",
		Privilege:     "USAGE",
	})
	require.NoError(t, err)

	err = grantRepo.RevokeByID(ctx, grant.ID)
	require.NoError(t, err)

	has, err := grantRepo.HasPrivilege(ctx, p.ID, "user", "schema", "s-1", "USAGE")
	require.NoError(t, err)
	assert.False(t, has)
}

func TestGrantRepo_ListForPrincipal(t *testing.T) {
	grantRepo, principalRepo := setupGrantRepo(t)
	ctx := context.Background()

	p := createTestPrincipal(t, principalRepo, "carol")

	// Create multiple grants.
	for _, priv := range []string{"SELECT", "INSERT", "UPDATE"} {
		_, err := grantRepo.Grant(ctx, &domain.PrivilegeGrant{
			PrincipalID:   p.ID,
			PrincipalType: "user",
			SecurableType: "table",
			SecurableID:   "t-1",
			Privilege:     priv,
		})
		require.NoError(t, err)
	}

	grants, total, err := grantRepo.ListForPrincipal(ctx, p.ID, "user", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, grants, 3)
}

func TestGrantRepo_ListForPrincipal_Empty(t *testing.T) {
	grantRepo, principalRepo := setupGrantRepo(t)
	ctx := context.Background()

	p := createTestPrincipal(t, principalRepo, "dave")

	grants, total, err := grantRepo.ListForPrincipal(ctx, p.ID, "user", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, grants)
}

func TestGrantRepo_ListForSecurable(t *testing.T) {
	grantRepo, principalRepo := setupGrantRepo(t)
	ctx := context.Background()

	p1 := createTestPrincipal(t, principalRepo, "alice")
	p2 := createTestPrincipal(t, principalRepo, "bob")

	// Both users get SELECT on same table.
	for _, p := range []*domain.Principal{p1, p2} {
		_, err := grantRepo.Grant(ctx, &domain.PrivilegeGrant{
			PrincipalID:   p.ID,
			PrincipalType: "user",
			SecurableType: "table",
			SecurableID:   "shared-table",
			Privilege:     "SELECT",
		})
		require.NoError(t, err)
	}

	grants, total, err := grantRepo.ListForSecurable(ctx, "table", "shared-table", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, grants, 2)
}

func TestGrantRepo_HasPrivilege_False(t *testing.T) {
	grantRepo, principalRepo := setupGrantRepo(t)
	ctx := context.Background()

	p := createTestPrincipal(t, principalRepo, "eve")

	has, err := grantRepo.HasPrivilege(ctx, p.ID, "user", "table", "nonexistent", "SELECT")
	require.NoError(t, err)
	assert.False(t, has)
}
