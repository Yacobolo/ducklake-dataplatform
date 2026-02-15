package security

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

func setupGrantService(t *testing.T) (*GrantService, *PrincipalService) {
	t.Helper()
	db, _ := internaldb.OpenTestSQLite(t)
	grantRepo := repository.NewGrantRepo(db)
	auditRepo := repository.NewAuditRepo(db)
	principalRepo := repository.NewPrincipalRepo(db)
	return NewGrantService(grantRepo, auditRepo), NewPrincipalService(principalRepo, auditRepo)
}

func TestGrantService_Grant_AdminRequired(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "grantee", Type: "user"})
	require.NoError(t, err)

	_, err = svc.Grant(nonAdminCtx(), domain.CreateGrantRequest{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   "1",
	})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGrantService_Grant_AdminAllowed(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "grantee", Type: "user"})
	require.NoError(t, err)

	grant, err := svc.Grant(adminCtx(), domain.CreateGrantRequest{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   "1",
	})
	require.NoError(t, err)
	assert.Equal(t, "SELECT", grant.Privilege)
	assert.NotEmpty(t, grant.ID)
	require.NotNil(t, grant.GrantedBy, "granted_by should be set")
	assert.Equal(t, "admin-user", *grant.GrantedBy, "granted_by should be the caller's name")
}

func TestGrantService_Revoke_AdminRequired(t *testing.T) {
	svc, _ := setupGrantService(t)

	err := svc.Revoke(nonAdminCtx(), "user", "1")
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGrantService_Revoke_AdminAllowed(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "grantee", Type: "user"})
	require.NoError(t, err)

	grant, err := svc.Grant(adminCtx(), domain.CreateGrantRequest{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   "1",
	})
	require.NoError(t, err)

	err = svc.Revoke(adminCtx(), "admin-user", grant.ID)
	require.NoError(t, err)
}

func TestGrantService_List_AdminAllowed(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "lister", Type: "user"})
	require.NoError(t, err)

	// Admin can list grants.
	grants, total, err := svc.ListForPrincipal(adminCtx(), p.ID, "user", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, grants)
}

func TestGrantService_Grant_EmptyPrivilege(t *testing.T) {
	svc, _ := setupGrantService(t)

	_, err := svc.Grant(adminCtx(), domain.CreateGrantRequest{
		PrincipalID:   "1",
		PrincipalType: "user",
		Privilege:     "", // empty
		SecurableType: "table",
		SecurableID:   "1",
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestGrantService_ListForPrincipal_RequiresAdmin(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "target", Type: "user"})
	require.NoError(t, err)

	// Create a grant for the target.
	_, err = svc.Grant(adminCtx(), domain.CreateGrantRequest{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   "1",
	})
	require.NoError(t, err)

	// Non-admin should NOT be able to list another user's grants.
	_, _, err = svc.ListForPrincipal(nonAdminCtx(), p.ID, "user", domain.PageRequest{})
	require.Error(t, err, "non-admin should not list other users' grants")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGrantService_ListForSecurable_RequiresAdmin(t *testing.T) {
	svc, _ := setupGrantService(t)

	// Non-admin should NOT be able to list all grants on a securable.
	_, _, err := svc.ListForSecurable(nonAdminCtx(), "table", "1", domain.PageRequest{})
	require.Error(t, err, "non-admin should not list grants for a securable")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}
