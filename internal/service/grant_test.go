package service

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

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "grantee", Type: "user"})
	require.NoError(t, err)

	_, err = svc.Grant(nonAdminCtx(), "grantee", &domain.PrivilegeGrant{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   1,
	})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGrantService_Grant_AdminAllowed(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "grantee", Type: "user"})
	require.NoError(t, err)

	grant, err := svc.Grant(adminCtx(), "admin-user", &domain.PrivilegeGrant{
		PrincipalID:   p.ID,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   1,
	})
	require.NoError(t, err)
	assert.Equal(t, "SELECT", grant.Privilege)
	assert.Positive(t, grant.ID)
}

func TestGrantService_Revoke_AdminRequired(t *testing.T) {
	svc, _ := setupGrantService(t)

	err := svc.Revoke(nonAdminCtx(), "user", &domain.PrivilegeGrant{
		PrincipalID:   1,
		PrincipalType: "user",
		Privilege:     "SELECT",
		SecurableType: "table",
		SecurableID:   1,
	})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGrantService_List_NoAdminRequired(t *testing.T) {
	svc, principalSvc := setupGrantService(t)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "lister", Type: "user"})
	require.NoError(t, err)

	// Non-admin can list grants (even if none exist).
	grants, total, err := svc.ListForPrincipal(nonAdminCtx(), p.ID, "user", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, grants)
}

func TestGrantService_Grant_EmptyPrivilege(t *testing.T) {
	svc, _ := setupGrantService(t)

	_, err := svc.Grant(adminCtx(), "admin-user", &domain.PrivilegeGrant{
		PrincipalID:   1,
		PrincipalType: "user",
		Privilege:     "", // empty
		SecurableType: "table",
		SecurableID:   1,
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}
