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

func setupPrincipalService(t *testing.T) (*PrincipalService, *repository.PrincipalRepo) {
	t.Helper()
	db, _ := internaldb.OpenTestSQLite(t)
	principalRepo := repository.NewPrincipalRepo(db)
	auditRepo := repository.NewAuditRepo(db)
	return NewPrincipalService(principalRepo, auditRepo), principalRepo
}

func TestPrincipalService_Create_AdminRequired(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	_, err := svc.Create(nonAdminCtx(), domain.CreatePrincipalRequest{Name: "test", Type: "user"})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestPrincipalService_Create_AdminAllowed(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "new-principal", Type: "user"})
	require.NoError(t, err)
	assert.Equal(t, "new-principal", p.Name)
	assert.Equal(t, "user", p.Type)
	assert.NotEmpty(t, p.ID)
}

func TestPrincipalService_Create_EmptyName(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	_, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "", Type: "user"})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestPrincipalService_Create_InvalidType(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	_, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "test", Type: "invalid"})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestPrincipalService_Create_DefaultType(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "defaulttype"})
	require.NoError(t, err)
	assert.Equal(t, "user", p.Type)
}

func TestPrincipalService_Delete_AdminRequired(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	// Create as admin first.
	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "to-delete", Type: "user"})
	require.NoError(t, err)

	// Non-admin cannot delete.
	err = svc.Delete(nonAdminCtx(), p.ID)
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestPrincipalService_Delete_AdminAllowed(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "to-delete", Type: "user"})
	require.NoError(t, err)

	err = svc.Delete(adminCtx(), p.ID)
	require.NoError(t, err)

	_, err = svc.GetByID(ctx, p.ID)
	require.Error(t, err)
}

func TestPrincipalService_SetAdmin_AdminRequired(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "user1", Type: "user"})
	require.NoError(t, err)

	err = svc.SetAdmin(nonAdminCtx(), p.ID, true)
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestPrincipalService_SetAdmin_AdminAllowed(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "user1", Type: "user"})
	require.NoError(t, err)

	err = svc.SetAdmin(adminCtx(), p.ID, true)
	require.NoError(t, err)

	found, err := svc.GetByID(ctx, p.ID)
	require.NoError(t, err)
	assert.True(t, found.IsAdmin)
}

func TestPrincipalService_GetByID_NoAdminRequired(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "readable", Type: "user"})
	require.NoError(t, err)

	// Non-admin can read.
	found, err := svc.GetByID(nonAdminCtx(), p.ID)
	require.NoError(t, err)
	assert.Equal(t, "readable", found.Name)
}

func TestPrincipalService_List_RequiresAdmin(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	_, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "listed", Type: "user"})
	require.NoError(t, err)

	t.Run("admin_can_list", func(t *testing.T) {
		ps, total, err := svc.List(adminCtx(), domain.PageRequest{})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, int64(1))
		assert.NotEmpty(t, ps)
	})

	t.Run("non_admin_denied", func(t *testing.T) {
		_, _, err := svc.List(nonAdminCtx(), domain.PageRequest{})
		require.Error(t, err)
		var accessDenied *domain.AccessDeniedError
		assert.ErrorAs(t, err, &accessDenied)
	})
}

func TestPrincipalService_ResolveOrProvision_Existing(t *testing.T) {
	svc, repo := setupPrincipalService(t)

	issuer := "https://issuer.example.com"
	extID := "ext-123"

	// Pre-create the principal with external ID.
	_, err := repo.Create(ctx, &domain.Principal{
		Name:           "existing-user",
		Type:           "user",
		ExternalID:     &extID,
		ExternalIssuer: &issuer,
	})
	require.NoError(t, err)

	// ResolveOrProvision should find the existing one.
	p, err := svc.ResolveOrProvision(ctx, domain.ResolveOrProvisionRequest{
		Issuer: issuer, ExternalID: extID, DisplayName: "existing-user",
	})
	require.NoError(t, err)
	assert.Equal(t, "existing-user", p.Name)
}

func TestPrincipalService_ResolveOrProvision_New(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.ResolveOrProvision(ctx, domain.ResolveOrProvisionRequest{
		Issuer: "https://issuer.example.com", ExternalID: "new-ext-id", DisplayName: "New User",
	})
	require.NoError(t, err)
	assert.Equal(t, "new user", p.Name) // sanitized: lowercased, trimmed
	assert.False(t, p.IsAdmin)
	assert.NotNil(t, p.ExternalID)
}

func TestPrincipalService_ResolveOrProvision_Bootstrap(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	p, err := svc.ResolveOrProvision(ctx, domain.ResolveOrProvisionRequest{
		Issuer: "https://issuer.example.com", ExternalID: "bootstrap-sub", DisplayName: "Bootstrap Admin", IsBootstrap: true,
	})
	require.NoError(t, err)
	assert.True(t, p.IsAdmin)
}

func TestPrincipalService_GetByName(t *testing.T) {
	svc, _ := setupPrincipalService(t)

	_, err := svc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "findme", Type: "user"})
	require.NoError(t, err)

	found, err := svc.GetByName(ctx, "findme")
	require.NoError(t, err)
	assert.Equal(t, "findme", found.Name)
}
