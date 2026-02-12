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

func setupGroupService(t *testing.T) (*GroupService, *PrincipalService) {
	t.Helper()
	db, _ := internaldb.OpenTestSQLite(t)
	groupRepo := repository.NewGroupRepo(db)
	principalRepo := repository.NewPrincipalRepo(db)
	auditRepo := repository.NewAuditRepo(db)
	return NewGroupService(groupRepo, auditRepo), NewPrincipalService(principalRepo, auditRepo)
}

func TestGroupService_Create_AdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(nonAdminCtx(), &domain.Group{Name: "test-group"})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_Create_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "engineering"})
	require.NoError(t, err)
	assert.Equal(t, "engineering", g.Name)
	assert.Positive(t, g.ID)
}

func TestGroupService_Create_EmptyName(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(adminCtx(), &domain.Group{Name: ""})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestGroupService_Delete_AdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "to-delete"})
	require.NoError(t, err)

	err = svc.Delete(nonAdminCtx(), g.ID)
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_Delete_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "to-delete"})
	require.NoError(t, err)

	err = svc.Delete(adminCtx(), g.ID)
	require.NoError(t, err)
}

func TestGroupService_AddMember_AdminRequired(t *testing.T) {
	svc, principalSvc := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "team"})
	require.NoError(t, err)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "member1", Type: "user"})
	require.NoError(t, err)

	err = svc.AddMember(nonAdminCtx(), &domain.GroupMember{
		GroupID:    g.ID,
		MemberID:   p.ID,
		MemberType: "user",
	})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_AddMember_AdminAllowed(t *testing.T) {
	svc, principalSvc := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "team"})
	require.NoError(t, err)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "member1", Type: "user"})
	require.NoError(t, err)

	err = svc.AddMember(adminCtx(), &domain.GroupMember{
		GroupID:    g.ID,
		MemberID:   p.ID,
		MemberType: "user",
	})
	require.NoError(t, err)
}

func TestGroupService_RemoveMember_AdminRequired(t *testing.T) {
	svc, principalSvc := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "team"})
	require.NoError(t, err)

	p, err := principalSvc.Create(adminCtx(), &domain.Principal{Name: "member1", Type: "user"})
	require.NoError(t, err)

	err = svc.AddMember(adminCtx(), &domain.GroupMember{GroupID: g.ID, MemberID: p.ID, MemberType: "user"})
	require.NoError(t, err)

	err = svc.RemoveMember(nonAdminCtx(), &domain.GroupMember{GroupID: g.ID, MemberID: p.ID, MemberType: "user"})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_List_NoAdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(adminCtx(), &domain.Group{Name: "visible"})
	require.NoError(t, err)

	groups, total, err := svc.List(nonAdminCtx(), domain.PageRequest{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, int64(1))
	assert.NotEmpty(t, groups)
}

func TestGroupService_GetByID_NoAdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "readable"})
	require.NoError(t, err)

	found, err := svc.GetByID(nonAdminCtx(), g.ID)
	require.NoError(t, err)
	assert.Equal(t, "readable", found.Name)
}

func TestGroupService_ListMembers_NoAdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), &domain.Group{Name: "team"})
	require.NoError(t, err)

	members, total, err := svc.ListMembers(nonAdminCtx(), g.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, members)
}
