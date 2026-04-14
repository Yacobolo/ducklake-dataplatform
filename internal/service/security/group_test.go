//go:build integration

package security

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/db/repository"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func setupGroupService(t *testing.T) (*GroupService, *PrincipalService) {
	t.Helper()
	db, _ := internaldb.OpenTestSQLite(t)
	groupRepo := repository.NewGroupRepo(db)
	principalRepo := repository.NewPrincipalRepo(db)
	auditRepo := repository.NewAuditRepo(db)
	return NewGroupService(groupRepo, principalRepo, auditRepo), NewPrincipalService(principalRepo, auditRepo)
}

func TestGroupService_Create_AdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(nonAdminCtx(), domain.CreateGroupRequest{Name: "test-group"})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_Create_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "engineering"})
	require.NoError(t, err)
	assert.Equal(t, "engineering", g.Name)
	assert.NotEmpty(t, g.ID)
}

func TestGroupService_Create_EmptyName(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: ""})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestGroupService_Delete_AdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "to-delete"})
	require.NoError(t, err)

	err = svc.Delete(nonAdminCtx(), g.ID)
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_Update_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	group, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team", Description: "Old"})
	require.NoError(t, err)

	description := "New"
	updated, err := svc.Update(adminCtx(), group.ID, domain.UpdateGroupRequest{Description: &description})
	require.NoError(t, err)
	assert.Equal(t, "New", updated.Description)
}

func TestGroupService_Update_AdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	group, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team", Description: "Old"})
	require.NoError(t, err)

	description := "New"
	_, err = svc.Update(nonAdminCtx(), group.ID, domain.UpdateGroupRequest{Description: &description})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_Delete_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "to-delete"})
	require.NoError(t, err)

	err = svc.Delete(adminCtx(), g.ID)
	require.NoError(t, err)
}

func TestGroupService_AddMember_AdminRequired(t *testing.T) {
	svc, principalSvc := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team"})
	require.NoError(t, err)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "member1", Type: "user"})
	require.NoError(t, err)

	err = svc.AddMember(nonAdminCtx(), domain.AddGroupMemberRequest{
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

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team"})
	require.NoError(t, err)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "member1", Type: "user"})
	require.NoError(t, err)

	err = svc.AddMember(adminCtx(), domain.AddGroupMemberRequest{
		GroupID:    g.ID,
		MemberID:   p.ID,
		MemberType: "user",
	})
	require.NoError(t, err)
}

func TestGroupService_AddMember_RejectsMissingMember(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team"})
	require.NoError(t, err)

	err = svc.AddMember(adminCtx(), domain.AddGroupMemberRequest{
		GroupID:    g.ID,
		MemberID:   "00000000-0000-0000-0000-000000000000",
		MemberType: "user",
	})
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestGroupService_AddMember_RejectsSelfCycle(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team"})
	require.NoError(t, err)

	err = svc.AddMember(adminCtx(), domain.AddGroupMemberRequest{
		GroupID:    g.ID,
		MemberID:   g.ID,
		MemberType: "group",
	})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestGroupService_RemoveMember_AdminRequired(t *testing.T) {
	svc, principalSvc := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team"})
	require.NoError(t, err)

	p, err := principalSvc.Create(adminCtx(), domain.CreatePrincipalRequest{Name: "member1", Type: "user"})
	require.NoError(t, err)

	err = svc.AddMember(adminCtx(), domain.AddGroupMemberRequest{GroupID: g.ID, MemberID: p.ID, MemberType: "user"})
	require.NoError(t, err)

	err = svc.RemoveMember(nonAdminCtx(), domain.RemoveGroupMemberRequest{GroupID: g.ID, MemberID: p.ID, MemberType: "user"})
	require.Error(t, err)
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_List_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "visible"})
	require.NoError(t, err)

	groups, total, err := svc.List(adminCtx(), domain.PageRequest{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, int64(1))
	assert.NotEmpty(t, groups)
}

func TestGroupService_GetByID_NoAdminRequired(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "readable"})
	require.NoError(t, err)

	found, err := svc.GetByID(nonAdminCtx(), g.ID)
	require.NoError(t, err)
	assert.Equal(t, "readable", found.Name)
}

func TestGroupService_ListMembers_AdminAllowed(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "team"})
	require.NoError(t, err)

	members, total, err := svc.ListMembers(adminCtx(), g.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, members)
}

func TestGroupService_ListMembers_MissingGroup(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, _, err := svc.ListMembers(adminCtx(), "00000000-0000-0000-0000-000000000000", domain.PageRequest{})
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestGroupService_List_RequiresAdmin(t *testing.T) {
	svc, _ := setupGroupService(t)

	_, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "secret-group"})
	require.NoError(t, err)

	// Non-admin should NOT be able to list all groups.
	_, _, err = svc.List(nonAdminCtx(), domain.PageRequest{})
	require.Error(t, err, "non-admin should not be able to list groups")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}

func TestGroupService_ListMembers_RequiresAdmin(t *testing.T) {
	svc, _ := setupGroupService(t)

	g, err := svc.Create(adminCtx(), domain.CreateGroupRequest{Name: "sensitive-team"})
	require.NoError(t, err)

	// Non-admin should NOT be able to list group members.
	_, _, err = svc.ListMembers(nonAdminCtx(), g.ID, domain.PageRequest{})
	require.Error(t, err, "non-admin should not be able to list group members")
	var accessDenied *domain.AccessDeniedError
	assert.ErrorAs(t, err, &accessDenied)
}
