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

func setupGroupRepo(t *testing.T) (*GroupRepo, *PrincipalRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewGroupRepo(writeDB), NewPrincipalRepo(writeDB)
}

func TestGroupRepo_CRUD(t *testing.T) {
	groupRepo, _ := setupGroupRepo(t)
	ctx := context.Background()

	// Create.
	g, err := groupRepo.Create(ctx, &domain.Group{
		Name:        "analysts",
		Description: "Data analysts team",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, g.ID)
	assert.Equal(t, "analysts", g.Name)
	assert.Equal(t, "Data analysts team", g.Description)
	assert.False(t, g.CreatedAt.IsZero())

	// GetByID.
	found, err := groupRepo.GetByID(ctx, g.ID)
	require.NoError(t, err)
	assert.Equal(t, g.ID, found.ID)
	assert.Equal(t, "analysts", found.Name)

	// GetByName.
	found, err = groupRepo.GetByName(ctx, "analysts")
	require.NoError(t, err)
	assert.Equal(t, g.ID, found.ID)

	// Delete.
	err = groupRepo.Delete(ctx, g.ID)
	require.NoError(t, err)
}

func TestGroupRepo_GetByName_NotFound(t *testing.T) {
	groupRepo, _ := setupGroupRepo(t)
	ctx := context.Background()

	_, err := groupRepo.GetByName(ctx, "nonexistent")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestGroupRepo_List(t *testing.T) {
	groupRepo, _ := setupGroupRepo(t)
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := groupRepo.Create(ctx, &domain.Group{Name: name})
		require.NoError(t, err)
	}

	groups, total, err := groupRepo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), total)
	assert.Len(t, groups, 3)
}

func TestGroupRepo_UniqueNameConstraint(t *testing.T) {
	groupRepo, _ := setupGroupRepo(t)
	ctx := context.Background()

	_, err := groupRepo.Create(ctx, &domain.Group{Name: "dup_group"})
	require.NoError(t, err)

	_, err = groupRepo.Create(ctx, &domain.Group{Name: "dup_group"})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestGroupRepo_AddAndRemoveMember(t *testing.T) {
	groupRepo, principalRepo := setupGroupRepo(t)
	ctx := context.Background()

	g, err := groupRepo.Create(ctx, &domain.Group{Name: "team"})
	require.NoError(t, err)

	p, err := principalRepo.Create(ctx, &domain.Principal{Name: "member1", Type: "user"})
	require.NoError(t, err)

	// Add member.
	err = groupRepo.AddMember(ctx, &domain.GroupMember{
		GroupID:    g.ID,
		MemberType: "user",
		MemberID:   p.ID,
	})
	require.NoError(t, err)

	// List members.
	members, total, err := groupRepo.ListMembers(ctx, g.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, members, 1)
	assert.Equal(t, p.ID, members[0].MemberID)

	// Remove member.
	err = groupRepo.RemoveMember(ctx, &domain.GroupMember{
		GroupID:    g.ID,
		MemberType: "user",
		MemberID:   p.ID,
	})
	require.NoError(t, err)

	members, total, err = groupRepo.ListMembers(ctx, g.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, members)
}

func TestGroupRepo_GetGroupsForMember(t *testing.T) {
	groupRepo, principalRepo := setupGroupRepo(t)
	ctx := context.Background()

	g1, err := groupRepo.Create(ctx, &domain.Group{Name: "group1"})
	require.NoError(t, err)
	g2, err := groupRepo.Create(ctx, &domain.Group{Name: "group2"})
	require.NoError(t, err)

	p, err := principalRepo.Create(ctx, &domain.Principal{Name: "multi_member", Type: "user"})
	require.NoError(t, err)

	// Add to both groups.
	for _, g := range []*domain.Group{g1, g2} {
		err = groupRepo.AddMember(ctx, &domain.GroupMember{
			GroupID:    g.ID,
			MemberType: "user",
			MemberID:   p.ID,
		})
		require.NoError(t, err)
	}

	groups, err := groupRepo.GetGroupsForMember(ctx, "user", p.ID)
	require.NoError(t, err)
	assert.Len(t, groups, 2)
}

func TestGroupRepo_ListMembers_Empty(t *testing.T) {
	groupRepo, _ := setupGroupRepo(t)
	ctx := context.Background()

	g, err := groupRepo.Create(ctx, &domain.Group{Name: "empty_group"})
	require.NoError(t, err)

	members, total, err := groupRepo.ListMembers(ctx, g.ID, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, members)
}
