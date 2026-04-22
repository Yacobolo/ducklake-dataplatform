package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestProjectRepo_CreateEnvironmentAndBuildLifecycle(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	groupRepo := NewGroupRepo(writeDB)
	workspaceRepo := NewWorkspaceRepo(writeDB)
	projectRepo := NewProjectRepo(writeDB)
	environmentRepo := NewEnvironmentRepo(writeDB)
	group, err := groupRepo.Create(ctx, &domain.Group{Name: "analytics-engineering"})
	require.NoError(t, err)
	ownerGroupID := group.ID
	workspace, err := workspaceRepo.Create(ctx, &domain.Workspace{
		Name:         "Revenue Workspace",
		Kind:         domain.WorkspaceKindShared,
		OwnerGroupID: &ownerGroupID,
		CreatedBy:    "alice",
	})
	require.NoError(t, err)

	project, err := projectRepo.Create(ctx, &domain.Project{
		WorkspaceID:    workspace.ID,
		Name:           "rev-orders",
		Kind:           domain.ProjectKindShared,
		Description:    "Revenue orders authoring",
		OwnerGroupID:   &ownerGroupID,
		DefaultBranch:  "main",
		CreatedBy:      "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, project.OwnerGroupID)
	assert.Equal(t, ownerGroupID, *project.OwnerGroupID)

	environment, err := environmentRepo.Create(ctx, &domain.Environment{
		ProjectID:     project.ID,
		Name:          "dev",
		Kind:          domain.EnvironmentKindDevelopment,
		TargetCatalog: "rev_orders_dev",
		TargetSchema:  "alice",
		Variables:     map[string]string{"target_name": "dev"},
		CreatedBy:     "alice",
	})
	require.NoError(t, err)
	assert.Equal(t, "rev-orders", environment.ProjectName)

	got, err := environmentRepo.GetByName(ctx, project.ID, "dev")
	require.NoError(t, err)
	assert.Equal(t, "rev_orders_dev", got.TargetCatalog)
	assert.Equal(t, "dev", got.Variables["target_name"])

	buildRepo := NewBuildRepo(writeDB)
	build, err := buildRepo.Create(ctx, &domain.Build{
		ProjectID:       project.ID,
		EnvironmentID:   environment.ID,
		GitRef:          "refs/heads/main",
		Selector:        "tag:revenue",
		TargetCatalog:   "rev_orders_dev",
		TargetSchema:    "alice",
		CompileManifest: `{"version":1,"models":[{"name":"rev_orders.fct_orders"}]}`,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)
	assert.Equal(t, "rev-orders", build.ProjectName)
	assert.Equal(t, "dev", build.EnvironmentName)

	builds, total, err := buildRepo.ListByProject(ctx, project.ID, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
	require.Len(t, builds, 1)
	assert.Equal(t, build.ID, builds[0].ID)
	assert.Equal(t, domain.BuildStateReady, builds[0].State)

	require.NoError(t, buildRepo.UpdateState(ctx, build.ID, domain.BuildStateReady))
	updated, err := buildRepo.GetByID(ctx, build.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.BuildStateReady, updated.State)
}
