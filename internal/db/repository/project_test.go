package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestProjectRepo_CreateListByProductEnvironmentAndBuildLifecycle(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	domainRepo := NewDomainRepo(writeDB)
	teamRepo := NewTeamRepo(writeDB)
	productRepo := NewDataProductRepo(writeDB)
	workspaceRepo := NewWorkspaceRepo(writeDB)
	projectRepo := NewProjectRepo(writeDB)
	environmentRepo := NewEnvironmentRepo(writeDB)

	domainItem, err := domainRepo.Create(ctx, &domain.Domain{Name: "Revenue"})
	require.NoError(t, err)
	teamItem, err := teamRepo.Create(ctx, &domain.Team{
		DomainID:       domainItem.ID,
		Name:           "Analytics Engineering",
		ContactChannel: "#rev-data",
	})
	require.NoError(t, err)
	productItem, err := productRepo.Create(ctx, &domain.DataProduct{
		Slug:              "daily-orders",
		Name:              "Daily Orders",
		DomainID:          domainItem.ID,
		OwnerTeamID:       teamItem.ID,
		StewardPrincipal:  "alice",
		ContactChannel:    "#rev-data",
		PublicationIntent: domain.ProductPublicationIntentDraft,
		CreatedBy:         "alice",
	})
	require.NoError(t, err)
	workspace, err := workspaceRepo.Create(ctx, &domain.Workspace{
		Name:        "Revenue Workspace",
		Kind:        domain.WorkspaceKindShared,
		OwnerTeamID: &teamItem.ID,
		CreatedBy:   "alice",
	})
	require.NoError(t, err)

	project, err := projectRepo.Create(ctx, &domain.Project{
		WorkspaceID:   workspace.ID,
		Name:          "rev-orders",
		Kind:          domain.ProjectKindShared,
		Description:   "Revenue orders authoring",
		OwnerTeamID:   &teamItem.ID,
		ProductID:     &productItem.ID,
		DefaultBranch: "main",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, project.ProductID)
	assert.Equal(t, productItem.ID, *project.ProductID)

	projects, total, err := projectRepo.ListByProduct(ctx, productItem.ID, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
	require.Len(t, projects, 1)
	assert.Equal(t, "rev-orders", projects[0].Name)

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
		ProductID:       &productItem.ID,
		EnvironmentID:   environment.ID,
		GitRef:          "refs/heads/main",
		Selector:        "tag:revenue",
		TargetCatalog:   "rev_orders_dev",
		TargetSchema:    "alice",
		CompileManifest: `{"version":1,"models":[{"name":"rev_orders.fct_orders"}]}`,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, build.ProductID)
	assert.Equal(t, productItem.ID, *build.ProductID)
	assert.Equal(t, "rev-orders", build.ProjectName)
	assert.Equal(t, "dev", build.EnvironmentName)

	builds, total, err := buildRepo.ListByProject(ctx, project.ID, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 1, total)
	require.Len(t, builds, 1)
	assert.Equal(t, build.ID, builds[0].ID)
	assert.Equal(t, domain.BuildStateReady, builds[0].State)

	require.NoError(t, buildRepo.UpdateState(ctx, build.ID, domain.BuildStateReleased))
	updated, err := buildRepo.GetByID(ctx, build.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.BuildStateReleased, updated.State)
}
