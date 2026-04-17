package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestProjectAuthoringRepos_DependencySourceAndSeedLifecycle(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	workspaceRepo := NewWorkspaceRepo(writeDB)
	projectRepo := NewProjectRepo(writeDB)
	dependencyRepo := NewProjectDependencyRepo(writeDB)
	sourceRepo := NewSourceDefinitionRepo(writeDB)
	seedRepo := NewSeedRepo(writeDB)

	workspace, err := workspaceRepo.Create(ctx, &domain.Workspace{
		Name:      "Authoring Workspace",
		Kind:      domain.WorkspaceKindShared,
		CreatedBy: "alice",
	})
	require.NoError(t, err)

	project, err := projectRepo.Create(ctx, &domain.Project{
		WorkspaceID:   workspace.ID,
		Name:          "analytics",
		Kind:          domain.ProjectKindShared,
		DefaultBranch: "main",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)

	dependency, err := dependencyRepo.Create(ctx, &domain.ProjectDependency{
		ProjectID:         project.ID,
		DependencyProject: "shared_lib",
		Position:          2,
		CreatedBy:         "alice",
	})
	require.NoError(t, err)
	assert.Equal(t, "analytics", dependency.ProjectName)
	assert.Equal(t, "project", dependency.DependencyKind)

	dependencies, err := dependencyRepo.ListByProject(ctx, project.ID)
	require.NoError(t, err)
	require.Len(t, dependencies, 1)
	assert.Equal(t, "shared_lib", dependencies[0].DependencyProject)

	source, err := sourceRepo.Create(ctx, &domain.SourceDefinition{
		ProjectName: "analytics",
		SourceName:  "raw",
		TableName:   "orders",
		RelationRef: "lake.raw.orders",
		Description: "raw order feed",
		Freshness: &domain.SourceFreshnessPolicy{
			TimestampColumn: "loaded_at",
			MaxLagSeconds:   3600,
		},
		CreatedBy: "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, source.Freshness)
	assert.Equal(t, "loaded_at", source.Freshness.TimestampColumn)

	source.Description = "updated source"
	source.RelationRef = "sandbox.raw.orders"
	source.Freshness = nil
	updatedSource, err := sourceRepo.Update(ctx, source.ID, source)
	require.NoError(t, err)
	assert.Equal(t, "updated source", updatedSource.Description)
	require.NotNil(t, updatedSource.Freshness)
	assert.Equal(t, int64(3600), updatedSource.Freshness.MaxLagSeconds)

	gotSource, err := sourceRepo.GetByName(ctx, "analytics", "raw", "orders")
	require.NoError(t, err)
	assert.Equal(t, "sandbox.raw.orders", gotSource.RelationRef)

	listedSources, err := sourceRepo.ListByProject(ctx, "analytics")
	require.NoError(t, err)
	require.Len(t, listedSources, 1)

	seed, err := seedRepo.Create(ctx, &domain.Seed{
		ProjectName: "analytics",
		Name:        "seed_orders",
		Description: "seed data",
		InputRef:    "fixtures/orders.csv",
		Format:      "csv",
		Delimiter:   ",",
		HasHeader:   true,
		ColumnTypes: map[string]string{"order_id": "INTEGER"},
		Tags:        []string{"seed", "finance"},
		CreatedBy:   "alice",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"seed", "finance"}, seed.Tags)

	seed.Description = "updated seed"
	seed.Format = "json"
	seed.InputRef = "fixtures/orders.json"
	seed.HasHeader = false
	seed.Tags = []string{"json"}
	updatedSeed, err := seedRepo.Update(ctx, seed.ID, seed)
	require.NoError(t, err)
	assert.Equal(t, "json", updatedSeed.Format)
	assert.False(t, updatedSeed.HasHeader)
	assert.Equal(t, []string{"json"}, updatedSeed.Tags)

	gotSeed, err := seedRepo.GetByName(ctx, "analytics", "seed_orders")
	require.NoError(t, err)
	assert.Equal(t, "fixtures/orders.json", gotSeed.InputRef)

	listedSeeds, err := seedRepo.ListByProject(ctx, "analytics")
	require.NoError(t, err)
	require.Len(t, listedSeeds, 1)

	require.NoError(t, sourceRepo.Delete(ctx, source.ID))
	require.NoError(t, seedRepo.Delete(ctx, seed.ID))
	require.NoError(t, dependencyRepo.Delete(ctx, project.ID, "shared_lib"))

	_, err = sourceRepo.GetByName(ctx, "analytics", "raw", "orders")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	require.ErrorAs(t, err, &notFound)

	_, err = seedRepo.GetByName(ctx, "analytics", "seed_orders")
	require.Error(t, err)
	require.ErrorAs(t, err, &notFound)

	dependencies, err = dependencyRepo.ListByProject(ctx, project.ID)
	require.NoError(t, err)
	assert.Empty(t, dependencies)
}
