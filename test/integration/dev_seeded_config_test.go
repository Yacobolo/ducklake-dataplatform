//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/db/repository"
	"duck-demo/internal/declarative"
	"duck-demo/internal/devseed"
	"duck-demo/internal/domain"
)

func TestDeclarative_RepoSeedConfigRoundTrip(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{
		WithDuckLake:         true,
		WithAssets:           true,
		WithModels:           true,
		WithSemantic:         true,
		WithAPIKeyService:    true,
		SeedDuckLakeMetadata: false,
	})

	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	inputDir := filepath.Join(repoRoot, "duck-config")
	tmpDir := t.TempDir()
	renderedDir := filepath.Join(tmpDir, "rendered")
	sampleCatalogDir := filepath.Join(tmpDir, "sample-catalog")
	require.NoError(t, os.MkdirAll(filepath.Join(sampleCatalogDir, "data"), 0o755))
	tripsPath := filepath.Join(tmpDir, "yellow_tripdata_2024-01.parquet")
	zonesPath := filepath.Join(tmpDir, "taxi_zone_lookup.csv")
	require.NoError(t, os.WriteFile(zonesPath, []byte("LocationID,Borough,Zone,service_zone\n132,Queens,JFK Airport,Airports\n161,Manhattan,Midtown Center,Yellow Zone\n"), 0o644))
	_, err := env.DuckDB.ExecContext(context.Background(), `
		COPY (
			SELECT
				TIMESTAMP '2024-01-01 08:00:00' AS tpep_pickup_datetime,
				TIMESTAMP '2024-01-01 08:20:00' AS tpep_dropoff_datetime,
				132 AS PULocationID,
				161 AS DOLocationID,
				1 AS passenger_count,
				4.2 AS trip_distance,
				25.5 AS total_amount,
				20.0 AS fare_amount,
				3.5 AS tip_amount
		) TO '`+filepath.ToSlash(tripsPath)+`' (FORMAT PARQUET)
	`)
	require.NoError(t, err)

	require.NoError(t, devseed.RenderDirectory(inputDir, renderedDir, map[string]string{
		devseed.PlaceholderBootstrapUser:   "admin_user",
		devseed.PlaceholderSampleMetastore: filepath.ToSlash(filepath.Join(sampleCatalogDir, "ducklake_sample_data.sqlite")),
		devseed.PlaceholderSampleDataDir:   filepath.ToSlash(filepath.Join(sampleCatalogDir, "data")),
		devseed.PlaceholderTaxiTripsPath:   filepath.ToSlash(tripsPath),
		devseed.PlaceholderTaxiZonesPath:   filepath.ToSlash(zonesPath),
	}))

	desired, err := declarative.LoadDirectory(renderedDir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	require.NotEmpty(t, plan.Actions)
	require.Empty(t, plan.Errors)
	executeActions(t, stateClient, nonDeleteActions(plan.Actions))

	workspaceRepo := repository.NewWorkspaceRepo(env.MetaDB)
	workspaces, _, err := workspaceRepo.List(context.Background(), domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	assert.Contains(t, workspaceNames(workspacesToResources(workspaces)), "dev-admin")
	assert.Contains(t, workspaceNames(workspacesToResources(workspaces)), "analyst-sandbox")

	projectRepo := repository.NewProjectRepo(env.MetaDB)
	project, err := projectRepo.GetByName(context.Background(), "taxi-analytics")
	require.NoError(t, err)

	folderRepo := repository.NewFolderRepo(env.MetaDB)
	folders, err := folderRepo.ListByWorkspace(context.Background(), project.WorkspaceID)
	require.NoError(t, err)
	assert.Contains(t, folderNames(foldersToResources(folders)), "analytics")

	environmentRepo := repository.NewEnvironmentRepo(env.MetaDB)
	environment, err := environmentRepo.GetByName(context.Background(), project.ID, "dev")
	require.NoError(t, err)
	assert.Equal(t, "seeded_local", environment.TargetCatalog)

	notebookRepo := repository.NewNotebookRepo(env.MetaDB)
	notebooks, _, err := notebookRepo.ListNotebooks(context.Background(), nil, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	assert.Contains(t, notebookNames(notebooksToResources(notebooks)), "nyc_taxi_explore")

	dashboardRepo := repository.NewDashboardRepo(env.MetaDB)
	dashboards, _, err := dashboardRepo.List(context.Background(), nil, domain.PageRequest{MaxResults: 100})
	require.NoError(t, err)
	assert.Contains(t, dashboardNames(dashboardsToResources(dashboards)), "nyc-taxi-ops")
}

func workspaceNames(items []declarative.WorkspaceResource) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func folderNames(items []declarative.FolderResource) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func projectNames(items []declarative.ProjectResource) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func environmentNames(items []declarative.EnvironmentResource) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func notebookNames(items []declarative.NotebookResource) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func dashboardNames(items []declarative.DashboardResource) []string {
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name)
	}
	return names
}

func nonDeleteActions(actions []declarative.Action) []declarative.Action {
	filtered := make([]declarative.Action, 0, len(actions))
	for _, action := range actions {
		if action.Operation == declarative.OpDelete {
			continue
		}
		filtered = append(filtered, action)
	}
	return filtered
}

func workspacesToResources(items []domain.Workspace) []declarative.WorkspaceResource {
	out := make([]declarative.WorkspaceResource, 0, len(items))
	for _, item := range items {
		out = append(out, declarative.WorkspaceResource{Name: item.Name})
	}
	return out
}

func foldersToResources(items []domain.Folder) []declarative.FolderResource {
	out := make([]declarative.FolderResource, 0, len(items))
	for _, item := range items {
		out = append(out, declarative.FolderResource{Name: item.Name})
	}
	return out
}

func notebooksToResources(items []domain.Notebook) []declarative.NotebookResource {
	out := make([]declarative.NotebookResource, 0, len(items))
	for _, item := range items {
		out = append(out, declarative.NotebookResource{Name: item.Name})
	}
	return out
}

func dashboardsToResources(items []domain.Dashboard) []declarative.DashboardResource {
	out := make([]declarative.DashboardResource, 0, len(items))
	for _, item := range items {
		out = append(out, declarative.DashboardResource{Name: item.Name})
	}
	return out
}
