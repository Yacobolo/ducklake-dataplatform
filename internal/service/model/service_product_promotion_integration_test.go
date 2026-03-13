//go:build integration

package model

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	productsvc "duck-demo/internal/service/product"
)

func TestIntegration_ModelRunProducesBuildForProductPublish(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	duckDB, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = duckDB.Close() })
	_, err = duckDB.ExecContext(ctx, "CREATE SCHEMA analytics")
	require.NoError(t, err)

	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	auditRepo := repository.NewAuditRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	environmentRepo := repository.NewEnvironmentRepo(writeDB)
	buildRepo := repository.NewBuildRepo(writeDB)
	modelRepo := repository.NewModelRepo(writeDB)
	modelRunRepo := repository.NewModelRunRepo(writeDB)
	modelTestRepo := repository.NewModelTestRepo(writeDB)
	modelTestResultRepo := repository.NewModelTestResultRepo(writeDB)

	productSvc := productsvc.NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo, auditRepo)
	productSvc.SetBuildRepository(buildRepo)
	productSvc.SetProjectRepository(projectRepo)

	assetKey := "memory.analytics.orders"
	_, err = assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  assetKey,
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)

	productDetail, err := productSvc.CreateProduct(ctx, domain.CreateDataProductRequest{
		Slug:              "orders",
		Name:              "Orders",
		DomainName:        "Revenue",
		TeamName:          "Analytics Engineering",
		StewardPrincipal:  "alice",
		ContactChannel:    "#rev-data",
		DocsURL:           "https://docs.example.com/orders",
		AccessRequestPath: "/access/orders",
		Contract: domain.ProductContract{
			DataGrain:            "one row per order",
			UpdateCadence:        "hourly",
			BreakingChangePolicy: "new version required",
		},
		SLO:             domain.ProductSLO{FreshnessSLO: "60m"},
		PrimaryAssetKey: &assetKey,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	project, err := projectRepo.Create(ctx, &domain.Project{
		Name:          "orders-authoring",
		Kind:          domain.ProjectKindShared,
		OwnerTeamID:   &productDetail.Product.OwnerTeamID,
		ProductID:     &productDetail.Product.ID,
		DefaultBranch: "main",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)

	_, err = environmentRepo.Create(ctx, &domain.Environment{
		ProjectID:     project.ID,
		Name:          "dev",
		Kind:          domain.EnvironmentKindDevelopment,
		TargetCatalog: "memory",
		TargetSchema:  "analytics",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)

	_, err = modelRepo.Create(ctx, &domain.Model{
		ProjectName:     project.Name,
		Name:            "orders",
		SQL:             "select 1 as order_id",
		Materialization: domain.MaterializationTable,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	modelSvc := NewService(ServiceDeps{
		Models:       modelRepo,
		Runs:         modelRunRepo,
		Projects:     projectRepo,
		Environments: environmentRepo,
		Builds:       buildRepo,
		Tests:        modelTestRepo,
		TestResults:  modelTestResultRepo,
		Audit:        auditRepo,
		Engine:       passthroughSessionEngine{},
		DuckDB:       duckDB,
		Logger:       slog.New(slog.DiscardHandler),
	})

	err = modelSvc.TriggerRunSync(ctx, "alice", domain.TriggerModelRunRequest{
		ProjectName:     project.Name,
		EnvironmentName: "dev",
	})
	require.NoError(t, err)

	runs, total, err := modelRunRepo.ListRuns(ctx, domain.ModelRunFilter{Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	require.EqualValues(t, 1, total)
	require.Len(t, runs, 1)
	require.NotNil(t, runs[0].BuildID)

	build, err := buildRepo.GetByID(ctx, *runs[0].BuildID)
	require.NoError(t, err)
	assert.Equal(t, project.ID, build.ProjectID)
	require.NotNil(t, build.ProductID)
	assert.Equal(t, productDetail.Product.ID, *build.ProductID)
	assert.Equal(t, "refs/heads/main", build.GitRef)
	assert.Equal(t, domain.BuildStateReady, build.State)

	_, err = productSvc.CreateVersion(ctx, "orders", domain.CreateDataProductVersionRequest{
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		Contract:           productDetail.Product.Contract,
		SLO:                productDetail.Product.SLO,
		DocsURL:            productDetail.Product.DocsURL,
		AccessRequestPath:  productDetail.Product.AccessRequestPath,
		ProducingBuildID:   runs[0].BuildID,
		OutputAssetKeys:    []string{assetKey},
		CreatedBy:          "alice",
	})
	require.NoError(t, err)

	published, err := productSvc.PublishVersion(ctx, "orders", 2)
	require.NoError(t, err)
	require.NotNil(t, published.Status)
	assert.Equal(t, domain.ProductReleaseStatePublished, published.Status.PublicationState)

	build, err = buildRepo.GetByID(ctx, *runs[0].BuildID)
	require.NoError(t, err)
	assert.Equal(t, domain.BuildStateReleased, build.State)
}
