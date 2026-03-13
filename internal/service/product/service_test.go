//go:build integration

package product

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

func TestService_CreateProductCreatesDraftVersionAndLinksAsset(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	svc := NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)
	svc.SetBuildRepository(repository.NewBuildRepo(writeDB))
	svc.SetProjectRepository(projectRepo)

	asset, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.daily_orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, asset)

	primaryAssetKey := "main.analytics.daily_orders"
	created, err := svc.CreateProduct(ctx, domain.CreateDataProductRequest{
		Slug:              "daily-orders",
		Name:              "Daily Orders",
		Description:       "Primary daily orders product",
		DomainName:        "Revenue",
		TeamName:          "Analytics Engineering",
		StewardPrincipal:  "alice",
		ContactChannel:    "#rev-data",
		DocsURL:           "https://docs.example.com/products/daily-orders",
		AccessRequestPath: "/access/daily-orders",
		Contract: domain.ProductContract{
			DataGrain:            "one row per order",
			UpdateCadence:        "hourly",
			BreakingChangePolicy: "new version required",
		},
		SLO:             domain.ProductSLO{FreshnessSLO: "60m"},
		PrimaryAssetKey: &primaryAssetKey,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, created)

	assert.Equal(t, "daily-orders", created.Product.Slug)
	assert.Equal(t, "Revenue", created.Domain.Name)
	assert.Equal(t, "Analytics Engineering", created.OwnerTeam.Name)
	require.Len(t, created.Versions, 1)
	assert.Equal(t, 1, created.Versions[0].Version)
	require.Len(t, created.Outputs, 1)
	assert.Equal(t, "main.analytics.daily_orders", created.Outputs[0].AssetKey)
	require.NotNil(t, created.Status)
	assert.Equal(t, domain.ProductReleaseStateDraft, created.Status.PublicationState)
}

func TestService_PublishVersionAndDependencyLifecycle(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	svc := NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)
	buildRepo := repository.NewBuildRepo(writeDB)
	svc.SetBuildRepository(buildRepo)
	svc.SetProjectRepository(projectRepo)

	_, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 3600,
		},
	})
	require.NoError(t, err)

	_, err = assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.customers",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 3600,
		},
	})
	require.NoError(t, err)

	ordersKey := "main.analytics.orders"
	_, err = svc.CreateProduct(ctx, domain.CreateDataProductRequest{
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
		PrimaryAssetKey: &ordersKey,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	_, err = svc.PublishVersion(ctx, "orders", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "producing_build_id is required")

	ordersDetail, err := svc.GetProduct(ctx, "orders")
	require.NoError(t, err)
	ordersBuildID := createBuildForProduct(t, ctx, writeDB, buildRepo, ordersDetail.Product, "alice")
	_, err = svc.CreateVersion(ctx, "orders", domain.CreateDataProductVersionRequest{
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		Contract: domain.ProductContract{
			DataGrain:            "one row per order",
			UpdateCadence:        "hourly",
			BreakingChangePolicy: "new version required",
		},
		SLO:               domain.ProductSLO{FreshnessSLO: "60m"},
		DocsURL:           "https://docs.example.com/orders",
		AccessRequestPath: "/access/orders",
		ProducingBuildID:  &ordersBuildID,
		OutputAssetKeys:   []string{ordersKey},
		CreatedBy:         "alice",
	})
	require.NoError(t, err)

	customersKey := "main.analytics.customers"
	customers, err := svc.CreateProduct(ctx, domain.CreateDataProductRequest{
		Slug:              "customers",
		Name:              "Customers",
		DomainName:        "Revenue",
		TeamName:          "Analytics Engineering",
		StewardPrincipal:  "alice",
		ContactChannel:    "#rev-data",
		DocsURL:           "https://docs.example.com/customers",
		AccessRequestPath: "/access/customers",
		Contract: domain.ProductContract{
			DataGrain:            "one row per customer",
			UpdateCadence:        "daily",
			BreakingChangePolicy: "new version required",
		},
		SLO:             domain.ProductSLO{FreshnessSLO: "24h"},
		PrimaryAssetKey: &customersKey,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	published, err := svc.PublishVersion(ctx, "orders", 2)
	require.NoError(t, err)
	require.NotNil(t, published.Status)
	assert.Equal(t, domain.ProductReleaseStatePublished, published.Status.PublicationState)
	build, err := buildRepo.GetByID(ctx, ordersBuildID)
	require.NoError(t, err)
	assert.Equal(t, domain.BuildStateReleased, build.State)

	withDependency, err := svc.AddDependency(ctx, "orders", "customers")
	require.NoError(t, err)
	require.Len(t, withDependency.Dependencies, 1)
	assert.Equal(t, customers.Product.Slug, withDependency.Dependencies[0].Product.Slug)

	subscription, err := svc.Subscribe(ctx, "orders", "alice", "freshness_breach", "inbox")
	require.NoError(t, err)
	assert.Equal(t, "alice", subscription.PrincipalName)

	deprecated, err := svc.DeprecateVersion(ctx, "orders", 2, &customers.Product.Slug)
	require.NoError(t, err)
	require.NotNil(t, deprecated.Status)
	assert.Equal(t, domain.ProductReleaseStateDeprecated, deprecated.Status.PublicationState)
	require.NotNil(t, deprecated.Status.ReplacementProductID)
	build, err = buildRepo.GetByID(ctx, ordersBuildID)
	require.NoError(t, err)
	assert.Equal(t, domain.BuildStateSuperseded, build.State)
}

func TestService_CreateAndPublishSemanticOnlyProduct(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	semanticRepo := repository.NewSemanticModelRepo(writeDB)
	svc := NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)
	buildRepo := repository.NewBuildRepo(writeDB)
	svc.SetBuildRepository(buildRepo)
	svc.SetProjectRepository(projectRepo)
	svc.SetSemanticModelRepository(semanticRepo)

	semanticModel, err := semanticRepo.Create(ctx, &domain.SemanticModel{
		ProjectName:  "sales",
		Name:         "orders",
		Description:  "Orders semantic model",
		BaseModelRef: "sales.orders",
		CreatedBy:    "alice",
	})
	require.NoError(t, err)
	require.NotNil(t, semanticModel)

	created, err := svc.CreateProduct(ctx, domain.CreateDataProductRequest{
		Slug:              "orders-semantic",
		Name:              "Orders Semantic Product",
		Description:       "Semantic-first orders product",
		DomainName:        "Revenue",
		TeamName:          "Analytics Engineering",
		StewardPrincipal:  "alice",
		ContactChannel:    "#rev-data",
		DocsURL:           "https://docs.example.com/orders-semantic",
		AccessRequestPath: "/access/orders-semantic",
		Contract: domain.ProductContract{
			DataGrain:            "one row per order",
			UpdateCadence:        "hourly",
			BreakingChangePolicy: "new version required",
		},
		SLO:               domain.ProductSLO{FreshnessSLO: "60m"},
		SemanticModelRefs: []string{"sales.orders"},
		CreatedBy:         "alice",
	})
	require.NoError(t, err)
	require.Len(t, created.SemanticEntrypoints, 1)
	assert.Equal(t, "sales", created.SemanticEntrypoints[0].ProjectName)
	assert.Equal(t, "orders", created.SemanticEntrypoints[0].ModelName)

	_, err = svc.PublishVersion(ctx, "orders-semantic", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "producing_build_id is required")

	buildID := createBuildForProduct(t, ctx, writeDB, buildRepo, created.Product, "alice")
	_, err = svc.CreateVersion(ctx, "orders-semantic", domain.CreateDataProductVersionRequest{
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		Contract: domain.ProductContract{
			DataGrain:            "one row per order",
			UpdateCadence:        "hourly",
			BreakingChangePolicy: "new version required",
		},
		SLO:               domain.ProductSLO{FreshnessSLO: "60m"},
		DocsURL:           "https://docs.example.com/orders-semantic",
		AccessRequestPath: "/access/orders-semantic",
		ProducingBuildID:  &buildID,
		SemanticModelRefs: []string{"sales.orders"},
		CreatedBy:         "alice",
	})
	require.NoError(t, err)

	published, err := svc.PublishVersion(ctx, "orders-semantic", 2)
	require.NoError(t, err)
	require.NotNil(t, published.Status)
	assert.Equal(t, domain.ProductReleaseStatePublished, published.Status.PublicationState)
	require.Len(t, published.SemanticEntrypoints, 1)
	build, err := buildRepo.GetByID(ctx, buildID)
	require.NoError(t, err)
	assert.Equal(t, domain.BuildStateReleased, build.State)
}

func TestService_GetPortfolioReport(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	semanticRepo := repository.NewSemanticModelRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	svc := NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)
	svc.SetBuildRepository(repository.NewBuildRepo(writeDB))
	svc.SetProjectRepository(projectRepo)
	svc.SetSemanticModelRepository(semanticRepo)

	_, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)
	_, err = assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.orphaned",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)
	_, err = semanticRepo.Create(ctx, &domain.SemanticModel{
		ProjectName:  "sales",
		Name:         "orphan_model",
		Description:  "Orphan semantic model",
		BaseModelRef: "sales.orders",
		CreatedBy:    "alice",
	})
	require.NoError(t, err)

	ordersKey := "main.analytics.orders"
	_, err = svc.CreateProduct(ctx, domain.CreateDataProductRequest{
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
		PrimaryAssetKey: &ordersKey,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)
	_, err = svc.Subscribe(ctx, "orders", "alice", "publication", "inbox")
	require.NoError(t, err)

	report, err := svc.GetPortfolioReport(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, report.TopUsed)
	assert.Equal(t, "orders", report.TopUsed[0].ProductSlug)
	require.NotEmpty(t, report.DomainScorecards)
	assert.Equal(t, "Revenue", report.DomainScorecards[0].Name)
	require.Len(t, report.OrphanAssets, 1)
	assert.Equal(t, "main.analytics.orphaned", report.OrphanAssets[0].ResourceName)
	require.Len(t, report.OrphanSemanticModels, 1)
	assert.Equal(t, "sales.orphan_model", report.OrphanSemanticModels[0].ResourceName)
}

func TestService_PublishVersion_RejectsNonDefaultBranchBuild(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	projectRepo := repository.NewProjectRepo(writeDB)
	buildRepo := repository.NewBuildRepo(writeDB)
	svc := NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)
	svc.SetBuildRepository(buildRepo)
	svc.SetProjectRepository(projectRepo)

	_, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.analytics.orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)

	ordersKey := "main.analytics.orders"
	created, err := svc.CreateProduct(ctx, domain.CreateDataProductRequest{
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
		PrimaryAssetKey: &ordersKey,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	project, err := projectRepo.Create(ctx, &domain.Project{
		Name:          "orders-authoring-feature",
		Kind:          domain.ProjectKindShared,
		OwnerTeamID:   &created.Product.OwnerTeamID,
		ProductID:     &created.Product.ID,
		DefaultBranch: "main",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)
	environmentRepo := repository.NewEnvironmentRepo(writeDB)
	environment, err := environmentRepo.Create(ctx, &domain.Environment{
		ProjectID:     project.ID,
		Name:          "prod",
		Kind:          domain.EnvironmentKindProduction,
		TargetCatalog: "main",
		TargetSchema:  "analytics",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)
	build, err := buildRepo.Create(ctx, &domain.Build{
		ProjectID:       project.ID,
		ProductID:       &created.Product.ID,
		EnvironmentID:   environment.ID,
		GitRef:          "refs/heads/feature",
		Selector:        "state:modified",
		TargetCatalog:   "main",
		TargetSchema:    "analytics",
		CompileManifest: `{"version":1}`,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	_, err = svc.CreateVersion(ctx, "orders", domain.CreateDataProductVersionRequest{
		CompatibilityLevel: domain.ProductCompatibilityBackwardCompatible,
		Contract:           created.Product.Contract,
		SLO:                created.Product.SLO,
		DocsURL:            created.Product.DocsURL,
		AccessRequestPath:  created.Product.AccessRequestPath,
		ProducingBuildID:   &build.ID,
		OutputAssetKeys:    []string{ordersKey},
		CreatedBy:          "alice",
	})
	require.NoError(t, err)

	_, err = svc.PublishVersion(ctx, "orders", 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default branch or a tag")
}

func createBuildForProduct(
	t *testing.T,
	ctx context.Context,
	writeDB *sql.DB,
	buildRepo *repository.BuildRepo,
	product domain.DataProduct,
	createdBy string,
) string {
	t.Helper()

	projectRepo := repository.NewProjectRepo(writeDB)
	environmentRepo := repository.NewEnvironmentRepo(writeDB)

	project, err := projectRepo.Create(ctx, &domain.Project{
		Name:          product.Slug + "-authoring",
		Kind:          domain.ProjectKindShared,
		OwnerTeamID:   &product.OwnerTeamID,
		ProductID:     &product.ID,
		DefaultBranch: "main",
		CreatedBy:     createdBy,
	})
	require.NoError(t, err)

	environment, err := environmentRepo.Create(ctx, &domain.Environment{
		ProjectID:     project.ID,
		Name:          "prod",
		Kind:          domain.EnvironmentKindProduction,
		TargetCatalog: product.Slug,
		TargetSchema:  "analytics",
		CreatedBy:     createdBy,
	})
	require.NoError(t, err)

	build, err := buildRepo.Create(ctx, &domain.Build{
		ProjectID:       project.ID,
		ProductID:       &product.ID,
		EnvironmentID:   environment.ID,
		GitRef:          "refs/heads/main",
		Selector:        "state:modified",
		TargetCatalog:   product.Slug,
		TargetSchema:    "analytics",
		CompileManifest: `{"version":1,"models":[{"name":"` + product.Slug + `.primary"}]}`,
		CreatedBy:       createdBy,
	})
	require.NoError(t, err)
	return build.ID
}
