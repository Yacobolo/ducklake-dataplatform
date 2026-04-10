// Package app provides application-level wiring and dependency injection
// for the duck-demo application following hexagonal architecture.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"duck-demo/internal/compute"
	"duck-demo/internal/config"
	"duck-demo/internal/db/crypto"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	assetsvc "duck-demo/internal/service/asset"
	authsvc "duck-demo/internal/service/auth"
	"duck-demo/internal/service/catalog"
	svccompute "duck-demo/internal/service/compute"
	"duck-demo/internal/service/dashboard"
	exploresvc "duck-demo/internal/service/explore"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/ingestion"
	"duck-demo/internal/service/macro"
	svcmodel "duck-demo/internal/service/model"
	"duck-demo/internal/service/notebook"
	"duck-demo/internal/service/orchestration"
	"duck-demo/internal/service/pipeline"
	productsvc "duck-demo/internal/service/product"
	projectsvc "duck-demo/internal/service/project"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/resourceaccess"
	"duck-demo/internal/service/savedresource"
	"duck-demo/internal/service/security"
	"duck-demo/internal/service/semantic"
	"duck-demo/internal/service/storage"
	workspacesvc "duck-demo/internal/service/workspace"
)

// Deps holds the external dependencies that main() must provide.
// These are things the app package cannot (or should not) create itself:
// database handles, config, and the DuckDB connection.
type Deps struct {
	Cfg     *config.Config
	DuckDB  *sql.DB
	WriteDB *sql.DB
	ReadDB  *sql.DB
	Logger  *slog.Logger
}

// Services groups all service pointers that the API handler and router need.
type Services struct {
	Query               *query.QueryService
	Principal           *security.PrincipalService
	Group               *security.GroupService
	Grant               *security.GrantService
	RowFilter           *security.RowFilterService
	ColumnMask          *security.ColumnMaskService
	Audit               *governance.AuditService
	QueryHistory        *governance.QueryHistoryService
	Lineage             *governance.LineageService
	Search              *catalog.SearchService
	Tag                 *governance.TagService
	View                *catalog.ViewService
	Catalog             *catalog.CatalogService
	CatalogRegistration *catalog.CatalogRegistrationService
	Manifest            *query.ManifestService
	Ingestion           *ingestion.IngestionService
	StorageCredential   *storage.StorageCredentialService
	ExternalLocation    *storage.ExternalLocationService
	Volume              *storage.VolumeService
	ComputeEndpoint     *svccompute.ComputeEndpointService
	APIKey              *security.APIKeyService
	Auth                *authsvc.Service
	WebSessionAuth      *authsvc.SessionService
	Notebook            *notebook.Service
	NotebookFolders     *notebook.FolderService
	Workspace           *workspacesvc.Service
	Project             *projectsvc.Service
	Explore             *exploresvc.Service
	SessionManager      *notebook.SessionManager
	GitService          *notebook.GitService
	Pipeline            *pipeline.Service
	Product             *productsvc.Service
	Asset               *assetsvc.Service
	Backfill            *orchestration.BackfillService
	Model               *svcmodel.Service
	Macro               *macro.Service
	Semantic            *semantic.Service
	Dashboard           *dashboard.Service
	ResourceAccess      *resourceaccess.Service
	SavedResource       *savedresource.Service
}

// App holds the fully-wired application: engine, services, and the
// repositories needed for router setup (APIKeyRepo for auth middleware).
type App struct {
	Services      Services
	Engine        *engine.SecureEngine
	APIKeyRepo    *repository.APIKeyRepo
	PrincipalRepo *repository.PrincipalRepo
	Reconciler    *orchestration.Reconciler
}

// New wires all repositories, services, and engine from the provided deps.
// It also performs external-table view restoration.
//
// Construction order is designed so every dependency is available at the
// time each constructor is called, including feature-gated collaborators.
func New(ctx context.Context, deps Deps) (*App, error) {
	cfg := deps.Cfg

	// === 1. Crypto / encryption (needed by credential repos) ===
	encryptor, err := crypto.NewEncryptor(cfg.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("encryption key: %w", err)
	}

	// === 2. All repositories (write-pool) ===
	principalRepo := repository.NewPrincipalRepo(deps.WriteDB)
	groupRepo := repository.NewGroupRepo(deps.WriteDB)
	domainRepo := repository.NewDomainRepo(deps.WriteDB)
	teamRepo := repository.NewTeamRepo(deps.WriteDB)
	grantRepo := repository.NewGrantRepo(deps.WriteDB)
	rowFilterRepo := repository.NewRowFilterRepo(deps.WriteDB)
	columnMaskRepo := repository.NewColumnMaskRepo(deps.WriteDB)
	auditRepo := repository.NewAuditRepo(deps.WriteDB)
	lineageRepo := repository.NewLineageRepo(deps.WriteDB)
	colLineageRepo := repository.NewColumnLineageRepo(deps.WriteDB)
	tagRepo := repository.NewTagRepo(deps.WriteDB)
	viewRepo := repository.NewViewRepo(deps.WriteDB)
	tableStatsRepo := repository.NewTableStatisticsRepo(deps.WriteDB)
	extTableRepo := repository.NewExternalTableRepo(deps.WriteDB)
	externalLocRepo := repository.NewExternalLocationRepo(deps.WriteDB)
	volumeRepo := repository.NewVolumeRepo(deps.WriteDB)
	storageCredRepo := repository.NewStorageCredentialRepo(deps.WriteDB, encryptor)
	computeEndpointRepo := repository.NewComputeEndpointRepo(deps.WriteDB, encryptor)
	computeRoutingRepo := repository.NewComputeRoutingRepo(deps.WriteDB)
	catalogRegRepo := repository.NewCatalogRegistrationRepo(deps.WriteDB)
	dataProductRepo := repository.NewDataProductRepo(deps.WriteDB)
	workspaceRepo := repository.NewWorkspaceRepo(deps.WriteDB)
	queryJobRepo := repository.NewQueryJobRepo(deps.WriteDB)
	authIdentityRepo := repository.NewAuthIdentityRepo(deps.WriteDB)
	localCredentialRepo := repository.NewLocalCredentialRepo(deps.WriteDB)
	webSessionRepo := repository.NewWebSessionRepo(deps.WriteDB)
	resourceAccessRepo := repository.NewResourceAccessRepo(deps.WriteDB)
	savedResourceRepo := repository.NewSavedResourceRepo(deps.WriteDB)
	authRecoveryRepo := repository.NewAuthRecoveryRepo(deps.WriteDB)
	authLoginAttemptRepo := repository.NewAuthLoginAttemptRepo(deps.WriteDB)
	setupStateRepo := repository.NewSetupStateRepo(deps.WriteDB)
	authProviderRepo := repository.NewAuthProviderRepo(deps.WriteDB)
	_ = authIdentityRepo
	_ = authRecoveryRepo

	// === 3. Factories (multi-catalog) ===
	catalogRepoFactory := repository.NewCatalogRepoFactory(
		catalogRegRepo, deps.WriteDB, deps.DuckDB, extTableRepo,
		deps.Logger.With("component", "catalog-repo"),
	)
	introspectionFactory := repository.NewIntrospectionRepoFactory(catalogRegRepo)
	metastoreFactory := repository.NewMetastoreRepoFactory(catalogRegRepo)

	// === 4. Repositories (read-pool) ===
	introspectionRepo := repository.NewIntrospectionRepo(deps.ReadDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(deps.ReadDB)
	searchRepo := repository.NewSearchRepo(deps.ReadDB, deps.ReadDB)

	// === 5. Compute resolver (needs endpoint repo, principal repo, group repo) ===
	localExec := compute.NewLocalExecutor(deps.DuckDB)
	remoteCache := compute.NewRemoteCacheWithOptions(deps.DuckDB, compute.RemoteExecutorOptions{
		CursorModeEnabled: cfg.FeatureCursorMode,
		InternalGRPC:      cfg.FeatureInternalGRPC,
	})
	fullResolver := compute.NewResolver(
		localExec, computeEndpointRepo, principalRepo, groupRepo,
		remoteCache, deps.Logger.With("component", "compute-resolver"),
	)
	fullResolver.SetRoutingEnabled(cfg.FeatureRemoteRouting && cfg.FeatureInternalGRPC)
	fullResolver.SetCanaryUsers(cfg.RemoteCanaryUsers)

	// === 6. Authorization (needs all security repos + extTableRepo) ===
	authSvc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		extTableRepo,
	)
	authSvc.SetCatalogTableLookup(func(ctx context.Context, catalogName, schemaName, tableName string) (*domain.TableDetail, error) {
		repo, err := catalogRepoFactory.ForCatalog(ctx, catalogName)
		if err != nil {
			return nil, err
		}
		return repo.GetTable(ctx, schemaName, tableName)
	})
	authSvc.SetCatalogColumnLookup(func(ctx context.Context, catalogName, tableID string) ([]string, error) {
		repo, err := introspectionFactory.ForCatalog(ctx, catalogName)
		if err != nil {
			return nil, err
		}
		cols, _, err := repo.ListColumns(ctx, tableID, domain.PageRequest{MaxResults: 10000})
		if err != nil {
			return nil, err
		}
		names := make([]string, len(cols))
		for i, col := range cols {
			names[i] = col.Name
		}
		return names, nil
	})
	authSvc.SetDefaultCatalogTableLookup(func(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error) {
		reg, err := catalogRegRepo.GetDefault(ctx)
		if err != nil {
			return nil, err
		}
		repo, err := catalogRepoFactory.ForCatalog(ctx, reg.Name)
		if err != nil {
			return nil, err
		}
		return repo.GetTable(ctx, schemaName, tableName)
	})
	authSvc.SetCatalogSchemaLookup(func(ctx context.Context, catalogName, schemaName string) (*domain.SchemaDetail, error) {
		repo, err := catalogRepoFactory.ForCatalog(ctx, catalogName)
		if err != nil {
			return nil, err
		}
		return repo.GetSchema(ctx, schemaName)
	})
	authSvc.SetViewRepository(viewRepo)
	authSvc.SetCatalogViewLookup(func(ctx context.Context, catalogName, schemaName, viewName string) (*domain.ViewDetail, error) {
		return authSvcLookupViewInCatalog(ctx, catalogName, schemaName, viewName, catalogRepoFactory, viewRepo, deps.DuckDB)
	})
	authSvc.SetDefaultCatalogViewLookup(func(ctx context.Context, schemaName, viewName string) (*domain.ViewDetail, error) {
		reg, err := catalogRegRepo.GetDefault(ctx)
		if err != nil {
			return nil, err
		}
		return authSvcLookupViewInCatalog(ctx, reg.Name, schemaName, viewName, catalogRepoFactory, viewRepo, deps.DuckDB)
	})
	authSvc.SetDefaultCatalogSchemaLookup(func(ctx context.Context, schemaName string) (*domain.Schema, error) {
		reg, err := catalogRegRepo.GetDefault(ctx)
		if err != nil {
			return nil, err
		}
		repo, err := introspectionFactory.ForCatalog(ctx, reg.Name)
		if err != nil {
			return nil, err
		}
		return repo.GetSchemaByName(ctx, schemaName)
	})
	authSvc.SetDefaultCatalogTableByIDLookup(func(ctx context.Context, tableID string) (*domain.Table, error) {
		repo, err := introspectionFactory.ForDefault(ctx)
		if err != nil {
			return nil, err
		}
		return repo.GetTable(ctx, tableID)
	})

	// === Check for empty database and log bootstrap instructions ===
	_, total, _ := principalRepo.List(ctx, domain.PageRequest{MaxResults: 1})
	if total == 0 {
		if cfg.Auth.BootstrapAdmin != "" {
			deps.Logger.Info("no principals found — first JWT login matching AUTH_BOOTSTRAP_ADMIN will be provisioned as admin",
				"bootstrap_admin", cfg.Auth.BootstrapAdmin)
		} else {
			deps.Logger.Warn("no principals found and AUTH_BOOTSTRAP_ADMIN is not set",
				"hint", "set AUTH_BOOTSTRAP_ADMIN=<jwt-sub> or run: go run ./cmd/server admin promote --principal=<name> --create")
		}
	}

	// === 7. Engine (needs auth + resolver + infoSchema provider) ===
	// InformationSchemaProvider aggregates metadata across all active catalogs.
	infoSchema := engine.NewInformationSchemaProvider(catalogRepoFactory, catalogRegRepo)
	eng := engine.NewSecureEngine(deps.DuckDB, authSvc, fullResolver, infoSchema, deps.Logger.With("component", "engine"))

	// Restore external table VIEWs (best-effort)
	if err := restoreExternalTableViews(ctx, deps.DuckDB, extTableRepo, deps.Logger); err != nil {
		deps.Logger.Warn("restore external table views failed", "error", err)
	}

	// === 8. All services (all deps available at construction) ===
	querySvc := query.NewQueryService(eng, auditRepo, lineageRepo)
	querySvc.SetAsyncEnabled(cfg.FeatureAsyncQueue)
	catalogAdapter := query.NewCatalogAdapter(introspectionRepo)
	querySvc.SetColumnLineage(colLineageRepo, catalogAdapter)
	querySvc.SetJobRepository(queryJobRepo)
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	principalSvc.SetAuthIdentityRepository(authIdentityRepo)
	authService := authsvc.NewService(principalRepo, localCredentialRepo, authLoginAttemptRepo, setupStateRepo, authProviderRepo, auditRepo, cfg.Auth.JWTSecret)
	webSessionAuth := authsvc.NewSessionService(principalRepo, webSessionRepo, auditRepo, cfg.Auth.WebSessionIdleTTL, cfg.Auth.WebSessionAbsoluteTTL)
	resourceAccessSvc := resourceaccess.NewService(resourceAccessRepo)
	savedResourceSvc := savedresource.NewService(savedResourceRepo)
	groupSvc := security.NewGroupService(groupRepo, principalRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo, authSvc)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	queryHistorySvc := governance.NewQueryHistoryService(queryHistoryRepo)
	lineageSvc := governance.NewLineageService(lineageRepo, colLineageRepo, auditRepo)
	searchRepoFactory := repository.NewSearchRepoFactory(deps.ReadDB, catalogRegRepo)
	searchSvc := catalog.NewSearchService(searchRepo, searchRepoFactory)
	tagSvc := governance.NewTagService(tagRepo, auditRepo)
	viewSvc := catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)
	catalogSvc := catalog.NewCatalogService(catalogRepoFactory, authSvc, auditRepo, tagRepo, tableStatsRepo, externalLocRepo)
	storageCredSvc := storage.NewStorageCredentialService(storageCredRepo, authSvc, auditRepo)
	computeEndpointSvc := svccompute.NewComputeEndpointService(computeEndpointRepo, authSvc, auditRepo)
	computeEndpointSvc.SetRoutingRepository(computeRoutingRepo)
	computeEndpointSvc.SetPrincipalRepository(principalRepo)
	computeEndpointSvc.SetGroupRepository(groupRepo)
	volumeSvc := storage.NewVolumeService(volumeRepo, authSvc, auditRepo)

	secretMgr := engine.NewDuckDBSecretManager(deps.DuckDB)
	extLocationSvc := storage.NewExternalLocationService(
		externalLocRepo, storageCredRepo, authSvc, auditRepo, secretMgr,
		deps.Logger.With("component", "external-location"),
	)

	// === CatalogRegistrationService ===
	catalogRegSvc := catalog.NewCatalogRegistrationService(catalog.RegistrationServiceDeps{
		Repo:               catalogRegRepo,
		Attacher:           secretMgr,
		Audit:              auditRepo,
		ControlPlaneDBPath: cfg.MetaDBPath,
		Logger:             deps.Logger.With("component", "catalog-registration"),
		MetastoreFactory:   metastoreFactory,
		IntrospectionClose: introspectionFactory.Close,
		CatalogRepoEvict:   catalogRepoFactory.Evict,
	})

	// === Manifest and Ingestion services (always available, use factory-based metastore) ===

	manifestSvc := query.NewManifestService(
		metastoreFactory, authSvc, nil, introspectionFactory, auditRepo,
		storageCredRepo, externalLocRepo,
	)

	duckExec := engine.NewDuckDBExecAdapter(deps.DuckDB)
	ingestionSvc := ingestion.NewIngestionService(
		duckExec, metastoreFactory, authSvc, nil, auditRepo, "",
		storageCredRepo, externalLocRepo,
	)

	// === Restore secrets (best-effort) ===
	if err := extLocationSvc.RestoreSecrets(ctx); err != nil {
		deps.Logger.Warn("restore secrets failed", "error", err)
	}

	// === Notebook services ===
	notebookRepo := repository.NewNotebookRepo(deps.WriteDB)
	folderRepo := repository.NewFolderRepo(deps.WriteDB)
	folderShareRepo := repository.NewFolderShareRepo(deps.WriteDB)
	notebookShareRepo := repository.NewNotebookShareRepo(deps.WriteDB)
	notebookJobRepo := repository.NewNotebookJobRepo(deps.WriteDB)
	notebookSvc := notebook.New(notebookRepo, auditRepo)
	notebookSvc.SetWorkspaceRepository(workspaceRepo)
	notebookSvc.SetFolderRepository(folderRepo)
	notebookSvc.SetAuthorization(authSvc)
	notebookSvc.SetGrantRepository(grantRepo)
	notebookSvc.SetShareRepositories(folderShareRepo, notebookShareRepo)
	folderSvc := notebook.NewFolderService(folderRepo, auditRepo)
	folderSvc.SetWorkspaceRepository(workspaceRepo)
	folderSvc.SetAuthorization(authSvc)
	folderSvc.SetGrantRepository(grantRepo)
	folderSvc.SetShareRepository(folderShareRepo)
	sessionMgr := notebook.NewSessionManager(deps.DuckDB, eng, notebookRepo, notebookJobRepo, auditRepo)
	sessionMgr.SetWorkspaceRepository(workspaceRepo)
	sessionMgr.SetAuthorization(authSvc)
	sessionMgr.SetAccessRepositories(folderRepo, folderShareRepo, notebookShareRepo)
	gitRepoRepo := repository.NewGitRepoRepo(deps.WriteDB)
	gitSvc := notebook.NewGitService(gitRepoRepo, notebookRepo, auditRepo)
	gitSvc.SetFolderRepository(folderRepo)
	notebookModelLinkRepo := repository.NewNotebookModelLinkRepo(deps.WriteDB)

	// === Asset orchestration ===
	assetRepo := repository.NewDataAssetRepo(deps.WriteDB)
	assetDepRepo := repository.NewAssetDependencyRepo(deps.WriteDB)
	assetPartitionRepo := repository.NewAssetPartitionRepo(deps.WriteDB)
	assetRunRepo := repository.NewAssetRunRepo(deps.WriteDB)
	assetCheckRepo := repository.NewAssetCheckRepo(deps.WriteDB)
	buildRepo := repository.NewBuildRepo(deps.WriteDB)
	projectRepo := repository.NewProjectRepo(deps.WriteDB)
	environmentRepo := repository.NewEnvironmentRepo(deps.WriteDB)
	workspaceSvc := workspacesvc.NewService(workspaceRepo, folderRepo, projectRepo, environmentRepo, teamRepo, auditRepo)
	projectSvc := projectsvc.NewService(workspaceRepo, projectRepo, environmentRepo, buildRepo, teamRepo, dataProductRepo, auditRepo)
	orchEventRepo := repository.NewOrchestrationEventRepo(deps.WriteDB)
	backfillRepo := repository.NewBackfillRepo(deps.WriteDB)
	notebookProvider := pipeline.NewDBNotebookProvider(notebookRepo)
	pipelineRepo := repository.NewPipelineRepo(deps.WriteDB)
	pipelineRunRepo := repository.NewPipelineRunRepo(deps.WriteDB)
	pipelineSvc := pipeline.NewService(
		pipelineRepo,
		pipelineRunRepo,
		auditRepo,
		notebookProvider,
		eng,
		deps.DuckDB,
		deps.Logger.With("component", "pipeline"),
	)
	pipelineSvc.SetFolderRepository(folderRepo)
	notebookSvc.SetProjectRepositories(projectRepo, environmentRepo)
	folderSvc.SetProjectRepositories(projectRepo, environmentRepo)
	notebookSvc.SetContextInvalidator(notebook.NewOrchestrationEventEnqueuer(orchEventRepo))
	assetScheduler := orchestration.NewAssetScheduler(assetRepo, assetDepRepo, assetRunRepo)
	ioManager, err := newOrchestrationIOManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure orchestration io manager: %w", err)
	}
	triggerRouter := orchestration.NewTriggerRouter(orchEventRepo)
	backfillSvc := orchestration.NewBackfillService(backfillRepo, triggerRouter, auditRepo, authSvc)
	assetSvc := assetsvc.NewService(assetRepo, assetDepRepo, assetPartitionRepo, assetRunRepo, assetCheckRepo, backfillRepo, orchEventRepo, auditRepo, authSvc, dataProductRepo)
	productSvc := productsvc.NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, dataProductRepo, auditRepo)
	productSvc.SetBuildRepository(buildRepo)
	productSvc.SetProjectRepository(projectRepo)
	notebookProduct, err := productSvc.EnsureManagedRuntimeProduct(ctx, productsvc.ManagedRuntimeProductNotebooks)
	if err != nil {
		return nil, fmt.Errorf("ensure runtime notebook product: %w", err)
	}
	notebookOutputProduct, err := productSvc.EnsureManagedRuntimeProduct(ctx, productsvc.ManagedRuntimeProductNotebookOutputs)
	if err != nil {
		return nil, fmt.Errorf("ensure runtime notebook output product: %w", err)
	}
	modelProduct, err := productSvc.EnsureManagedRuntimeProduct(ctx, productsvc.ManagedRuntimeProductModels)
	if err != nil {
		return nil, fmt.Errorf("ensure runtime model product: %w", err)
	}
	if err := pipeline.SyncNotebooksToAssets(ctx, notebookRepo, assetRepo, assetDepRepo, notebookProduct.Product.ID); err != nil {
		return nil, fmt.Errorf("sync notebooks to assets: %w", err)
	}
	if err := pipeline.SyncNotebookOutputsToAssets(ctx, notebookRepo, notebookModelLinkRepo, assetRepo, assetDepRepo, notebookOutputProduct.Product.ID); err != nil {
		return nil, fmt.Errorf("sync notebook outputs to assets: %w", err)
	}

	// === Model ===
	modelRepo := repository.NewModelRepo(deps.WriteDB)
	if err := pipeline.SyncModelsToAssets(ctx, modelRepo, notebookModelLinkRepo, assetRepo, assetDepRepo, modelProduct.Product.ID); err != nil {
		return nil, fmt.Errorf("sync models to assets: %w", err)
	}
	modelRunRepo := repository.NewModelRunRepo(deps.WriteDB)
	modelTestRepo := repository.NewModelTestRepo(deps.WriteDB)
	modelTestResultRepo := repository.NewModelTestResultRepo(deps.WriteDB)
	// === Macro ===
	macroRepo := repository.NewMacroRepo(deps.WriteDB)
	macroSvc := macro.NewService(macroRepo, auditRepo)
	modelSvc := svcmodel.NewService(svcmodel.ServiceDeps{
		Models:        modelRepo,
		Runs:          modelRunRepo,
		Projects:      projectRepo,
		Environments:  environmentRepo,
		Builds:        buildRepo,
		Tests:         modelTestRepo,
		TestResults:   modelTestResultRepo,
		Audit:         auditRepo,
		Lineage:       lineageRepo,
		ColumnLineage: colLineageRepo,
		Macros:        macroRepo,
		Notebooks:     notebookProvider,
		NotebookLinks: notebookModelLinkRepo,
		Engine:        eng,
		DuckDB:        deps.DuckDB,
		Logger:        deps.Logger.With("component", "model"),
	})
	notebookSvc.SetPublishRepositories(modelRepo, notebookModelLinkRepo)
	gitSvc.SetPublishDependencies(modelSvc, notebookModelLinkRepo)

	// === Semantic ===
	semanticModelRepo := repository.NewSemanticModelRepo(deps.WriteDB)
	semanticMetricRepo := repository.NewSemanticMetricRepo(deps.WriteDB)
	semanticRelRepo := repository.NewSemanticRelationshipRepo(deps.WriteDB)
	semanticPreAggRepo := repository.NewSemanticPreAggregationRepo(deps.WriteDB)
	semanticSvc := semantic.NewService(semanticModelRepo, semanticMetricRepo, semanticRelRepo, semanticPreAggRepo)
	productSvc.SetSemanticModelRepository(semanticModelRepo)
	semanticProduct, err := productSvc.EnsureManagedRuntimeProduct(ctx, productsvc.ManagedRuntimeProductSemantic)
	if err != nil {
		return nil, fmt.Errorf("ensure runtime semantic product: %w", err)
	}
	semanticSvc.SetQueryExecutor(querySvc)
	semanticSvc.SetModelRepository(modelRepo)
	semanticSvc.SetDDLExecutor(duckExec)
	dashboardRepo := repository.NewDashboardRepo(deps.WriteDB)
	dashboardWidgetRepo := repository.NewDashboardWidgetRepo(deps.WriteDB)
	dashboardSvc := dashboard.NewService(dashboardRepo, dashboardWidgetRepo, notebookRepo, auditRepo, querySvc, semanticSvc)
	dashboardSvc.SetFolderRepository(folderRepo)
	exploreSvc := exploresvc.NewService(workspaceRepo, folderRepo, notebookRepo, dashboardRepo, pipelineRepo, projectRepo, modelRepo, macroRepo, semanticModelRepo)
	exploreSvc.SetAccessRepositories(folderShareRepo, notebookShareRepo)
	exploreSvc.SetAuthorization(authSvc)
	if err := pipeline.SyncSemanticResourcesToAssets(ctx, semanticModelRepo, semanticMetricRepo, semanticPreAggRepo, modelRepo, assetRepo, assetDepRepo, semanticProduct.Product.ID); err != nil {
		return nil, fmt.Errorf("sync semantic resources to assets: %w", err)
	}
	dashboardProduct, err := productSvc.EnsureManagedRuntimeProduct(ctx, productsvc.ManagedRuntimeProductDashboards)
	if err != nil {
		return nil, fmt.Errorf("ensure runtime dashboard product: %w", err)
	}
	if err := pipeline.SyncDashboardsToAssets(ctx, dashboardRepo, dashboardWidgetRepo, modelRepo, notebookRepo, notebookModelLinkRepo, semanticModelRepo, semanticMetricRepo, semanticPreAggRepo, assetRepo, assetDepRepo, dashboardProduct.Product.ID); err != nil {
		return nil, fmt.Errorf("sync dashboards to assets: %w", err)
	}
	assetExecutor := orchestration.NewAssetExecutor(
		assetRunRepo,
		orchestration.NewAssetRunStateMachine(),
		ioManager,
		orchestration.NewConcurrencyLimiter(16, 2),
		orchestration.NewMaterializingAssetStepper(
			assetRepo,
			assetDepRepo,
			modelRepo,
			modelSvc,
			notebookRepo,
			sessionMgr,
			semanticSvc,
		),
	)
	backfillRunner := orchestration.NewBackfillRunner(backfillRepo, assetDepRepo, assetRunRepo, assetScheduler, assetExecutor)
	folderSvc.SetContextInvalidation(notebookRepo, orchEventRepo)
	reconciler := orchestration.NewReconciler(
		orchEventRepo,
		assetRepo,
		assetRunRepo,
		assetScheduler,
		assetExecutor,
		backfillRunner,
		cfg.FeatureReconcilerShadow,
		notebook.NewOrchestrationEventHandler(sessionMgr),
	)

	// === API Key ===
	apiKeyRepo := repository.NewAPIKeyRepo(deps.ReadDB)
	apiKeySvc := security.NewAPIKeyService(apiKeyRepo, principalRepo, auditRepo)

	return &App{
		Services: Services{
			Query:               querySvc,
			Principal:           principalSvc,
			Group:               groupSvc,
			Grant:               grantSvc,
			RowFilter:           rowFilterSvc,
			ColumnMask:          columnMaskSvc,
			Audit:               auditSvc,
			QueryHistory:        queryHistorySvc,
			Lineage:             lineageSvc,
			Search:              searchSvc,
			Tag:                 tagSvc,
			View:                viewSvc,
			Catalog:             catalogSvc,
			CatalogRegistration: catalogRegSvc,
			Manifest:            manifestSvc,
			Ingestion:           ingestionSvc,
			StorageCredential:   storageCredSvc,
			ExternalLocation:    extLocationSvc,
			Volume:              volumeSvc,
			ComputeEndpoint:     computeEndpointSvc,
			APIKey:              apiKeySvc,
			Auth:                authService,
			WebSessionAuth:      webSessionAuth,
		Notebook:            notebookSvc,
		NotebookFolders:     folderSvc,
		Workspace:           workspaceSvc,
		Project:             projectSvc,
		Explore:             exploreSvc,
			SessionManager:      sessionMgr,
			GitService:          gitSvc,
			Pipeline:            pipelineSvc,
			Product:             productSvc,
			Asset:               assetSvc,
			Backfill:            backfillSvc,
			Model:               modelSvc,
			Macro:               macroSvc,
			Semantic:            semanticSvc,
			Dashboard:           dashboardSvc,
			ResourceAccess:      resourceAccessSvc,
			SavedResource:       savedResourceSvc,
		},
		Engine:        eng,
		APIKeyRepo:    apiKeyRepo,
		PrincipalRepo: principalRepo,
		Reconciler:    reconciler,
	}, nil
}

func authSvcLookupViewInCatalog(
	ctx context.Context,
	catalogName string,
	schemaName string,
	viewName string,
	catalogRepoFactory *repository.CatalogRepoFactory,
	viewRepo domain.ViewRepository,
	duckDB *sql.DB,
) (*domain.ViewDetail, error) {
	repo, err := catalogRepoFactory.ForCatalog(ctx, catalogName)
	if err != nil {
		return nil, err
	}
	schema, err := repo.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	view, err := viewRepo.GetByName(ctx, schema.SchemaID, viewName)
	if err == nil {
		return view, nil
	}

	var notFoundErr *domain.NotFoundError
	if !errors.As(err, &notFoundErr) {
		return nil, err
	}

	query := "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ? AND table_name = ? AND table_type = 'VIEW' LIMIT 1"
	var foundName string
	if scanErr := duckDB.QueryRowContext(ctx, query, catalogName, schemaName, viewName).Scan(&foundName); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil, domain.ErrNotFound("view %q not found in schema %q", viewName, schemaName)
		}
		return nil, fmt.Errorf("lookup catalog view %q.%q.%q: %w", catalogName, schemaName, viewName, scanErr)
	}

	return &domain.ViewDetail{SchemaID: schema.SchemaID, Name: foundName}, nil
}
