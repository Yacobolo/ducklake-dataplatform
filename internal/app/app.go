// Package app provides application-level wiring and dependency injection
// for the duck-demo application following hexagonal architecture.
package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"duck-demo/internal/compute"
	"duck-demo/internal/config"
	"duck-demo/internal/db/crypto"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/service/catalog"
	svccompute "duck-demo/internal/service/compute"
	"duck-demo/internal/service/governance"
	"duck-demo/internal/service/ingestion"
	"duck-demo/internal/service/notebook"
	"duck-demo/internal/service/pipeline"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/security"
	"duck-demo/internal/service/storage"
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
	Notebook            *notebook.Service
	SessionManager      *notebook.SessionManager
	GitService          *notebook.GitService
	Pipeline            *pipeline.Service
}

// App holds the fully-wired application: engine, services, and the
// repositories needed for router setup (APIKeyRepo for auth middleware).
type App struct {
	Services      Services
	Engine        *engine.SecureEngine
	APIKeyRepo    *repository.APIKeyRepo
	PrincipalRepo *repository.PrincipalRepo
	Scheduler     *pipeline.Scheduler
}

// New wires all repositories, services, and engine from the provided deps.
// It also performs external-table view restoration.
//
// Construction order is designed so every dependency is available at the
// time each constructor is called — no post-construction Set*() calls.
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
	grantRepo := repository.NewGrantRepo(deps.WriteDB)
	rowFilterRepo := repository.NewRowFilterRepo(deps.WriteDB)
	columnMaskRepo := repository.NewColumnMaskRepo(deps.WriteDB)
	auditRepo := repository.NewAuditRepo(deps.WriteDB)
	lineageRepo := repository.NewLineageRepo(deps.WriteDB)
	tagRepo := repository.NewTagRepo(deps.WriteDB)
	viewRepo := repository.NewViewRepo(deps.WriteDB)
	tableStatsRepo := repository.NewTableStatisticsRepo(deps.WriteDB)
	extTableRepo := repository.NewExternalTableRepo(deps.WriteDB)
	externalLocRepo := repository.NewExternalLocationRepo(deps.WriteDB)
	volumeRepo := repository.NewVolumeRepo(deps.WriteDB)
	storageCredRepo := repository.NewStorageCredentialRepo(deps.WriteDB, encryptor)
	computeEndpointRepo := repository.NewComputeEndpointRepo(deps.WriteDB, encryptor)
	catalogRegRepo := repository.NewCatalogRegistrationRepo(deps.WriteDB)

	// === 3. Factories (multi-catalog) ===
	catalogRepoFactory := repository.NewCatalogRepoFactory(
		deps.WriteDB, deps.DuckDB, extTableRepo,
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
	remoteCache := compute.NewRemoteCache(deps.DuckDB)
	fullResolver := compute.NewResolver(
		localExec, computeEndpointRepo, principalRepo, groupRepo,
		remoteCache, deps.Logger.With("component", "compute-resolver"),
	)

	// === 6. Authorization (needs all security repos + extTableRepo) ===
	authSvc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		extTableRepo,
	)

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
	principalSvc := security.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := security.NewGroupService(groupRepo, auditRepo)
	grantSvc := security.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := security.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := security.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := governance.NewAuditService(auditRepo)
	queryHistorySvc := governance.NewQueryHistoryService(queryHistoryRepo)
	lineageSvc := governance.NewLineageService(lineageRepo)
	searchRepoFactory := repository.NewSearchRepoFactory(deps.ReadDB, catalogRegRepo)
	searchSvc := catalog.NewSearchService(searchRepo, searchRepoFactory)
	tagSvc := governance.NewTagService(tagRepo, auditRepo)
	viewSvc := catalog.NewViewService(viewRepo, catalogRepoFactory, authSvc, auditRepo)
	catalogSvc := catalog.NewCatalogService(catalogRepoFactory, authSvc, auditRepo, tagRepo, tableStatsRepo, externalLocRepo)
	storageCredSvc := storage.NewStorageCredentialService(storageCredRepo, authSvc, auditRepo)
	computeEndpointSvc := svccompute.NewComputeEndpointService(computeEndpointRepo, authSvc, auditRepo)
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
		ControlPlaneDBPath: cfg.MetaDBPath,
		DuckDB:             deps.DuckDB,
		Logger:             deps.Logger.With("component", "catalog-registration"),
		MetastoreFactory:   metastoreFactory,
		IntrospectionClose: introspectionFactory.Close,
		CatalogRepoEvict:   catalogRepoFactory.Evict,
	})

	// === Manifest and Ingestion services (always available, use factory-based metastore) ===
	var legacyGetPresigner query.FilePresigner
	var legacyUploadPresigner query.FileUploadPresigner
	bucket := "duck-demo"

	if cfg.HasS3Config() {
		s3p, err := query.NewS3Presigner(cfg)
		if err != nil {
			deps.Logger.Warn("could not create legacy S3 presigner", "error", err)
		} else {
			legacyGetPresigner = s3p
			legacyUploadPresigner = s3p
			bucket = s3p.Bucket()
			deps.Logger.Info("legacy S3 presigner configured")
		}
	}

	manifestSvc := query.NewManifestService(
		metastoreFactory, authSvc, legacyGetPresigner, introspectionRepo, auditRepo,
		storageCredRepo, externalLocRepo,
	)

	duckExec := engine.NewDuckDBExecAdapter(deps.DuckDB)
	ingestionSvc := ingestion.NewIngestionService(
		duckExec, metastoreFactory, authSvc, legacyUploadPresigner, auditRepo, bucket,
		storageCredRepo, externalLocRepo,
	)

	// === Restore secrets (best-effort) ===
	if err := extLocationSvc.RestoreSecrets(ctx); err != nil {
		deps.Logger.Warn("restore secrets failed", "error", err)
	}

	// === Notebook services ===
	notebookRepo := repository.NewNotebookRepo(deps.WriteDB)
	notebookJobRepo := repository.NewNotebookJobRepo(deps.WriteDB)
	notebookSvc := notebook.New(notebookRepo, auditRepo)
	sessionMgr := notebook.NewSessionManager(deps.DuckDB, eng, notebookRepo, notebookJobRepo, auditRepo)
	gitRepoRepo := repository.NewGitRepoRepo(deps.WriteDB)
	gitSvc := notebook.NewGitService(gitRepoRepo, auditRepo)

	// === Pipeline ===
	pipelineRepo := repository.NewPipelineRepo(deps.WriteDB)
	pipelineRunRepo := repository.NewPipelineRunRepo(deps.WriteDB)
	notebookProvider := pipeline.NewDBNotebookProvider(notebookRepo)
	pipelineSvc := pipeline.NewService(
		pipelineRepo, pipelineRunRepo, auditRepo,
		notebookProvider, eng, deps.DuckDB,
		deps.Logger.With("component", "pipeline"),
	)
	pipelineScheduler := pipeline.NewScheduler(pipelineSvc, pipelineRepo,
		deps.Logger.With("component", "pipeline-scheduler"))
	pipelineSvc.SetScheduleReloader(pipelineScheduler)

	// === API Key ===
	apiKeyRepo := repository.NewAPIKeyRepo(deps.ReadDB)
	apiKeySvc := security.NewAPIKeyService(apiKeyRepo, auditRepo)

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
			Notebook:            notebookSvc,
			SessionManager:      sessionMgr,
			GitService:          gitSvc,
			Pipeline:            pipelineSvc,
		},
		Engine:        eng,
		APIKeyRepo:    apiKeyRepo,
		PrincipalRepo: principalRepo,
		Scheduler:     pipelineScheduler,
	}, nil
}
