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
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/engine"
	"duck-demo/internal/service"
)

// Deps holds the external dependencies that main() must provide.
// These are things the app package cannot (or should not) create itself:
// database handles, config, and the DuckDB connection.
type Deps struct {
	Cfg             *config.Config
	DuckDB          *sql.DB
	WriteDB         *sql.DB
	ReadDB          *sql.DB
	CatalogAttached bool // true when legacy S3 DuckLake setup succeeded
	Logger          *slog.Logger
}

// Services groups all service pointers that the API handler and router need.
// Conditional services (Manifest, Ingestion) are nil when S3 is not configured.
type Services struct {
	Query             *service.QueryService
	Principal         *service.PrincipalService
	Group             *service.GroupService
	Grant             *service.GrantService
	RowFilter         *service.RowFilterService
	ColumnMask        *service.ColumnMaskService
	Audit             *service.AuditService
	QueryHistory      *service.QueryHistoryService
	Lineage           *service.LineageService
	Search            *service.SearchService
	Tag               *service.TagService
	View              *service.ViewService
	Catalog           *service.CatalogService
	Manifest          *service.ManifestService  // nil when S3 not configured
	Ingestion         *service.IngestionService // nil when S3 not configured
	StorageCredential *service.StorageCredentialService
	ExternalLocation  *service.ExternalLocationService
	Volume            *service.VolumeService
	ComputeEndpoint   *service.ComputeEndpointService
}

// App holds the fully-wired application: engine, services, and the
// repositories needed for router setup (APIKeyRepo for auth middleware).
type App struct {
	Services   Services
	Engine     *engine.SecureEngine
	APIKeyRepo *repository.APIKeyRepo
}

// New wires all repositories, services, and engine from the provided deps.
// It also runs conditional seeding and external-table view restoration.
func New(ctx context.Context, deps Deps) (*App, error) {
	cfg := deps.Cfg

	// === Repositories (write-pool) ===
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
	catalogRepo := repository.NewCatalogRepo(deps.WriteDB, deps.DuckDB, deps.Logger.With("component", "catalog-repo"))

	// === Repositories (read-pool) ===
	introspectionRepo := repository.NewIntrospectionRepo(deps.ReadDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(deps.ReadDB)
	searchRepo := repository.NewSearchRepo(deps.ReadDB)

	// === Authorization ===
	authSvc := service.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
	)

	// === Seed demo data (only when catalog attached) ===
	if deps.CatalogAttached {
		q := dbstore.New(deps.WriteDB)
		if err := seedCatalog(ctx, authSvc, q); err != nil {
			deps.Logger.Warn("seed catalog failed", "error", err)
		}
	}

	// === Engine (resolver wired below after compute repo is created) ===
	localExec := compute.NewLocalExecutor(deps.DuckDB)
	placeholderResolver := compute.NewDefaultResolver(localExec)
	eng := engine.NewSecureEngine(deps.DuckDB, authSvc, placeholderResolver, deps.Logger.With("component", "engine"))
	eng.SetInformationSchemaProvider(engine.NewInformationSchemaProvider(catalogRepo))

	// === Core services ===
	querySvc := service.NewQueryService(eng, auditRepo, lineageRepo)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	auditSvc := service.NewAuditService(auditRepo)
	queryHistorySvc := service.NewQueryHistoryService(queryHistoryRepo)
	lineageSvc := service.NewLineageService(lineageRepo)
	searchSvc := service.NewSearchService(searchRepo)
	tagSvc := service.NewTagService(tagRepo, auditRepo)
	viewSvc := service.NewViewService(viewRepo, catalogRepo, authSvc, auditRepo)

	// === Conditional S3 services ===
	var manifestSvc *service.ManifestService
	var ingestionSvc *service.IngestionService
	if cfg.HasS3Config() && deps.CatalogAttached {
		presigner, err := service.NewS3Presigner(cfg)
		if err != nil {
			deps.Logger.Warn("could not create S3 presigner", "error", err)
		} else {
			metastoreRepo := repository.NewMetastoreRepo(deps.ReadDB)
			manifestSvc = service.NewManifestService(metastoreRepo, authSvc, presigner, introspectionRepo, auditRepo)
			deps.Logger.Info("manifest service enabled (duck_access extension support)")

			bucket := "duck-demo"
			if cfg.S3Bucket != nil {
				bucket = *cfg.S3Bucket
			}
			ingestionSvc = service.NewIngestionService(
				deps.DuckDB, metastoreRepo, authSvc, presigner, auditRepo, "lake", bucket,
			)
			deps.Logger.Info("ingestion service enabled")
		}
	}

	// === External table repo + wiring ===
	extTableRepo := repository.NewExternalTableRepo(deps.WriteDB)
	catalogRepo.SetExternalTableRepo(extTableRepo)
	authSvc.SetExternalTableRepo(extTableRepo)

	// Restore external table VIEWs (best-effort)
	if err := restoreExternalTableViews(ctx, deps.DuckDB, extTableRepo, deps.Logger); err != nil {
		deps.Logger.Warn("restore external table views failed", "error", err)
	}

	// === Catalog service ===
	catalogSvc := service.NewCatalogService(catalogRepo, authSvc, auditRepo, tagRepo, tableStatsRepo)

	// === Crypto + credential/location layer ===
	encryptor, err := crypto.NewEncryptor(cfg.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("encryption key: %w", err)
	}
	storageCredRepo := repository.NewStorageCredentialRepo(deps.WriteDB, encryptor)
	computeEndpointRepo := repository.NewComputeEndpointRepo(deps.WriteDB, encryptor)

	// Wire the full resolver now that compute repo is available
	remoteCache := compute.NewRemoteCache(deps.DuckDB)
	fullResolver := compute.NewResolver(
		localExec, computeEndpointRepo, principalRepo, groupRepo,
		remoteCache, deps.Logger.With("component", "compute-resolver"),
	)
	eng.SetResolver(fullResolver)
	externalLocRepo := repository.NewExternalLocationRepo(deps.WriteDB)

	storageCredSvc := service.NewStorageCredentialService(storageCredRepo, authSvc, auditRepo)
	computeEndpointSvc := service.NewComputeEndpointService(computeEndpointRepo, authSvc, auditRepo)
	secretMgr := engine.NewDuckDBSecretManager(deps.DuckDB)
	extLocationSvc := service.NewExternalLocationService(
		externalLocRepo, storageCredRepo, authSvc, auditRepo, secretMgr, secretMgr, cfg.MetaDBPath,
		deps.Logger.With("component", "external-location"),
	)

	// === Volume ===
	volumeRepo := repository.NewVolumeRepo(deps.WriteDB)
	volumeSvc := service.NewVolumeService(volumeRepo, authSvc, auditRepo)

	// === Post-construction wiring ===
	catalogSvc.SetExternalLocationRepo(externalLocRepo)
	if manifestSvc != nil {
		manifestSvc.SetCredentialRepos(storageCredRepo, externalLocRepo)
	}
	if ingestionSvc != nil {
		ingestionSvc.SetCredentialRepos(storageCredRepo, externalLocRepo)
	}
	if deps.CatalogAttached {
		extLocationSvc.SetCatalogAttached(true)
	}
	if err := extLocationSvc.RestoreSecrets(ctx); err != nil {
		deps.Logger.Warn("restore secrets failed", "error", err)
	}

	// === APIKeyRepo (needed by router for auth middleware) ===
	apiKeyRepo := repository.NewAPIKeyRepo(deps.ReadDB)

	return &App{
		Services: Services{
			Query:             querySvc,
			Principal:         principalSvc,
			Group:             groupSvc,
			Grant:             grantSvc,
			RowFilter:         rowFilterSvc,
			ColumnMask:        columnMaskSvc,
			Audit:             auditSvc,
			QueryHistory:      queryHistorySvc,
			Lineage:           lineageSvc,
			Search:            searchSvc,
			Tag:               tagSvc,
			View:              viewSvc,
			Catalog:           catalogSvc,
			Manifest:          manifestSvc,
			Ingestion:         ingestionSvc,
			StorageCredential: storageCredSvc,
			ExternalLocation:  extLocationSvc,
			Volume:            volumeSvc,
			ComputeEndpoint:   computeEndpointSvc,
		},
		Engine:     eng,
		APIKeyRepo: apiKeyRepo,
	}, nil
}
