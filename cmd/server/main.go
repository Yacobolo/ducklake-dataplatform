package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	_ "github.com/mattn/go-sqlite3"

	"duck-demo/internal/api"
	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/middleware"
	"duck-demo/internal/service"
)

// seedCatalog populates the metastore with demo principals, groups, grants,
// row filters, and column masks. Idempotent — checks if data already exists.
func seedCatalog(ctx context.Context, cat *service.AuthorizationService, q *dbstore.Queries) error {

	// Check if already seeded
	principals, _ := q.ListPrincipals(ctx)
	if len(principals) > 0 {
		return nil // already seeded
	}

	// --- Principals ---
	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin_user", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		return fmt.Errorf("create admin_user: %w", err)
	}

	analyst1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create analyst1: %w", err)
	}

	researcher1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "researcher1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create researcher1: %w", err)
	}

	_, err = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access_user", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create no_access_user: %w", err)
	}

	// --- Groups ---
	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "admins",
		Description: sql.NullString{String: "Administrators with full access", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create admins group: %w", err)
	}

	firstClassGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "first_class_analysts",
		Description: sql.NullString{String: "Analysts restricted to first-class passengers", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create first_class_analysts group: %w", err)
	}

	survivorGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "survivor_researchers",
		Description: sql.NullString{String: "Researchers restricted to survivors", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create survivor_researchers group: %w", err)
	}

	// --- Group memberships ---
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID,
	}); err != nil {
		return fmt.Errorf("add admin to admins: %w", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: firstClassGroup.ID, MemberType: "user", MemberID: analyst1.ID,
	}); err != nil {
		return fmt.Errorf("add analyst1 to first_class_analysts: %w", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: survivorGroup.ID, MemberType: "user", MemberID: researcher1.ID,
	}); err != nil {
		return fmt.Errorf("add researcher1 to survivor_researchers: %w", err)
	}

	// --- Lookup DuckLake IDs ---
	schemaID, err := cat.LookupSchemaID(ctx, "main")
	if err != nil {
		return fmt.Errorf("lookup schema: %w", err)
	}

	titanicID, _, err := cat.LookupTableID(ctx, "titanic")
	if err != nil {
		return fmt.Errorf("lookup titanic: %w", err)
	}

	// --- Privilege grants ---
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableCatalog, SecurableID: domain.CatalogID,
		Privilege: domain.PrivAllPrivileges,
	})
	if err != nil {
		return fmt.Errorf("grant admins ALL_PRIVILEGES: %w", err)
	}

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: firstClassGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableSchema, SecurableID: schemaID,
		Privilege: domain.PrivUsage,
	})
	if err != nil {
		return fmt.Errorf("grant first_class USAGE: %w", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: firstClassGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableTable, SecurableID: titanicID,
		Privilege: domain.PrivSelect,
	})
	if err != nil {
		return fmt.Errorf("grant first_class SELECT: %w", err)
	}

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableSchema, SecurableID: schemaID,
		Privilege: domain.PrivUsage,
	})
	if err != nil {
		return fmt.Errorf("grant survivor USAGE: %w", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableTable, SecurableID: titanicID,
		Privilege: domain.PrivSelect,
	})
	if err != nil {
		return fmt.Errorf("grant survivor SELECT: %w", err)
	}

	// --- Row filters ---
	firstClassFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:     titanicID,
		FilterSql:   `"Pclass" = 1`,
		Description: sql.NullString{String: "Only first-class passengers", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create first-class row filter: %w", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: firstClassFilter.ID, PrincipalID: firstClassGroup.ID, PrincipalType: "group",
	}); err != nil {
		return fmt.Errorf("bind first-class row filter: %w", err)
	}

	// --- Column masks ---
	nameMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID:        titanicID,
		ColumnName:     "Name",
		MaskExpression: `'***'`,
		Description:    sql.NullString{String: "Hide passenger names", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create Name column mask: %w", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: firstClassGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		return fmt.Errorf("bind Name mask for analysts: %w", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: survivorGroup.ID,
		PrincipalType: "group", SeeOriginal: 1,
	}); err != nil {
		return fmt.Errorf("bind Name mask for researchers: %w", err)
	}

	fmt.Println("Catalog seeded with demo principals, groups, grants, row filters, and column masks.")
	return nil
}

func main() {
	ctx := context.Background()

	// Load .env file (if present)
	if err := config.LoadDotEnv(".env"); err != nil {
		log.Printf("warning: could not load .env: %v", err)
	}

	cfg, cfgErr := config.LoadFromEnv()

	// Determine metastore path
	metaDBPath := "ducklake_meta.sqlite"
	if cfgErr == nil {
		metaDBPath = cfg.MetaDBPath
	}

	// Open DuckDB
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer duckDB.Close()

	// Try DuckLake setup
	if cfgErr == nil {
		fmt.Println("Setting up DuckLake with Hetzner S3...")
		if err := engine.SetupDuckLake(ctx, duckDB, cfg); err != nil {
			log.Printf("DuckLake setup failed, falling back to local parquet: %v", err)
			if _, err := duckDB.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
				log.Fatalf("failed to create table: %v", err)
			}
		} else {
			fmt.Println("DuckLake ready (data on Hetzner S3, metadata in SQLite)")
		}
	} else {
		fmt.Println("No S3 config found, using local parquet file...")
		if _, err := duckDB.Exec("CREATE TABLE titanic AS SELECT * FROM 'titanic.parquet'"); err != nil {
			log.Fatalf("failed to create table: %v", err)
		}
	}

	// Open SQLite metastore with hardened connection settings.
	// writeDB: single-connection pool for serialized writes (WAL + txlock=immediate).
	// readDB:  4-connection pool for concurrent reads (WAL, no txlock).
	writeDB, readDB, err := internaldb.OpenSQLitePair(metaDBPath, 4)
	if err != nil {
		log.Fatalf("failed to open metastore: %v", err)
	}
	defer writeDB.Close()
	defer readDB.Close()

	// Run migrations on the write pool (DDL requires write access)
	fmt.Println("Running catalog migrations...")
	if err := internaldb.RunMigrations(writeDB); err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	// Create repositories — write-pool for repos that INSERT/UPDATE/DELETE,
	// read-pool for repos that only SELECT.
	principalRepo := repository.NewPrincipalRepo(writeDB)
	groupRepo := repository.NewGroupRepo(writeDB)
	grantRepo := repository.NewGrantRepo(writeDB)
	rowFilterRepo := repository.NewRowFilterRepo(writeDB)
	columnMaskRepo := repository.NewColumnMaskRepo(writeDB)
	auditRepo := repository.NewAuditRepo(writeDB)
	lineageRepo := repository.NewLineageRepo(writeDB)
	tagRepo := repository.NewTagRepo(writeDB)
	viewRepo := repository.NewViewRepo(writeDB)
	tableStatsRepo := repository.NewTableStatisticsRepo(writeDB)
	catalogRepo := repository.NewCatalogRepo(writeDB, duckDB)

	introspectionRepo := repository.NewIntrospectionRepo(readDB)
	queryHistoryRepo := repository.NewQueryHistoryRepo(readDB)
	searchRepo := repository.NewSearchRepo(readDB)

	// Create authorization service
	cat := service.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
	)

	// Seed demo data (writes, so use writeDB)
	q := dbstore.New(writeDB)
	if err := seedCatalog(ctx, cat, q); err != nil {
		log.Fatalf("seed catalog: %v", err)
	}

	// Create secure engine
	eng := engine.NewSecureEngine(duckDB, cat)
	eng.SetInformationSchemaProvider(engine.NewInformationSchemaProvider(catalogRepo))

	// Create services
	querySvc := service.NewQueryService(eng, auditRepo, lineageRepo)
	principalSvc := service.NewPrincipalService(principalRepo, auditRepo)
	groupSvc := service.NewGroupService(groupRepo, auditRepo)
	grantSvc := service.NewGrantService(grantRepo, auditRepo)
	rowFilterSvc := service.NewRowFilterService(rowFilterRepo, auditRepo)
	columnMaskSvc := service.NewColumnMaskService(columnMaskRepo, auditRepo)
	introspectionSvc := service.NewIntrospectionService(introspectionRepo)
	auditSvc := service.NewAuditService(auditRepo)
	queryHistorySvc := service.NewQueryHistoryService(queryHistoryRepo)
	lineageSvc := service.NewLineageService(lineageRepo)
	searchSvc := service.NewSearchService(searchRepo)
	tagSvc := service.NewTagService(tagRepo, auditRepo)
	viewSvc := service.NewViewService(viewRepo, catalogRepo, cat, auditRepo)

	// Create manifest and ingestion services for duck_access extension support.
	// Only available when S3 credentials are configured (DuckLake mode).
	var manifestSvc *service.ManifestService
	var ingestionSvc *service.IngestionService
	if cfgErr == nil {
		presigner, err := service.NewS3Presigner(cfg)
		if err != nil {
			log.Printf("warning: could not create S3 presigner: %v", err)
		} else {
			manifestSvc = service.NewManifestService(readDB, cat, presigner, introspectionRepo, auditRepo)
			log.Println("Manifest service enabled (duck_access extension support)")

			ingestionSvc = service.NewIngestionService(
				duckDB, readDB, cat, presigner, auditRepo, "lake", cfg.S3Bucket,
			)
			log.Println("Ingestion service enabled")
		}
	}

	// Create catalog service for UC-compatible catalog management
	catalogSvc := service.NewCatalogService(catalogRepo, cat, auditRepo, tagRepo, tableStatsRepo)

	// Create API handler
	handler := api.NewHandler(
		querySvc, principalSvc, groupSvc, grantSvc,
		rowFilterSvc, columnMaskSvc, introspectionSvc, auditSvc,
		manifestSvc, catalogSvc,
		queryHistorySvc, lineageSvc, searchSvc, tagSvc, viewSvc,
		ingestionSvc,
	)

	// Create strict handler wrapper
	strictHandler := api.NewStrictHandler(handler, nil)

	// Setup Chi router
	r := chi.NewRouter()
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)

	// Determine JWT secret and listen address
	jwtSecret := []byte("dev-secret-change-in-production")
	listenAddr := ":8080"
	if cfgErr == nil {
		jwtSecret = []byte(cfg.JWTSecret)
		listenAddr = cfg.ListenAddr
	}

	// Public endpoints — no auth required
	r.Get("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
		swagger, err := api.GetSwagger()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(swagger)
	})

	r.Get("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<!DOCTYPE html>
<html>
<head>
    <title>DuckDB Data Platform API</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.44.16/dist/style.min.css" />
</head>
<body>
    <script id="api-reference" data-url="/openapi.json"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.44.16/dist/browser/standalone.min.js"></script>
</body>
</html>`)
	})

	// Authenticated API routes under /v1 prefix
	apiKeyRepo := repository.NewAPIKeyRepo(readDB)
	r.Route("/v1", func(r chi.Router) {
		r.Use(middleware.AuthMiddleware(jwtSecret, apiKeyRepo))
		api.HandlerFromMux(strictHandler, r)
	})

	// Start server
	log.Printf("HTTP API listening on %s", listenAddr)
	log.Printf("Try: curl -H 'Authorization: Bearer <jwt>' http://localhost%s/v1/principals", listenAddr)
	if err := http.ListenAndServe(listenAddr, r); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
