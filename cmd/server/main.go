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
	"duck-demo/internal/app"
	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/engine"
	"duck-demo/internal/middleware"
)

func main() {
	ctx := context.Background()

	// Load .env file (if present)
	if err := config.LoadDotEnv(".env"); err != nil {
		log.Printf("warning: could not load .env: %v", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Open DuckDB (in-memory)
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer duckDB.Close()

	// Install DuckLake extensions (no credentials needed)
	if err := engine.InstallExtensions(ctx, duckDB); err != nil {
		log.Fatalf("install extensions: %v", err)
	}
	log.Println("DuckDB extensions installed (ducklake, sqlite, httpfs)")

	// If legacy S3 env vars are present, set up DuckLake for backward compat
	catalogAttached := false
	if cfg.HasS3Config() {
		log.Println("Legacy S3 config detected, setting up DuckLake...")
		if err := engine.CreateS3Secret(ctx, duckDB, "hetzner_s3",
			*cfg.S3KeyID, *cfg.S3Secret, *cfg.S3Endpoint, *cfg.S3Region, "path"); err != nil {
			log.Printf("warning: S3 secret creation failed: %v", err)
		} else {
			bucket := "duck-demo"
			if cfg.S3Bucket != nil {
				bucket = *cfg.S3Bucket
			}
			dataPath := fmt.Sprintf("s3://%s/lake_data/", bucket)
			if err := engine.AttachDuckLake(ctx, duckDB, cfg.MetaDBPath, dataPath); err != nil {
				log.Printf("warning: DuckLake attach failed: %v", err)
			} else {
				catalogAttached = true
				log.Println("DuckLake ready (legacy S3 mode)")
			}
		}
	} else {
		log.Println("No S3 config — running in local mode. Use External Locations API to add storage.")
	}

	// Open SQLite metastore with hardened connection settings.
	// writeDB: single-connection pool for serialized writes (WAL + txlock=immediate).
	// readDB:  4-connection pool for concurrent reads (WAL, no txlock).
	writeDB, readDB, err := internaldb.OpenSQLitePair(cfg.MetaDBPath, 4)
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

	// Wire application dependencies
	application, err := app.New(ctx, app.Deps{
		Cfg:             cfg,
		DuckDB:          duckDB,
		WriteDB:         writeDB,
		ReadDB:          readDB,
		CatalogAttached: catalogAttached,
	})
	if err != nil {
		log.Fatalf("app init: %v", err)
	}

	// Create API handler
	svc := application.Services
	handler := api.NewHandler(
		svc.Query, svc.Principal, svc.Group, svc.Grant,
		svc.RowFilter, svc.ColumnMask, svc.Audit,
		svc.Manifest, svc.Catalog,
		svc.QueryHistory, svc.Lineage, svc.Search, svc.Tag, svc.View,
		svc.Ingestion,
		svc.StorageCredential, svc.ExternalLocation,
		svc.Volume,
	)

	// Create strict handler wrapper
	strictHandler := api.NewStrictHandler(handler, nil)

	// Setup Chi router
	r := chi.NewRouter()
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)

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
	r.Route("/v1", func(r chi.Router) {
		r.Use(middleware.AuthMiddleware([]byte(cfg.JWTSecret), application.APIKeyRepo))
		api.HandlerFromMux(strictHandler, r)
	})

	// Start server
	log.Printf("HTTP API listening on %s", cfg.ListenAddr)
	log.Printf("Try: curl -H 'Authorization: Bearer <jwt>' http://localhost%s/v1/principals", cfg.ListenAddr)
	if err := http.ListenAndServe(cfg.ListenAddr, r); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
