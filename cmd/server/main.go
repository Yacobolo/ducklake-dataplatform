package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"

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
		slog.Warn("could not load .env", "error", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	// Create structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: cfg.SlogLevel(),
	}))
	slog.SetDefault(logger)

	// Replay config warnings (generated before logger existed)
	for _, w := range cfg.Warnings {
		logger.Warn(w)
	}

	// Open DuckDB (in-memory)
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		logger.Error("failed to open duckdb", "error", err)
		os.Exit(1)
	}
	defer duckDB.Close()

	// Install DuckLake extensions (no credentials needed)
	if err := engine.InstallExtensions(ctx, duckDB); err != nil {
		logger.Error("install extensions", "error", err)
		os.Exit(1)
	}
	logger.Info("DuckDB extensions installed", "extensions", "ducklake, sqlite, httpfs")

	// If legacy S3 env vars are present, set up DuckLake for backward compat
	catalogAttached := false
	if cfg.HasS3Config() {
		logger.Info("legacy S3 config detected, setting up DuckLake")
		if err := engine.CreateS3Secret(ctx, duckDB, "hetzner_s3",
			*cfg.S3KeyID, *cfg.S3Secret, *cfg.S3Endpoint, *cfg.S3Region, "path"); err != nil {
			logger.Warn("S3 secret creation failed", "error", err)
		} else {
			bucket := "duck-demo"
			if cfg.S3Bucket != nil {
				bucket = *cfg.S3Bucket
			}
			dataPath := fmt.Sprintf("s3://%s/lake_data/", bucket)
			if err := engine.AttachDuckLake(ctx, duckDB, cfg.MetaDBPath, dataPath); err != nil {
				logger.Warn("DuckLake attach failed", "error", err)
			} else {
				catalogAttached = true
				logger.Info("DuckLake ready", "mode", "legacy S3")
			}
		}
	} else {
		logger.Info("no S3 config — running in local mode, use External Locations API to add storage")
	}

	// Open SQLite metastore with hardened connection settings.
	// writeDB: single-connection pool for serialized writes (WAL + txlock=immediate).
	// readDB:  4-connection pool for concurrent reads (WAL, no txlock).
	writeDB, readDB, err := internaldb.OpenSQLitePair(cfg.MetaDBPath, 4)
	if err != nil {
		logger.Error("failed to open metastore", "error", err)
		os.Exit(1)
	}
	defer writeDB.Close()
	defer readDB.Close()

	// Run migrations on the write pool (DDL requires write access)
	logger.Info("running catalog migrations")
	if err := internaldb.RunMigrations(writeDB); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}

	// Wire application dependencies
	application, err := app.New(ctx, app.Deps{
		Cfg:             cfg,
		DuckDB:          duckDB,
		WriteDB:         writeDB,
		ReadDB:          readDB,
		CatalogAttached: catalogAttached,
		Logger:          logger,
	})
	if err != nil {
		logger.Error("app init", "error", err)
		os.Exit(1)
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
	logger.Info("HTTP API listening", "addr", cfg.ListenAddr)
	logger.Info("try", "curl", fmt.Sprintf("curl -H 'Authorization: Bearer <jwt>' http://localhost%s/v1/principals", cfg.ListenAddr))
	if err := http.ListenAndServe(cfg.ListenAddr, r); err != nil {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
