// Package main is the entry point for the data platform server.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

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
	// Handle admin subcommands before starting the server.
	if len(os.Args) >= 2 && os.Args[1] == "admin" {
		if err := runAdmin(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	// Load .env file (if present) — no logger yet, so use stderr directly.
	if err := config.LoadDotEnv(".env"); err != nil {
		fmt.Fprintf(os.Stderr, "warn: could not load .env: %v\n", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// Create structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: cfg.SlogLevel(),
	}))
	slog.SetDefault(logger)

	// Replay config warnings (generated before logger existed)
	for _, w := range cfg.Warnings {
		logger.Warn("config warning", "detail", w)
	}

	// Open DuckDB (in-memory)
	duckDB, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer duckDB.Close() //nolint:errcheck

	// Install DuckLake extensions (no credentials needed)
	if err := engine.InstallExtensions(ctx, duckDB); err != nil {
		return fmt.Errorf("install extensions: %w", err)
	}
	logger.Info("DuckDB extensions installed", "extensions", "ducklake, sqlite, httpfs")

	// Install postgres extension if using PostgreSQL catalog
	if cfg.CatalogDBType == "postgres" {
		if err := engine.InstallPostgresExtension(ctx, duckDB); err != nil {
			return fmt.Errorf("install postgres extension: %w", err)
		}
		logger.Info("PostgreSQL extension installed")
	}

	// If legacy S3 env vars are present, set up DuckLake for backward compat
	catalogAttached := false
	switch {
	case cfg.HasS3Config():
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
	case cfg.CatalogDBType == "postgres" && cfg.CatalogDSN != "":
		// PostgreSQL-based DuckLake catalog
		bucket := "duck-demo"
		if cfg.S3Bucket != nil {
			bucket = *cfg.S3Bucket
		}
		dataPath := fmt.Sprintf("s3://%s/lake_data/", bucket)
		if err := engine.AttachDuckLakePostgres(ctx, duckDB, cfg.CatalogDSN, dataPath); err != nil {
			logger.Warn("DuckLake PostgreSQL attach failed", "error", err)
		} else {
			catalogAttached = true
			logger.Info("DuckLake ready", "mode", "PostgreSQL catalog")
		}
	default:
		logger.Info("no S3 config — running in local mode, use External Locations API to add storage")
	}

	// Open SQLite metastore with hardened connection settings.
	// writeDB: single-connection pool for serialized writes (WAL + txlock=immediate).
	// readDB:  4-connection pool for concurrent reads (WAL, no txlock).
	writeDB, readDB, err := internaldb.OpenSQLitePair(cfg.MetaDBPath, 4)
	if err != nil {
		return fmt.Errorf("open metastore: %w", err)
	}
	defer writeDB.Close() //nolint:errcheck
	defer readDB.Close()  //nolint:errcheck

	// Run migrations on the write pool (DDL requires write access)
	logger.Info("running catalog migrations")
	if err := internaldb.RunMigrations(writeDB); err != nil {
		return fmt.Errorf("migration: %w", err)
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
		return fmt.Errorf("app init: %w", err)
	}

	// Create API handler.
	// Use local interface variables for nil-able services (Manifest, Ingestion)
	// so that a nil concrete pointer becomes a true nil interface rather than
	// a non-nil interface wrapping a nil pointer.
	svc := application.Services
	var manifestSvc api.ManifestService
	if svc.Manifest != nil {
		manifestSvc = svc.Manifest
	}
	var ingestionSvc api.IngestionService
	if svc.Ingestion != nil {
		ingestionSvc = svc.Ingestion
	}
	handler := api.NewHandler(
		svc.Query, svc.Principal, svc.Group, svc.Grant,
		svc.RowFilter, svc.ColumnMask, svc.Audit,
		manifestSvc, svc.Catalog,
		svc.QueryHistory, svc.Lineage, svc.Search, svc.Tag, svc.View,
		ingestionSvc,
		svc.StorageCredential, svc.ExternalLocation,
		svc.Volume,
		svc.ComputeEndpoint,
		svc.APIKey,
	)

	// Create strict handler wrapper
	strictHandler := api.NewStrictHandler(handler, nil)

	// Setup Chi router
	r := chi.NewRouter()
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)

	// Public endpoints — no auth required
	r.Get("/openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		swagger, err := api.GetSwagger()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(swagger)
	})

	r.Get("/docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<!DOCTYPE html>
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
	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

// runAdmin handles "admin promote" and "admin demote" subcommands.
// These operate directly on the SQLite metastore without starting the server.
//
// Usage:
//
//	go run ./cmd/server admin promote --principal=<name>
//	go run ./cmd/server admin demote  --principal=<name>
func runAdmin(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: server admin <promote|demote> --principal=<name>")
	}
	action := args[0]
	if action != "promote" && action != "demote" {
		return fmt.Errorf("unknown admin action %q; use 'promote' or 'demote'", action)
	}

	var principalName string
	for _, arg := range args[1:] {
		if len(arg) > len("--principal=") && arg[:len("--principal=")] == "--principal=" {
			principalName = arg[len("--principal="):]
		}
	}
	if principalName == "" {
		return fmt.Errorf("--principal=<name> is required")
	}

	// Load config for MetaDBPath.
	if err := config.LoadDotEnv(".env"); err != nil {
		// Non-fatal; .env may not exist.
		fmt.Fprintf(os.Stderr, "warn: could not load .env: %v\n", err)
	}
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// Open SQLite directly (single connection is fine for a one-shot CLI).
	db, err := sql.Open("sqlite3", cfg.MetaDBPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("open metastore: %w", err)
	}
	defer db.Close() //nolint:errcheck

	isAdmin := int64(1)
	if action == "demote" {
		isAdmin = 0
	}

	result, err := db.ExecContext(context.Background(),
		"UPDATE principals SET is_admin = ? WHERE name = ?",
		isAdmin, principalName)
	if err != nil {
		return fmt.Errorf("update principal: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check result: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("principal %q not found", principalName)
	}

	fmt.Printf("principal %q %sd successfully\n", principalName, action)
	return nil
}
