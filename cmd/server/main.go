// Package main is the entry point for the data platform server.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	_ "github.com/mattn/go-sqlite3"

	"duck-demo/internal/api"
	"duck-demo/internal/app"
	"duck-demo/internal/config"
	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
	"duck-demo/internal/flightsql"
	"duck-demo/internal/middleware"
	"duck-demo/internal/pgwire"
	"duck-demo/internal/ui"
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
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Load .env file (if present) — no logger yet, so use stderr directly.
	if err := config.LoadDotEnv(".env"); err != nil {
		fmt.Fprintf(os.Stderr, "warn: could not load .env: %v\n", err)
	}

	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if cfg.Auth.OIDCEnabled() {
		if err := cfg.Auth.Validate(); err != nil {
			return fmt.Errorf("auth config: %w", err)
		}
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
		Cfg:     cfg,
		DuckDB:  duckDB,
		WriteDB: writeDB,
		ReadDB:  readDB,
		Logger:  logger,
	})
	if err != nil {
		return fmt.Errorf("app init: %w", err)
	}
	var flightServer *flightsql.Server
	if cfg.FeatureFlightSQL {
		flightServer = flightsql.NewServer(cfg.FlightSQLAddr, logger.With("component", "flightsql"), func(ctx context.Context, principal string, sqlQuery string) (*flightsql.QueryResult, error) {
			res, err := application.Services.Query.Execute(ctx, principal, sqlQuery)
			if err != nil {
				return nil, err
			}
			return &flightsql.QueryResult{Columns: res.Columns, Rows: res.Rows}, nil
		})
		if err := flightServer.Start(); err != nil {
			return fmt.Errorf("start Flight SQL listener: %w", err)
		}
	}

	var pgWireServer *pgwire.Server
	if cfg.FeaturePGWire {
		pgWireServer = pgwire.NewServer(cfg.PGWireAddr, logger.With("component", "pgwire"), func(ctx context.Context, principal string, sqlQuery string) (*pgwire.QueryResult, error) {
			res, err := application.Services.Query.Execute(ctx, principal, sqlQuery)
			if err != nil {
				return nil, err
			}
			return &pgwire.QueryResult{Columns: res.Columns, Rows: res.Rows}, nil
		})
		if err := pgWireServer.Start(); err != nil {
			if flightServer != nil {
				_ = flightServer.Shutdown(context.Background())
			}
			return fmt.Errorf("start PG-wire listener: %w", err)
		}
	}
	defer func() {
		if pgWireServer != nil {
			_ = pgWireServer.Shutdown(context.Background())
		}
		if flightServer != nil {
			_ = flightServer.Shutdown(context.Background())
		}
	}()

	// Attach all registered catalogs (concurrent, bounded parallelism)
	if err := application.Services.CatalogRegistration.AttachAll(ctx); err != nil {
		logger.Warn("catalog AttachAll failed", "error", err)
	}

	// Start pipeline scheduler
	if err := application.Scheduler.Start(ctx); err != nil {
		logger.Warn("pipeline scheduler failed to start", "error", err)
	}
	defer application.Scheduler.Stop()

	// Create API handler.
	svc := application.Services
	handler := api.NewHandler(
		svc.Query, svc.Principal, svc.Group, svc.Grant,
		svc.RowFilter, svc.ColumnMask, svc.Audit,
		svc.Manifest, svc.Catalog, svc.CatalogRegistration,
		svc.QueryHistory, svc.Lineage, svc.Search, svc.Tag, svc.View,
		svc.Ingestion,
		svc.StorageCredential, svc.ExternalLocation,
		svc.Volume,
		svc.ComputeEndpoint,
		svc.APIKey,
		svc.Notebook,
		svc.SessionManager,
		svc.GitService,
		svc.Pipeline,
		svc.Model,
		svc.Macro,
		svc.Semantic,
	)

	// Create strict handler wrapper
	strictHandler := api.NewStrictHandler(handler, nil)

	// Setup Chi router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   cfg.CORSAllowedOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-API-Key", "X-Request-ID"},
		ExposedHeaders:   []string{"X-Request-ID", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"},
		AllowCredentials: false,
		MaxAge:           300,
	}))
	r.Use(middleware.RateLimiter(middleware.RateLimitConfig{
		RequestsPerSecond: cfg.RateLimitRPS,
		Burst:             cfg.RateLimitBurst,
	}))

	// Consistent JSON error responses for unknown routes and wrong methods
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 404, "message": "not found"})
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"code": 405, "message": "method not allowed"})
	})

	// Health check endpoint — no auth required, used by load balancers / K8s probes
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

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

	// Construct JWT validator based on auth config.
	var jwtValidator middleware.JWTValidator
	if cfg.Auth.OIDCEnabled() {
		if cfg.Auth.JWKSURL != "" {
			jwtValidator, err = middleware.NewOIDCValidatorFromJWKS(ctx,
				cfg.Auth.JWKSURL, cfg.Auth.IssuerURL, cfg.Auth.Audience,
				cfg.Auth.AllowedIssuers)
		} else {
			jwtValidator, err = middleware.NewOIDCValidator(ctx,
				cfg.Auth.IssuerURL, cfg.Auth.Audience, cfg.Auth.AllowedIssuers)
		}
		if err != nil {
			return fmt.Errorf("oidc validator: %w", err)
		}
		logger.Info("OIDC JWT validation enabled", "issuer", cfg.Auth.IssuerURL)
	} else if cfg.Auth.JWTSecret != "" {
		jwtValidator, err = middleware.NewHS256Validator(cfg.Auth.JWTSecret)
		if err != nil {
			return fmt.Errorf("hs256 validator: %w", err)
		}
		logger.Info("HS256 JWT validation enabled (dev mode)")
	}

	// Authenticated API routes under /v1 prefix (disabled in development)
	authenticator := middleware.NewAuthenticator(
		jwtValidator,
		application.APIKeyRepo,
		application.PrincipalRepo,
		application.Services.Principal, // PrincipalProvisioner for JIT
		cfg.Auth,
		logger,
	)
	if cfg.IsProduction() {
		r.Route("/v1", func(r chi.Router) {
			r.Use(authenticator.Middleware())
			api.HandlerFromMux(strictHandler, r)
		})
	} else {
		logger.Warn("development mode: API auth disabled for /v1")
		r.Route("/v1", func(r chi.Router) {
			api.HandlerFromMux(strictHandler, r)
		})
	}

	uiHandler := ui.NewHandler(
		svc.CatalogRegistration,
		svc.Catalog,
		svc.Query,
		svc.View,
		svc.Pipeline,
		svc.Notebook,
		svc.SessionManager,
		svc.Macro,
		svc.Model,
		cfg.Auth,
		cfg.IsProduction(),
	)
	uiAuth := authenticator.MiddlewareWithUnauthorized(ui.RedirectToLogin)
	if !cfg.IsProduction() {
		logger.Warn("development mode: UI auth disabled for /ui")
		uiAuth = func(next http.Handler) http.Handler { return next }
	}
	r.Route("/ui", func(r chi.Router) {
		ui.MountRoutes(r, uiHandler, uiAuth)
	})

	// Start server
	logger.Info("HTTP API listening", "addr", cfg.ListenAddr)
	logger.Info("try", "curl", fmt.Sprintf("curl -H 'Authorization: Bearer <jwt>' http://%s/v1/principals", curlHostForListenAddr(cfg.ListenAddr)))
	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start session reaper
	go application.Services.SessionManager.ReapIdle(ctx)

	// Graceful shutdown: wait for SIGTERM/SIGINT, then drain connections.
	go func() {
		<-ctx.Done()
		logger.Info("shutting down server")
		if pgWireServer != nil {
			if err := pgWireServer.Shutdown(context.Background()); err != nil {
				logger.Warn("shutdown pgwire listener failed", "error", err)
			}
		}
		if flightServer != nil {
			if err := flightServer.Shutdown(context.Background()); err != nil {
				logger.Warn("shutdown flightsql listener failed", "error", err)
			}
		}
		application.Services.SessionManager.CloseAll()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		logger.Info("TLS enabled for API server", "cert_file", cfg.TLSCertFile)
		if err := srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server: %w", err)
		}
		return nil
	}

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

func curlHostForListenAddr(listenAddr string) string {
	trimmed := strings.TrimSpace(listenAddr)
	if host, port, err := net.SplitHostPort(trimmed); err == nil {
		host = strings.TrimSpace(host)
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "localhost"
		}
		return net.JoinHostPort(host, port)
	}
	if strings.HasPrefix(trimmed, ":") {
		return "localhost" + trimmed
	}
	if trimmed == "" {
		return "localhost:8080"
	}
	return trimmed
}

// runAdmin handles "admin promote" and "admin demote" subcommands.
// These operate directly on the SQLite metastore without starting the server.
//
// Usage:
//
//	go run ./cmd/server admin promote --principal=<name> [--create]
//	go run ./cmd/server admin demote  --principal=<name>
func runAdmin(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: server admin <promote|demote> --principal=<name> [--create]")
	}
	action := args[0]
	if action != "promote" && action != "demote" {
		return fmt.Errorf("unknown admin action %q; use 'promote' or 'demote'", action)
	}

	var principalName string
	var createIfMissing bool
	for _, arg := range args[1:] {
		if len(arg) > len("--principal=") && arg[:len("--principal=")] == "--principal=" {
			principalName = arg[len("--principal="):]
		}
		if arg == "--create" {
			createIfMissing = true
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

	// Run migrations to ensure schema is up to date.
	if err := internaldb.RunMigrations(db); err != nil {
		return fmt.Errorf("migration: %w", err)
	}

	isAdmin := int64(1)
	if action == "demote" {
		isAdmin = 0
	}

	// Check if principal exists.
	var exists bool
	err = db.QueryRowContext(context.Background(),
		"SELECT 1 FROM principals WHERE name = ?", principalName).Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check principal: %w", err)
	}

	if !exists {
		if !createIfMissing {
			return fmt.Errorf("principal %q not found (use --create to create it)", principalName)
		}
		// Create the principal with the desired admin status.
		id := domain.NewID()
		_, err = db.ExecContext(context.Background(),
			"INSERT INTO principals (id, name, type, is_admin) VALUES (?, ?, 'user', ?)",
			id, principalName, isAdmin)
		if err != nil {
			return fmt.Errorf("create principal: %w", err)
		}
		fmt.Printf("principal %q created and %sd successfully\n", principalName, action)
		return nil
	}

	// Principal exists — update admin status.
	_, err = db.ExecContext(context.Background(),
		"UPDATE principals SET is_admin = ? WHERE name = ?",
		isAdmin, principalName)
	if err != nil {
		return fmt.Errorf("update principal: %w", err)
	}

	fmt.Printf("principal %q %sd successfully\n", principalName, action)
	return nil
}
