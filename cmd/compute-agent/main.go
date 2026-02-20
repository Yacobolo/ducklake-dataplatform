// Package main is the entry point for the compute agent binary.
// The agent opens an in-memory DuckDB, optionally attaches a DuckLake catalog
// via PostgreSQL, and exposes POST /execute and GET /health over HTTP.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"duck-demo/internal/agent"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := loadAgentConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Open DuckDB in-memory
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close() //nolint:errcheck

	// Set memory limit if configured
	if cfg.MaxMemoryGB > 0 {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("SET max_memory='%dGB'", cfg.MaxMemoryGB)); err != nil {
			return fmt.Errorf("set max_memory: %w", err)
		}
		logger.Info("memory limit set", "max_memory_gb", cfg.MaxMemoryGB)
	}

	// Install extensions
	extensions := []string{
		"INSTALL ducklake; LOAD ducklake;",
		"INSTALL httpfs; LOAD httpfs;",
		"INSTALL postgres; LOAD postgres;",
	}
	for _, ext := range extensions {
		if _, err := db.ExecContext(ctx, ext); err != nil {
			return fmt.Errorf("extension setup (%s): %w", ext, err)
		}
	}
	logger.Info("DuckDB extensions installed")

	// Create S3 secret if configured
	if cfg.S3KeyID != "" {
		secretSQL := fmt.Sprintf(`CREATE SECRET agent_s3 (
			TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', REGION '%s', URL_STYLE 'path'
		)`, cfg.S3KeyID, cfg.S3Secret, cfg.S3Endpoint, cfg.S3Region)
		if _, err := db.ExecContext(ctx, secretSQL); err != nil {
			return fmt.Errorf("create S3 secret: %w", err)
		}
		logger.Info("S3 secret created")
	}

	// Attach DuckLake via PostgreSQL if catalog DSN is provided
	if cfg.CatalogDSN != "" {
		dataPath := fmt.Sprintf("s3://%s/lake_data/", cfg.S3Bucket)
		attachSQL := fmt.Sprintf("ATTACH 'ducklake:postgres:%s' AS lake (DATA_PATH '%s')", cfg.CatalogDSN, dataPath)
		if _, err := db.ExecContext(ctx, attachSQL); err != nil {
			return fmt.Errorf("attach ducklake: %w", err)
		}
		if _, err := db.ExecContext(ctx, "USE lake"); err != nil {
			return fmt.Errorf("use lake: %w", err)
		}
		logger.Info("DuckLake attached via PostgreSQL", "catalog_dsn", "[redacted]", "data_path", dataPath)
	}

	handler := agent.NewHandler(agent.HandlerConfig{
		DB:                    db,
		AgentToken:            cfg.AgentToken,
		StartTime:             time.Now(),
		MaxMemoryGB:           cfg.MaxMemoryGB,
		MaxResultRows:         cfg.MaxResultRows,
		MaxConcurrentQueries:  cfg.MaxConcurrentQueries,
		QueryTimeout:          cfg.QueryTimeout,
		RequireSignedRequests: cfg.RequireSignedRequests,
		SignatureMaxSkew:      cfg.SignatureMaxSkew,
		Logger:                logger,
	})

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		logger.Info("shutting down agent")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("compute agent listening", "addr", cfg.ListenAddr)
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		logger.Info("TLS enabled for compute agent", "cert_file", cfg.TLSCertFile)
		if err := srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("server: %w", err)
		}
		return nil
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}
