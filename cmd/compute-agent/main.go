// Package main is the entry point for the compute agent binary.
// The agent opens an in-memory DuckDB, optionally attaches a DuckLake catalog
// via PostgreSQL, and exposes POST /execute and GET /health over HTTP.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
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
	defer db.Close()

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

	startTime := time.Now()

	// HTTP server
	mux := http.NewServeMux()

	mux.HandleFunc("POST /execute", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")

		// Validate token
		if r.Header.Get("X-Agent-Token") != cfg.AgentToken {
			writeJSON(w, http.StatusUnauthorized, map[string]interface{}{
				"error":      "unauthorized",
				"code":       "AUTH_ERROR",
				"request_id": requestID,
			})
			return
		}

		var req struct {
			SQL       string `json:"sql"`
			RequestID string `json:"request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]interface{}{
				"error":      "invalid request body",
				"code":       "PARSE_ERROR",
				"request_id": requestID,
			})
			return
		}
		if requestID == "" {
			requestID = req.RequestID
		}

		logger.Info("executing query", "request_id", requestID)

		rows, err := db.QueryContext(r.Context(), req.SQL)
		if err != nil {
			logger.Error("query execution failed", "request_id", requestID, "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":      err.Error(),
				"code":       "EXECUTION_ERROR",
				"request_id": requestID,
			})
			return
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":      err.Error(),
				"code":       "EXECUTION_ERROR",
				"request_id": requestID,
			})
			return
		}

		var resultRows [][]interface{}
		for rows.Next() {
			values := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"error":      err.Error(),
					"code":       "SCAN_ERROR",
					"request_id": requestID,
				})
				return
			}
			resultRows = append(resultRows, values)
		}
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":      err.Error(),
				"code":       "EXECUTION_ERROR",
				"request_id": requestID,
			})
			return
		}

		logger.Info("query completed", "request_id", requestID, "row_count", len(resultRows))

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"columns":    cols,
			"rows":       resultRows,
			"row_count":  len(resultRows),
			"request_id": requestID,
		})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		var version string
		row := db.QueryRowContext(r.Context(), "SELECT version()")
		_ = row.Scan(&version)

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":         "ok",
			"uptime_seconds": int(time.Since(startTime).Seconds()),
			"duckdb_version": version,
			"max_memory_gb":  cfg.MaxMemoryGB,
		})
	})

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      mux,
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
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
