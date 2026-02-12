// Package agent provides the HTTP handler for the compute agent.
// It is extracted from cmd/compute-agent so that integration tests can
// spin up an in-process agent via httptest.NewServer.
package agent

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// HandlerConfig holds the parameters needed to build the agent HTTP handler.
type HandlerConfig struct {
	DB          *sql.DB
	AgentToken  string
	StartTime   time.Time
	MaxMemoryGB int
	Logger      *slog.Logger
}

// NewHandler builds the compute agent's http.Handler with /execute and /health
// routes. The handler validates X-Agent-Token on /execute requests, executes
// the SQL against the provided DuckDB, and returns JSON results.
func NewHandler(cfg HandlerConfig) http.Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

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

		rows, err := cfg.DB.QueryContext(r.Context(), req.SQL)
		if err != nil {
			logger.Error("query execution failed", "request_id", requestID, "error", err)
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":      err.Error(),
				"code":       "EXECUTION_ERROR",
				"request_id": requestID,
			})
			return
		}
		defer rows.Close() //nolint:errcheck

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
		row := cfg.DB.QueryRowContext(r.Context(), "SELECT version()")
		_ = row.Scan(&version)

		// Query memory usage from DuckDB
		var memUsedBytes int64
		memRow := cfg.DB.QueryRowContext(r.Context(), "SELECT memory_usage FROM duckdb_memory()")
		_ = memRow.Scan(&memUsedBytes)
		memUsedMB := memUsedBytes / (1024 * 1024)

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":         "ok",
			"uptime_seconds": int(time.Since(cfg.StartTime).Seconds()),
			"duckdb_version": version,
			"memory_used_mb": memUsedMB,
			"max_memory_gb":  cfg.MaxMemoryGB,
		})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
