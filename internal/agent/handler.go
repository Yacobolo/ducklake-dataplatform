// Package agent provides the HTTP handler for the compute agent.
// It is extracted from cmd/compute-agent so that integration tests can
// spin up an in-process agent via httptest.NewServer.
package agent

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"duck-demo/internal/compute"
)

// HandlerConfig holds the parameters needed to build the agent HTTP handler.
type HandlerConfig struct {
	DB                    *sql.DB
	AgentToken            string
	StartTime             time.Time
	MaxMemoryGB           int
	MaxResultRows         int
	MaxConcurrentQueries  int
	QueryTimeout          time.Duration
	RequireSignedRequests bool
	SignatureMaxSkew      time.Duration
	Logger                *slog.Logger
}

// NewHandler builds the compute agent's http.Handler with /execute and /health
// routes. The handler validates X-Agent-Token on /execute requests, executes
// the SQL against the provided DuckDB, and returns JSON results.
func NewHandler(cfg HandlerConfig) http.Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.MaxResultRows <= 0 {
		cfg.MaxResultRows = 10000
	}
	if cfg.MaxConcurrentQueries <= 0 {
		cfg.MaxConcurrentQueries = 8
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 2 * time.Minute
	}
	if cfg.SignatureMaxSkew <= 0 {
		cfg.SignatureMaxSkew = 2 * time.Minute
	}

	querySem := make(chan struct{}, cfg.MaxConcurrentQueries)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /execute", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")

		if subtle.ConstantTimeCompare([]byte(r.Header.Get(compute.HeaderAgentAuth)), []byte(cfg.AgentToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, compute.ErrorResponse{
				Error:     "unauthorized",
				Code:      "AUTH_ERROR",
				RequestID: requestID,
			})
			return
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, compute.ErrorResponse{
				Error:     "invalid request body",
				Code:      "PARSE_ERROR",
				RequestID: requestID,
			})
			return
		}

		if cfg.RequireSignedRequests {
			if err := compute.VerifySignedAgentHeaders(r, cfg.AgentToken, bodyBytes, time.Now(), cfg.SignatureMaxSkew); err != nil {
				writeJSON(w, http.StatusUnauthorized, compute.ErrorResponse{
					Error:     "invalid request signature",
					Code:      "AUTH_ERROR",
					RequestID: requestID,
				})
				return
			}
		}

		select {
		case querySem <- struct{}{}:
			defer func() { <-querySem }()
		default:
			writeJSON(w, http.StatusTooManyRequests, compute.ErrorResponse{
				Error:     "too many concurrent queries",
				Code:      "TOO_MANY_REQUESTS",
				RequestID: requestID,
			})
			return
		}

		var req compute.ExecuteRequest
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, compute.ErrorResponse{
				Error:     "invalid request body",
				Code:      "PARSE_ERROR",
				RequestID: requestID,
			})
			return
		}
		if requestID == "" {
			requestID = req.RequestID
		}
		if strings.TrimSpace(req.SQL) == "" {
			writeJSON(w, http.StatusBadRequest, compute.ErrorResponse{
				Error:     "sql is required",
				Code:      "VALIDATION_ERROR",
				RequestID: requestID,
			})
			return
		}

		logger.Info("executing query", "request_id", requestID)

		queryCtx, cancel := context.WithTimeout(r.Context(), cfg.QueryTimeout)
		defer cancel()

		rows, err := cfg.DB.QueryContext(queryCtx, req.SQL)
		if err != nil {
			logger.Error("query execution failed", "request_id", requestID, "error", err)
			writeJSON(w, http.StatusInternalServerError, compute.ErrorResponse{
				Error:     err.Error(),
				Code:      "EXECUTION_ERROR",
				RequestID: requestID,
			})
			return
		}
		defer rows.Close() //nolint:errcheck

		cols, err := rows.Columns()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, compute.ErrorResponse{
				Error:     err.Error(),
				Code:      "EXECUTION_ERROR",
				RequestID: requestID,
			})
			return
		}

		var resultRows [][]interface{}
		for rows.Next() {
			if len(resultRows) >= cfg.MaxResultRows {
				writeJSON(w, http.StatusRequestEntityTooLarge, compute.ErrorResponse{
					Error:     "result row limit exceeded",
					Code:      "RESULT_LIMIT_EXCEEDED",
					RequestID: requestID,
				})
				return
			}
			values := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				writeJSON(w, http.StatusInternalServerError, compute.ErrorResponse{
					Error:     err.Error(),
					Code:      "SCAN_ERROR",
					RequestID: requestID,
				})
				return
			}
			resultRows = append(resultRows, values)
		}
		if err := rows.Err(); err != nil {
			writeJSON(w, http.StatusInternalServerError, compute.ErrorResponse{
				Error:     err.Error(),
				Code:      "EXECUTION_ERROR",
				RequestID: requestID,
			})
			return
		}

		logger.Info("query completed", "request_id", requestID, "row_count", len(resultRows))

		writeJSON(w, http.StatusOK, compute.ExecuteResponse{
			Columns:   cols,
			Rows:      resultRows,
			RowCount:  len(resultRows),
			RequestID: requestID,
		})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if subtle.ConstantTimeCompare([]byte(r.Header.Get(compute.HeaderAgentAuth)), []byte(cfg.AgentToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, compute.ErrorResponse{
				Error:     "unauthorized",
				Code:      "AUTH_ERROR",
				RequestID: requestID,
			})
			return
		}
		if cfg.RequireSignedRequests {
			if err := compute.VerifySignedAgentHeaders(r, cfg.AgentToken, nil, time.Now(), cfg.SignatureMaxSkew); err != nil {
				writeJSON(w, http.StatusUnauthorized, compute.ErrorResponse{
					Error:     "invalid request signature",
					Code:      "AUTH_ERROR",
					RequestID: requestID,
				})
				return
			}
		}

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
