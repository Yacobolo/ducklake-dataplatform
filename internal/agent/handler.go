// Package agent provides the HTTP handler for the compute agent.
// It is extracted from cmd/compute-agent so that integration tests can
// spin up an in-process agent via httptest.NewServer.
package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"duck-demo/internal/compute"
)

const (
	defaultPageSize        = 1000
	defaultQueryResultTTL  = 10 * time.Minute
	defaultCleanupInterval = 1 * time.Minute
)

// HandlerConfig holds the parameters needed to build the agent HTTP handler.
type HandlerConfig struct {
	DB              *sql.DB
	AgentToken      string
	StartTime       time.Time
	MaxMemoryGB     int
	QueryResultTTL  time.Duration
	CleanupInterval time.Duration
	Logger          *slog.Logger
}

type queryJob struct {
	mu          sync.RWMutex
	id          string
	requestID   string
	status      string
	columns     []string
	rows        [][]interface{}
	errorMsg    string
	createdAt   time.Time
	completedAt time.Time
	cancel      context.CancelFunc
}

func (j *queryJob) statusResponse() compute.QueryStatusResponse {
	j.mu.RLock()
	defer j.mu.RUnlock()

	resp := compute.QueryStatusResponse{
		QueryID:   j.id,
		Status:    j.status,
		Columns:   append([]string(nil), j.columns...),
		RowCount:  len(j.rows),
		Error:     j.errorMsg,
		RequestID: j.requestID,
	}
	if !j.completedAt.IsZero() {
		resp.CompletedAt = j.completedAt.UTC().Format(time.RFC3339)
	}
	return resp
}

func (j *queryJob) resultPage(offset, limit int) compute.FetchQueryResultsResponse {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultPageSize
	}

	end := offset + limit
	if end > len(j.rows) {
		end = len(j.rows)
	}

	rows := make([][]interface{}, 0, end-offset)
	for i := offset; i < end; i++ {
		rows = append(rows, append([]interface{}(nil), j.rows[i]...))
	}

	nextPageToken := ""
	if end < len(j.rows) {
		nextPageToken = compute.EncodePageToken(end)
	}

	return compute.FetchQueryResultsResponse{
		QueryID:       j.id,
		Columns:       append([]string(nil), j.columns...),
		Rows:          rows,
		RowCount:      len(j.rows),
		NextPageToken: nextPageToken,
		RequestID:     j.requestID,
	}
}

func (j *queryJob) setRunning(cancel context.CancelFunc) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = compute.QueryStatusRunning
	j.cancel = cancel
}

func (j *queryJob) setSucceeded(columns []string, rows [][]interface{}) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = compute.QueryStatusSucceeded
	j.columns = append([]string(nil), columns...)
	j.rows = rows
	j.errorMsg = ""
	j.completedAt = time.Now()
	j.cancel = nil
}

func (j *queryJob) setFailed(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = compute.QueryStatusFailed
	j.errorMsg = err.Error()
	j.completedAt = time.Now()
	j.cancel = nil
}

func (j *queryJob) setCanceled() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = compute.QueryStatusCanceled
	if j.errorMsg == "" {
		j.errorMsg = "query canceled"
	}
	j.completedAt = time.Now()
	j.cancel = nil
}

func (j *queryJob) cancelQuery() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cancel != nil {
		j.cancel()
	}
	if j.status == compute.QueryStatusQueued || j.status == compute.QueryStatusRunning {
		j.status = compute.QueryStatusCanceled
		j.errorMsg = "query canceled"
		j.completedAt = time.Now()
	}
	j.cancel = nil
}

type queryStore struct {
	mu              sync.RWMutex
	jobs            map[string]*queryJob
	requestToQuery  map[string]string
	ttl             time.Duration
	cleanupInterval time.Duration
	lastCleanup     time.Time
	cleanedJobs     int64
}

func newQueryStore(ttl, cleanupInterval time.Duration) *queryStore {
	if ttl <= 0 {
		ttl = defaultQueryResultTTL
	}
	if cleanupInterval <= 0 {
		cleanupInterval = defaultCleanupInterval
	}

	return &queryStore{
		jobs:            map[string]*queryJob{},
		requestToQuery:  map[string]string{},
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
	}
}

func (s *queryStore) set(job *queryJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.id] = job
	if job.requestID != "" {
		s.requestToQuery[job.requestID] = job.id
	}
}

func (s *queryStore) get(id string) (*queryJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

func (s *queryStore) getByRequestID(requestID string) (*queryJob, bool) {
	if requestID == "" {
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	queryID, ok := s.requestToQuery[requestID]
	if !ok {
		return nil, false
	}
	job, ok := s.jobs[queryID]
	return job, ok
}

func (s *queryStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for requestID, queryID := range s.requestToQuery {
		if queryID == id {
			delete(s.requestToQuery, requestID)
		}
	}
	delete(s.jobs, id)
}

func (s *queryStore) maybeCleanup(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.lastCleanup.IsZero() && now.Sub(s.lastCleanup) < s.cleanupInterval {
		return
	}

	for id, job := range s.jobs {
		job.mu.RLock()
		status := job.status
		completedAt := job.completedAt
		job.mu.RUnlock()

		if !isTerminalStatus(status) || completedAt.IsZero() {
			continue
		}
		if now.Sub(completedAt) < s.ttl {
			continue
		}

		delete(s.jobs, id)
		for requestID, queryID := range s.requestToQuery {
			if queryID == id {
				delete(s.requestToQuery, requestID)
			}
		}
		s.cleanedJobs++
	}

	s.lastCleanup = now
}

func (s *queryStore) metrics() (queued int64, running int64, completed int64, stored int64, cleaned int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored = int64(len(s.jobs))
	cleaned = s.cleanedJobs

	for _, job := range s.jobs {
		status := job.statusResponse().Status
		switch status {
		case compute.QueryStatusQueued:
			queued++
		case compute.QueryStatusRunning:
			running++
		case compute.QueryStatusSucceeded, compute.QueryStatusFailed, compute.QueryStatusCanceled:
			completed++
		}
	}
	return queued, running, completed, stored, cleaned
}

func isTerminalStatus(status string) bool {
	switch status {
	case compute.QueryStatusSucceeded, compute.QueryStatusFailed, compute.QueryStatusCanceled:
		return true
	default:
		return false
	}
}

// NewHandler builds the compute agent's http.Handler with query execution and
// health routes. The handler validates X-Agent-Token on all compute routes.
func NewHandler(cfg HandlerConfig) http.Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var activeQueries atomic.Int64
	jobs := newQueryStore(cfg.QueryResultTTL, cfg.CleanupInterval)

	mux := http.NewServeMux()

	authorize := func(w http.ResponseWriter, r *http.Request, requestID string) bool {
		if r.Header.Get("X-Agent-Token") == cfg.AgentToken {
			return true
		}
		writeJSON(w, http.StatusUnauthorized, compute.ErrorResponse{Error: "unauthorized", Code: "AUTH_ERROR", RequestID: requestID})
		return false
	}

	mux.HandleFunc("POST /execute", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !authorize(w, r, requestID) {
			return
		}

		var req compute.ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, compute.ErrorResponse{Error: "invalid request body", Code: "PARSE_ERROR", RequestID: requestID})
			return
		}
		if requestID == "" {
			requestID = req.RequestID
		}

		result, err := runQuery(r.Context(), cfg.DB, req.SQL, &activeQueries)
		if err != nil {
			logger.Error("query execution failed", "request_id", requestID, "error", err)
			writeJSON(w, http.StatusInternalServerError, compute.ErrorResponse{Error: err.Error(), Code: "EXECUTION_ERROR", RequestID: requestID})
			return
		}
		result.RequestID = requestID
		writeJSON(w, http.StatusOK, result)
	})

	mux.HandleFunc("POST /queries", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !authorize(w, r, requestID) {
			return
		}

		var req compute.SubmitQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, compute.ErrorResponse{Error: "invalid request body", Code: "PARSE_ERROR", RequestID: requestID})
			return
		}
		if req.SQL == "" {
			writeJSON(w, http.StatusBadRequest, compute.ErrorResponse{Error: "sql is required", Code: "VALIDATION_ERROR", RequestID: requestID})
			return
		}
		if requestID == "" {
			requestID = req.RequestID
		}
		jobs.maybeCleanup(time.Now())

		if existing, ok := jobs.getByRequestID(requestID); ok {
			status := existing.statusResponse().Status
			writeJSON(w, http.StatusAccepted, compute.SubmitQueryResponse{QueryID: existing.id, Status: status, RequestID: requestID})
			return
		}

		job := &queryJob{
			id:        uuid.NewString(),
			requestID: requestID,
			status:    compute.QueryStatusQueued,
			createdAt: time.Now(),
		}
		jobs.set(job)

		go func() {
			ctx, cancel := context.WithCancel(context.Background())
			job.setRunning(cancel)

			result, err := runQuery(ctx, cfg.DB, req.SQL, &activeQueries)
			if err != nil {
				if ctx.Err() == context.Canceled {
					job.setCanceled()
					return
				}
				job.setFailed(err)
				return
			}
			job.setSucceeded(result.Columns, result.Rows)
		}()

		writeJSON(w, http.StatusAccepted, compute.SubmitQueryResponse{QueryID: job.id, Status: compute.QueryStatusQueued, RequestID: requestID})
	})

	mux.HandleFunc("GET /queries/{queryID}", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !authorize(w, r, requestID) {
			return
		}

		jobs.maybeCleanup(time.Now())

		jobID := r.PathValue("queryID")
		job, ok := jobs.get(jobID)
		if !ok {
			writeJSON(w, http.StatusNotFound, compute.ErrorResponse{Error: "query not found", Code: "NOT_FOUND", RequestID: requestID})
			return
		}
		writeJSON(w, http.StatusOK, job.statusResponse())
	})

	mux.HandleFunc("GET /queries/{queryID}/results", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !authorize(w, r, requestID) {
			return
		}

		jobs.maybeCleanup(time.Now())

		jobID := r.PathValue("queryID")
		job, ok := jobs.get(jobID)
		if !ok {
			writeJSON(w, http.StatusNotFound, compute.ErrorResponse{Error: "query not found", Code: "NOT_FOUND", RequestID: requestID})
			return
		}

		status := job.statusResponse()
		switch status.Status {
		case compute.QueryStatusQueued, compute.QueryStatusRunning:
			writeJSON(w, http.StatusConflict, compute.ErrorResponse{Error: "query is not ready", Code: "QUERY_NOT_READY", RequestID: requestID})
			return
		case compute.QueryStatusFailed, compute.QueryStatusCanceled:
			writeJSON(w, http.StatusConflict, compute.ErrorResponse{Error: status.Error, Code: "QUERY_NOT_AVAILABLE", RequestID: requestID})
			return
		}

		pageToken := r.URL.Query().Get("page_token")
		offset := compute.DecodePageToken(pageToken)
		limit := defaultPageSize
		if raw := r.URL.Query().Get("max_results"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				if parsed > 5000 {
					parsed = 5000
				}
				limit = parsed
			}
		}

		writeJSON(w, http.StatusOK, job.resultPage(offset, limit))
	})

	mux.HandleFunc("POST /queries/{queryID}/cancel", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !authorize(w, r, requestID) {
			return
		}

		jobs.maybeCleanup(time.Now())

		jobID := r.PathValue("queryID")
		job, ok := jobs.get(jobID)
		if !ok {
			writeJSON(w, http.StatusNotFound, compute.ErrorResponse{Error: "query not found", Code: "NOT_FOUND", RequestID: requestID})
			return
		}
		job.cancelQuery()
		writeJSON(w, http.StatusOK, compute.CancelQueryResponse{QueryID: jobID, Status: job.statusResponse().Status, RequestID: requestID})
	})

	mux.HandleFunc("DELETE /queries/{queryID}", func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if !authorize(w, r, requestID) {
			return
		}

		jobs.maybeCleanup(time.Now())

		jobID := r.PathValue("queryID")
		job, ok := jobs.get(jobID)
		if !ok {
			writeJSON(w, http.StatusNotFound, compute.ErrorResponse{Error: "query not found", Code: "NOT_FOUND", RequestID: requestID})
			return
		}
		job.cancelQuery()
		jobs.delete(jobID)
		writeJSON(w, http.StatusOK, compute.CancelQueryResponse{QueryID: jobID, Status: compute.QueryStatusCanceled, RequestID: requestID})
	})

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		jobs.maybeCleanup(time.Now())

		var version string
		row := cfg.DB.QueryRowContext(r.Context(), "SELECT version()")
		_ = row.Scan(&version)

		var memUsedBytes int64
		memRow := cfg.DB.QueryRowContext(r.Context(), "SELECT memory_usage FROM duckdb_memory()")
		_ = memRow.Scan(&memUsedBytes)
		memUsedMB := memUsedBytes / (1024 * 1024)
		queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs := jobs.metrics()

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":                   "ok",
			"uptime_seconds":           int(time.Since(cfg.StartTime).Seconds()),
			"duckdb_version":           version,
			"memory_used_mb":           memUsedMB,
			"max_memory_gb":            cfg.MaxMemoryGB,
			"active_queries":           activeQueries.Load(),
			"queued_jobs":              queuedJobs,
			"running_jobs":             runningJobs,
			"completed_jobs":           completedJobs,
			"stored_jobs":              storedJobs,
			"cleaned_jobs":             cleanedJobs,
			"query_result_ttl_seconds": int(jobs.ttl.Seconds()),
		})
	})

	return mux
}

func runQuery(ctx context.Context, db *sql.DB, sqlQuery string, activeQueries *atomic.Int64) (compute.ExecuteResponse, error) {
	activeQueries.Add(1)
	defer activeQueries.Add(-1)

	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return compute.ExecuteResponse{}, err
	}
	defer rows.Close() //nolint:errcheck

	cols, err := rows.Columns()
	if err != nil {
		return compute.ExecuteResponse{}, err
	}

	resultRows := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return compute.ExecuteResponse{}, err
		}
		resultRows = append(resultRows, values)
	}
	if err := rows.Err(); err != nil {
		return compute.ExecuteResponse{}, err
	}

	return compute.ExecuteResponse{
		Columns:  cols,
		Rows:     resultRows,
		RowCount: len(resultRows),
	}, nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
