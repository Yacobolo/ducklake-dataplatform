// Package agent provides the HTTP handler for the compute agent.
// It is extracted from cmd/compute-agent so that integration tests can
// spin up an in-process agent via httptest.NewServer.
package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	CursorMode      bool
	MetricsProvider func() (active, queued, running, completed, stored, cleaned int64)
	Logger          *slog.Logger
}

type queryJob struct {
	mu          sync.RWMutex
	id          string
	requestID   string
	status      string
	columns     []string
	rowCount    int
	tableName   string
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
		RowCount:  j.rowCount,
		Error:     j.errorMsg,
		RequestID: j.requestID,
	}
	if !j.completedAt.IsZero() {
		resp.CompletedAt = j.completedAt.UTC().Format(time.RFC3339)
	}
	return resp
}

func (j *queryJob) resultPage(ctx context.Context, db *sql.DB, offset, limit int) (compute.FetchQueryResultsResponse, error) {
	j.mu.RLock()
	tableName := j.tableName
	columns := append([]string(nil), j.columns...)
	rowCount := j.rowCount
	requestID := j.requestID
	j.mu.RUnlock()

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultPageSize
	}

	rows, err := queryResultPage(ctx, db, tableName, offset, limit)
	if err != nil {
		return compute.FetchQueryResultsResponse{}, err
	}

	nextPageToken := ""
	if offset+len(rows) < rowCount {
		nextPageToken = compute.EncodePageToken(offset + len(rows))
	}

	return compute.FetchQueryResultsResponse{
		QueryID:       j.id,
		Columns:       columns,
		Rows:          rows,
		RowCount:      rowCount,
		NextPageToken: nextPageToken,
		RequestID:     requestID,
	}, nil
}

func (j *queryJob) setRunning(cancel context.CancelFunc) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = compute.QueryStatusRunning
	j.cancel = cancel
}

func (j *queryJob) setSucceeded(columns []string, tableName string, rowCount int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.status = compute.QueryStatusSucceeded
	j.columns = append([]string(nil), columns...)
	j.rowCount = rowCount
	j.tableName = tableName
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
	db              *sql.DB
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
		tableName := job.tableName
		job.mu.RUnlock()

		if !isTerminalStatus(status) || completedAt.IsZero() {
			continue
		}
		if now.Sub(completedAt) < s.ttl {
			continue
		}

		if tableName != "" {
			_, _ = s.db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %q", tableName)) //nolint:gosec // internal generated identifier
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
	mux := http.NewServeMux()
	ttl := cfg.QueryResultTTL
	if ttl <= 0 {
		ttl = defaultQueryResultTTL
	}

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		var version string
		row := cfg.DB.QueryRowContext(r.Context(), "SELECT version()")
		_ = row.Scan(&version)

		var memUsedBytes int64
		memRow := cfg.DB.QueryRowContext(r.Context(), "SELECT memory_usage FROM duckdb_memory()")
		_ = memRow.Scan(&memUsedBytes)
		memUsedMB := memUsedBytes / (1024 * 1024)
		var activeQueries int64
		queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs := int64(0), int64(0), int64(0), int64(0), int64(0)
		if cfg.MetricsProvider != nil {
			activeQueries, queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs = cfg.MetricsProvider()
		}

		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status":                   "ok",
			"uptime_seconds":           int(time.Since(cfg.StartTime).Seconds()),
			"duckdb_version":           version,
			"memory_used_mb":           memUsedMB,
			"max_memory_gb":            cfg.MaxMemoryGB,
			"active_queries":           activeQueries,
			"queued_jobs":              queuedJobs,
			"running_jobs":             runningJobs,
			"completed_jobs":           completedJobs,
			"stored_jobs":              storedJobs,
			"cleaned_jobs":             cleanedJobs,
			"query_result_ttl_seconds": int(ttl.Seconds()),
		})
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		var activeQueries int64
		queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs := int64(0), int64(0), int64(0), int64(0), int64(0)
		if cfg.MetricsProvider != nil {
			activeQueries, queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs = cfg.MetricsProvider()
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w,
			"# HELP duck_compute_active_queries Number of currently running SQL statements\n"+
				"# TYPE duck_compute_active_queries gauge\n"+
				"duck_compute_active_queries %d\n"+
				"# HELP duck_compute_queued_jobs Number of queued lifecycle jobs\n"+
				"# TYPE duck_compute_queued_jobs gauge\n"+
				"duck_compute_queued_jobs %d\n"+
				"# HELP duck_compute_running_jobs Number of running lifecycle jobs\n"+
				"# TYPE duck_compute_running_jobs gauge\n"+
				"duck_compute_running_jobs %d\n"+
				"# HELP duck_compute_completed_jobs Number of retained completed lifecycle jobs\n"+
				"# TYPE duck_compute_completed_jobs gauge\n"+
				"duck_compute_completed_jobs %d\n"+
				"# HELP duck_compute_stored_jobs Number of stored lifecycle jobs\n"+
				"# TYPE duck_compute_stored_jobs gauge\n"+
				"duck_compute_stored_jobs %d\n"+
				"# HELP duck_compute_cleaned_jobs_total Number of cleaned jobs\n"+
				"# TYPE duck_compute_cleaned_jobs_total counter\n"+
				"duck_compute_cleaned_jobs_total %d\n",
			activeQueries, queuedJobs, runningJobs, completedJobs, storedJobs, cleanedJobs,
		)
	})

	return mux
}

func runQueryToTable(ctx context.Context, db *sql.DB, sqlQuery string, activeQueries *atomic.Int64, queryID string) ([]string, string, int, error) {
	activeQueries.Add(1)
	defer activeQueries.Add(-1)

	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close() //nolint:errcheck

	cols, err := rows.Columns()
	if err != nil {
		return nil, "", 0, err
	}

	tableName := "_agent_result_" + strings.ReplaceAll(queryID, "-", "")
	if err := createResultTable(ctx, db, tableName, cols); err != nil {
		return nil, "", 0, err
	}

	insertSQL := fmt.Sprintf("INSERT INTO %q VALUES (%s)", tableName, strings.TrimRight(strings.Repeat("?,", len(cols)), ",")) //nolint:gosec // internal identifier
	count := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, "", 0, err
		}

		args := make([]interface{}, len(cols))
		for i, v := range values {
			if v == nil {
				args[i] = nil
				continue
			}
			args[i] = fmt.Sprintf("%v", v)
		}
		if _, err := db.ExecContext(ctx, insertSQL, args...); err != nil {
			return nil, "", 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}

	return cols, tableName, count, nil
}

func createResultTable(ctx context.Context, db *sql.DB, tableName string, cols []string) error {
	colDefs := make([]string, 0, len(cols))
	for _, col := range cols {
		colDefs = append(colDefs, fmt.Sprintf("%q VARCHAR", col))
	}
	createSQL := fmt.Sprintf("CREATE TABLE %q (%s)", tableName, strings.Join(colDefs, ", "))
	_, err := db.ExecContext(ctx, createSQL)
	if err != nil {
		return err
	}
	return nil
}

func queryResultPage(ctx context.Context, db *sql.DB, tableName string, offset, limit int) ([][]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %q LIMIT ? OFFSET ?", tableName) //nolint:gosec // internal identifier
	rows, err := db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	out := make([][]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]interface{}, len(values))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
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
