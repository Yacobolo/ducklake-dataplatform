package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

// testDB returns an in-memory SQLite DB suitable for satisfying *sql.DB.Conn() calls.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// connEngine returns a mock engine that executes a trivial query on the passed
// connection to produce a valid *sql.Rows. This avoids nil-pointer panics in execOnConn.
func connEngine() *testutil.MockSessionEngine {
	return &testutil.MockSessionEngine{
		QueryOnConnFn: func(ctx context.Context, conn *sql.Conn, principalName, sqlQuery string) (*sql.Rows, error) {
			// Execute a real query on the conn so we get a valid *sql.Rows.
			return conn.QueryContext(ctx, "SELECT 1 WHERE 0")
		},
	}
}

// recordingEngine returns a mock engine that records executed SQL and produces
// valid *sql.Rows via the connection.
func recordingEngine(captured *[]string) *testutil.MockSessionEngine {
	return &testutil.MockSessionEngine{
		QueryOnConnFn: func(ctx context.Context, conn *sql.Conn, principalName, sqlQuery string) (*sql.Rows, error) {
			*captured = append(*captured, sqlQuery)
			return conn.QueryContext(ctx, "SELECT 1 WHERE 0")
		},
	}
}

// === Issue #49: Parameter SQL injection ===

func TestIsValidVariableName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "simple_alpha", input: "date", want: true},
		{name: "with_underscore", input: "start_date", want: true},
		{name: "leading_underscore", input: "_private", want: true},
		{name: "alphanumeric", input: "col1", want: true},
		{name: "all_caps", input: "MY_VAR", want: true},
		{name: "mixed_case", input: "myVar2", want: true},
		{name: "single_char", input: "x", want: true},
		{name: "empty_string", input: "", want: false},
		{name: "starts_with_digit", input: "1abc", want: false},
		{name: "contains_space", input: "my var", want: false},
		{name: "contains_dash", input: "my-var", want: false},
		{name: "contains_semicolon", input: "var;DROP", want: false},
		{name: "contains_quote", input: "var'", want: false},
		{name: "sql_injection_attempt", input: "x; DROP TABLE users --", want: false},
		{name: "contains_dot", input: "schema.table", want: false},
		{name: "contains_parens", input: "fn()", want: false},
		{name: "contains_equals", input: "a=b", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidVariableName(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParameterSanitization_SQLGeneration(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		wantValid bool
		wantSQL   string // expected SQL when valid
	}{
		{
			name:      "valid_parameter",
			key:       "start_date",
			value:     "2026-01-01",
			wantValid: true,
			wantSQL:   "SET VARIABLE start_date = '2026-01-01'",
		},
		{
			name:      "value_with_single_quote_escaped",
			key:       "name",
			value:     "O'Brien",
			wantValid: true,
			wantSQL:   "SET VARIABLE name = 'O''Brien'",
		},
		{
			name:      "value_with_multiple_quotes",
			key:       "val",
			value:     "it''s a 'test'",
			wantValid: true,
			wantSQL:   "SET VARIABLE val = 'it''''s a ''test'''",
		},
		{
			name:      "invalid_key_semicolon",
			key:       "x; DROP TABLE users --",
			value:     "val",
			wantValid: false,
		},
		{
			name:      "invalid_key_starts_with_digit",
			key:       "1bad",
			value:     "val",
			wantValid: false,
		},
		{
			name:      "invalid_key_empty",
			key:       "",
			value:     "val",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := isValidVariableName(tt.key)
			assert.Equal(t, tt.wantValid, valid)

			if valid && tt.wantSQL != "" {
				// Replicate the escaping from executor.go.
				escaped := strings.ReplaceAll(tt.value, "'", "''")
				gotSQL := fmt.Sprintf("SET VARIABLE %s = '%s'", tt.key, escaped)
				assert.Equal(t, tt.wantSQL, gotSQL)
			}
		})
	}
}

func TestExecuteJobAttempt_InvalidParamName(t *testing.T) {
	var capturedSQL []string

	db := testDB(t)
	engine := recordingEngine(&capturedSQL)
	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			return []string{}, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, nil, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	job := domain.PipelineJob{ID: "j1", Name: "test", NotebookID: "nb1"}

	// Invalid param name should return a validation error.
	err := svc.executeJobAttempt(context.Background(), job,
		map[string]string{"bad;key": "val"}, "alice", logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid variable name")

	// No SQL should have been executed.
	assert.Empty(t, capturedSQL)
}

func TestExecuteJobAttempt_QuoteEscaping(t *testing.T) {
	var capturedSQL []string

	db := testDB(t)
	engine := recordingEngine(&capturedSQL)
	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			return []string{}, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, nil, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	job := domain.PipelineJob{ID: "j1", Name: "test", NotebookID: "nb1"}

	err := svc.executeJobAttempt(context.Background(), job,
		map[string]string{"name": "O'Brien"}, "alice", logger)
	require.NoError(t, err)

	// The SET VARIABLE SQL should have escaped single quotes.
	require.Len(t, capturedSQL, 1)
	assert.Equal(t, "SET VARIABLE name = 'O''Brien'", capturedSQL[0])
}

// === Issue #51: CancelRun stops the goroutine ===

func TestExecuteRun_CancellationStopsExecution(t *testing.T) {
	var jobRunStatuses sync.Map // jrID → status

	db := testDB(t)
	engine := connEngine()

	runRepo := &testutil.MockPipelineRunRepo{
		UpdateRunStartedFn: func(ctx context.Context, id string) error {
			return nil
		},
		ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
			return []domain.PipelineJobRun{
				{ID: "jr1", RunID: runID, JobID: "j1", Status: domain.PipelineJobRunStatusPending},
				{ID: "jr2", RunID: runID, JobID: "j2", Status: domain.PipelineJobRunStatusPending},
			}, nil
		},
		UpdateJobRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			jobRunStatuses.Store(id, status)
			return nil
		},
		UpdateRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			return nil
		},
	}

	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			return []string{}, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	jobs := []domain.PipelineJob{
		{ID: "j1", Name: "first", NotebookID: "nb1"},
		{ID: "j2", Name: "second", NotebookID: "nb2"},
	}
	levels := [][]string{{"j1"}, {"j2"}}

	// Create an already-cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	done := make(chan struct{})
	go func() {
		svc.executeRun(ctx, "run1", jobs, levels, map[string]string{}, "alice")
		close(done)
	}()

	select {
	case <-done:
		// Good — completed quickly.
	case <-time.After(5 * time.Second):
		t.Fatal("executeRun did not complete in time after cancellation")
	}

	// Both jobs should be marked as cancelled.
	status1, ok1 := jobRunStatuses.Load("jr1")
	status2, ok2 := jobRunStatuses.Load("jr2")
	assert.True(t, ok1, "jr1 should have been updated")
	assert.True(t, ok2, "jr2 should have been updated")
	assert.Equal(t, domain.PipelineJobRunStatusCancelled, status1)
	assert.Equal(t, domain.PipelineJobRunStatusCancelled, status2)
}

func TestCancelRun_SignalsCancelFunc(t *testing.T) {
	var cancelCalled atomic.Bool

	runRepo := &testutil.MockPipelineRunRepo{
		GetRunByIDFn: func(ctx context.Context, id string) (*domain.PipelineRun, error) {
			return &domain.PipelineRun{
				ID:         id,
				PipelineID: "p1",
				Status:     domain.PipelineRunStatusRunning,
			}, nil
		},
		UpdateRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			return nil
		},
		ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
			return nil, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{}, nil, nil, logger)

	// Simulate a running run by storing a cancel func.
	_, cancel := context.WithCancel(context.Background())
	wrappedCancel := context.CancelFunc(func() {
		cancelCalled.Store(true)
		cancel()
	})
	svc.runCancels.Store("run1", wrappedCancel)

	err := svc.CancelRun(context.Background(), "alice", "run1")
	require.NoError(t, err)

	assert.True(t, cancelCalled.Load(), "cancel function should have been called")

	// Verify it was removed from the map.
	_, exists := svc.runCancels.Load("run1")
	assert.False(t, exists, "cancel func should be removed from map after cancel")
}

func TestExecuteRun_CleansUpCancelFunc(t *testing.T) {
	runRepo := &testutil.MockPipelineRunRepo{
		UpdateRunStartedFn: func(ctx context.Context, id string) error {
			return nil
		},
		ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
			return nil, nil
		},
		UpdateRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			return nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{}, nil, nil, logger)

	// Store a cancel func to simulate TriggerRun behavior.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.runCancels.Store("run1", cancel)

	done := make(chan struct{})
	go func() {
		// No jobs/levels = empty run, completes immediately with SUCCESS.
		svc.executeRun(ctx, "run1", nil, nil, nil, "alice")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeRun did not complete in time")
	}

	// Cancel func should have been cleaned up.
	_, exists := svc.runCancels.Load("run1")
	assert.False(t, exists, "cancel func should be cleaned up after executeRun completes")
}

// === Issue #65: Same-level failure skips remaining jobs ===

func TestExecuteRun_SameLevelFailureSkipsRemainingJobs(t *testing.T) {
	var jobRunStatuses sync.Map // jrID → status

	db := testDB(t)
	engine := connEngine()

	runRepo := &testutil.MockPipelineRunRepo{
		UpdateRunStartedFn: func(ctx context.Context, id string) error {
			return nil
		},
		ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
			return []domain.PipelineJobRun{
				{ID: "jr1", RunID: runID, JobID: "j1", Status: domain.PipelineJobRunStatusPending},
				{ID: "jr2", RunID: runID, JobID: "j2", Status: domain.PipelineJobRunStatusPending},
				{ID: "jr3", RunID: runID, JobID: "j3", Status: domain.PipelineJobRunStatusPending},
			}, nil
		},
		UpdateJobRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			jobRunStatuses.Store(id, status)
			return nil
		},
		UpdateRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			return nil
		},
	}

	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			if notebookID == "nb-fail" {
				return nil, fmt.Errorf("notebook error")
			}
			return []string{}, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	// Jobs j1 and j2 are at the same level. j1 fails, j2 should be skipped.
	// j3 is at the next level and should also be skipped.
	jobs := []domain.PipelineJob{
		{ID: "j1", Name: "failing-job", NotebookID: "nb-fail"},
		{ID: "j2", Name: "should-skip", NotebookID: "nb-ok"},
		{ID: "j3", Name: "next-level", NotebookID: "nb-ok"},
	}
	levels := [][]string{{"j1", "j2"}, {"j3"}}

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		svc.executeRun(ctx, "run1", jobs, levels, map[string]string{}, "alice")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeRun did not complete in time")
	}

	// j1 should be FAILED (executeJob marks it).
	status1, ok1 := jobRunStatuses.Load("jr1")
	require.True(t, ok1, "jr1 should have a status")
	assert.Equal(t, domain.PipelineJobRunStatusFailed, status1, "j1 should be FAILED")

	// j2 should be SKIPPED (same level as j1 which failed).
	status2, ok2 := jobRunStatuses.Load("jr2")
	require.True(t, ok2, "jr2 should have a status")
	assert.Equal(t, domain.PipelineJobRunStatusSkipped, status2, "j2 should be SKIPPED")

	// j3 should be SKIPPED (subsequent level).
	status3, ok3 := jobRunStatuses.Load("jr3")
	require.True(t, ok3, "jr3 should have a status")
	assert.Equal(t, domain.PipelineJobRunStatusSkipped, status3, "j3 should be SKIPPED")
}

func TestExecuteRun_AllJobsSucceed(t *testing.T) {
	var jobRunStatuses sync.Map
	var runFinalStatus string

	db := testDB(t)
	engine := connEngine()

	runRepo := &testutil.MockPipelineRunRepo{
		UpdateRunStartedFn: func(ctx context.Context, id string) error {
			return nil
		},
		ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
			return []domain.PipelineJobRun{
				{ID: "jr1", RunID: runID, JobID: "j1", Status: domain.PipelineJobRunStatusPending},
				{ID: "jr2", RunID: runID, JobID: "j2", Status: domain.PipelineJobRunStatusPending},
			}, nil
		},
		UpdateJobRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			jobRunStatuses.Store(id, status)
			return nil
		},
		UpdateRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			runFinalStatus = status
			return nil
		},
	}

	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			return []string{}, nil // Empty blocks = success
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	jobs := []domain.PipelineJob{
		{ID: "j1", Name: "first", NotebookID: "nb1"},
		{ID: "j2", Name: "second", NotebookID: "nb2"},
	}
	levels := [][]string{{"j1"}, {"j2"}}

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		svc.executeRun(ctx, "run1", jobs, levels, map[string]string{}, "alice")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeRun did not complete in time")
	}

	status1, _ := jobRunStatuses.Load("jr1")
	status2, _ := jobRunStatuses.Load("jr2")
	assert.Equal(t, domain.PipelineJobRunStatusSuccess, status1)
	assert.Equal(t, domain.PipelineJobRunStatusSuccess, status2)
	assert.Equal(t, domain.PipelineRunStatusSuccess, runFinalStatus)
}

func TestExecuteRun_SecondLevelSkippedOnFirstLevelFailure(t *testing.T) {
	var jobRunStatuses sync.Map

	db := testDB(t)
	engine := connEngine()

	runRepo := &testutil.MockPipelineRunRepo{
		UpdateRunStartedFn: func(ctx context.Context, id string) error {
			return nil
		},
		ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
			return []domain.PipelineJobRun{
				{ID: "jr1", RunID: runID, JobID: "j1", Status: domain.PipelineJobRunStatusPending},
				{ID: "jr2", RunID: runID, JobID: "j2", Status: domain.PipelineJobRunStatusPending},
			}, nil
		},
		UpdateJobRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			jobRunStatuses.Store(id, status)
			return nil
		},
		UpdateRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			return nil
		},
	}

	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			if notebookID == "nb-fail" {
				return nil, fmt.Errorf("notebook error")
			}
			return []string{}, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	jobs := []domain.PipelineJob{
		{ID: "j1", Name: "failing", NotebookID: "nb-fail"},
		{ID: "j2", Name: "skipped", NotebookID: "nb-ok"},
	}
	levels := [][]string{{"j1"}, {"j2"}}

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		svc.executeRun(ctx, "run1", jobs, levels, map[string]string{}, "alice")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("executeRun did not complete in time")
	}

	status1, _ := jobRunStatuses.Load("jr1")
	status2, _ := jobRunStatuses.Load("jr2")
	assert.Equal(t, domain.PipelineJobRunStatusFailed, status1)
	assert.Equal(t, domain.PipelineJobRunStatusSkipped, status2)
}

// Test that interruptible retry respects cancellation.
func TestExecuteJob_RetryInterruptedByCancellation(t *testing.T) {
	var attemptCount atomic.Int32

	db := testDB(t)
	engine := connEngine()

	runRepo := &testutil.MockPipelineRunRepo{
		UpdateJobRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string) error {
			return nil
		},
	}

	nbProvider := &testutil.MockNotebookProvider{
		GetSQLBlocksFn: func(ctx context.Context, notebookID string) ([]string, error) {
			attemptCount.Add(1)
			return nil, fmt.Errorf("always fails")
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(nil, runRepo, &testutil.MockAuditRepo{}, nbProvider, engine, db, logger)

	// Job with 5 retries — but we cancel after first attempt.
	job := domain.PipelineJob{
		ID:         "j1",
		Name:       "retryable",
		NotebookID: "nb1",
		RetryCount: 5,
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay (first attempt should complete, then cancel during backoff).
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := svc.executeJob(ctx, job, "jr1", map[string]string{}, "alice", logger)
	require.Error(t, err)

	// Should not have run all 6 attempts — cancellation should have interrupted retry loop.
	attempts := attemptCount.Load()
	assert.Less(t, attempts, int32(6), "should not exhaust all retry attempts when cancelled; got %d", attempts)
}
