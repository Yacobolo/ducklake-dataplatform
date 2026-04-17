package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/testutil"
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
	err := svc.executeJobAttempt(context.Background(), nil, job,
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

	err := svc.executeJobAttempt(context.Background(), nil, job,
		map[string]string{"name": "O'Brien"}, "alice", logger)
	require.NoError(t, err)

	// The SET VARIABLE SQL should have escaped single quotes.
	require.Len(t, capturedSQL, 1)
	assert.Equal(t, "SET VARIABLE name = 'O''Brien'", capturedSQL[0])
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
	pipelineRepo := &testutil.MockPipelineRepo{
		GetPipelineByIDFn: func(ctx context.Context, id string) (*domain.Pipeline, error) {
			return &domain.Pipeline{ID: id, Name: "pipe", CreatedBy: "alice"}, nil
		},
	}

	logger := slog.New(slog.DiscardHandler)
	svc := NewService(pipelineRepo, runRepo, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{}, nil, nil, logger)

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

// Test that interruptible retry respects cancellation.
func TestExecuteJob_RetryInterruptedByCancellation(t *testing.T) {
	var attemptCount atomic.Int32

	db := testDB(t)
	engine := connEngine()

	runRepo := &testutil.MockPipelineRunRepo{
		UpdateJobRunStartedFn: func(ctx context.Context, id string, effectiveComputeEndpointID *string, attemptCount int) error {
			return nil
		},
		UpdateJobRunFinishedFn: func(ctx context.Context, id string, status string, errMsg *string, lastErrorCode *string, attemptCount int) error {
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

	err := svc.executeJob(ctx, nil, job, "run1", "jr1", map[string]string{}, "alice", logger)
	require.Error(t, err)

	// Should not have run all 6 attempts — cancellation should have interrupted retry loop.
	attempts := attemptCount.Load()
	assert.Less(t, attempts, int32(6), "should not exhaust all retry attempts when cancelled; got %d", attempts)
}

func TestExecuteRemoteNotebookJob_CompilesSingleScript(t *testing.T) {
	db := testDB(t)
	var queries []string
	engine := &testutil.MockSessionEngine{
		QueryFn: func(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error) {
			queries = append(queries, sqlQuery)
			return db.QueryContext(ctx, "SELECT 1 WHERE 0")
		},
	}

	svc := NewService(nil, nil, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{}, engine, db, slog.New(slog.DiscardHandler))
	err := svc.executeRemoteNotebookJob(
		context.Background(),
		domain.ComputeEndpoint{ID: "cmp-1", Name: "warehouse", Type: "REMOTE"},
		[]domain.NotebookExecutableCell{
			{SQL: "SELECT getvariable('city')", Role: domain.CellRoleTransform},
			{SQL: "SELECT * FROM failures", Role: domain.CellRoleTest, Test: &domain.NotebookCellTestConfig{Severity: domain.NotebookTestSeverityError}},
			{SQL: "SELECT * FROM warnings", Role: domain.CellRoleTest, Test: &domain.NotebookCellTestConfig{Severity: domain.NotebookTestSeverityWarn}},
		},
		map[string]string{"city": "Copenhagen"},
		"alice",
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	require.Len(t, queries, 1, "remote execution should compile into a single routed query")
	assert.Contains(t, queries[0], "SET VARIABLE city = 'Copenhagen';")
	assert.Contains(t, queries[0], "SELECT getvariable('city');")
	assert.Contains(t, queries[0], "error-severity notebook test failed")
	assert.Contains(t, queries[0], "SELECT COUNT(*) FROM (SELECT * FROM warnings) AS __pipeline_test_warn;")
}

func TestNotifyRunEvent_IgnoresCancelledCallerContext(t *testing.T) {
	delivered := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered <- r
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	svc := NewService(nil, nil, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{}, nil, nil, slog.New(slog.DiscardHandler))
	svc.SetHTTPClient(server.Client())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	svc.notifyRunEvent(ctx, &domain.Pipeline{
		ID:   "pipe-1",
		Name: "demo",
		NotificationWebhooks: []domain.PipelineNotificationWebhook{{
			URL:    server.URL,
			Events: []string{domain.PipelineRunEventFailed},
		}},
	}, &domain.PipelineRun{
		ID:                 "run-1",
		PipelineID:         "pipe-1",
		Status:             domain.PipelineRunStatusFailed,
		TriggerType:        domain.TriggerTypeManual,
		TriggeredBy:        "alice",
		EffectivePrincipal: "alice",
	}, domain.PipelineRunEventFailed)

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("expected webhook delivery despite cancelled caller context")
	}
}
