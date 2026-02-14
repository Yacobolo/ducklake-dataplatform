package notebook

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSessionEngine implements domain.SessionEngine for testing.
type mockSessionEngine struct {
	queryOnConnFn func(ctx context.Context, conn *sql.Conn, principalName, sqlQuery string) (*sql.Rows, error)
	queryFn       func(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error)
}

func (m *mockSessionEngine) QueryOnConn(ctx context.Context, conn *sql.Conn, principalName, sqlQuery string) (*sql.Rows, error) {
	if m.queryOnConnFn != nil {
		return m.queryOnConnFn(ctx, conn, principalName, sqlQuery)
	}
	// Default: execute the SQL directly on the conn
	return conn.QueryContext(ctx, sqlQuery)
}

func (m *mockSessionEngine) Query(ctx context.Context, principalName, sqlQuery string) (*sql.Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, principalName, sqlQuery)
	}
	panic("unexpected call to mockSessionEngine.Query")
}

var _ domain.SessionEngine = (*mockSessionEngine)(nil)

// openTestDB returns an in-memory SQLite DB for session testing.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// setupSessionManager creates a SessionManager with test dependencies.
func setupSessionManager(t *testing.T) (*SessionManager, *testutil.MockNotebookRepo, *testutil.MockNotebookJobRepo, *mockSessionEngine, *sql.DB) {
	t.Helper()
	db := openTestDB(t)
	repo := &testutil.MockNotebookRepo{}
	jobRepo := &testutil.MockNotebookJobRepo{}
	audit := &testutil.MockAuditRepo{}
	engine := &mockSessionEngine{}
	sm := NewSessionManager(db, engine, repo, jobRepo, audit)
	t.Cleanup(func() { sm.CloseAll() })
	return sm, repo, jobRepo, engine, db
}

// === CreateSession ===

func TestSessionManager_CreateSession(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)
		assert.NotEmpty(t, sess.ID)
		assert.Equal(t, "nb-1", sess.NotebookID)
		assert.Equal(t, "alice", sess.Principal)
		assert.Equal(t, "active", sess.State)
	})

	t.Run("notebook not found", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, _ string) (*domain.Notebook, error) {
			return nil, domain.ErrNotFound("notebook not found")
		}

		_, err := sm.CreateSession(ctx, "nonexistent", "alice")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("multiple sessions for same notebook", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		s1, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		s2, err := sm.CreateSession(ctx, "nb-1", "bob")
		require.NoError(t, err)

		assert.NotEqual(t, s1.ID, s2.ID, "each session should have a unique ID")
	})
}

// === CloseSession ===

func TestSessionManager_CloseSession(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		err = sm.CloseSession(ctx, sess.ID)
		require.NoError(t, err)

		// Closing again should fail with not found
		err = sm.CloseSession(ctx, sess.ID)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("not found", func(t *testing.T) {
		sm, _, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		err := sm.CloseSession(ctx, "nonexistent-session")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === ExecuteCell ===

func TestSessionManager_ExecuteCell(t *testing.T) {
	t.Run("success with rows", func(t *testing.T) {
		sm, repo, _, engine, db := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			return &domain.Cell{ID: id, NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1 AS val"}, nil
		}

		var lastResultSaved *string
		repo.UpdateCellResultFn = func(_ context.Context, _ string, result *string) error {
			lastResultSaved = result
			return nil
		}

		// Engine delegates to the real conn, which is SQLite
		engine.queryOnConnFn = func(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
			return conn.QueryContext(ctx, sqlQuery)
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		result, err := sm.ExecuteCell(ctx, sess.ID, "cell-1")
		require.NoError(t, err)
		assert.Nil(t, result.Error)
		assert.Equal(t, "cell-1", result.CellID)
		assert.Equal(t, []string{"val"}, result.Columns)
		assert.Equal(t, 1, result.RowCount)
		assert.Greater(t, result.Duration, time.Duration(0))
		assert.NotNil(t, lastResultSaved, "result should be cached")

		_ = db // keep reference alive
	})

	t.Run("sql error is captured not returned", func(t *testing.T) {
		sm, repo, _, engine, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			return &domain.Cell{ID: id, NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "INVALID SQL"}, nil
		}

		engine.queryOnConnFn = func(_ context.Context, _ *sql.Conn, _ string, _ string) (*sql.Rows, error) {
			return nil, fmt.Errorf("near \"INVALID\": syntax error")
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		result, err := sm.ExecuteCell(ctx, sess.ID, "cell-1")
		require.NoError(t, err, "ExecuteCell should return nil error even when SQL fails")
		require.NotNil(t, result.Error)
		assert.Contains(t, *result.Error, "INVALID")
	})

	t.Run("non-SQL cell rejected", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			return &domain.Cell{ID: id, NotebookID: "nb-1", CellType: domain.CellTypeMarkdown, Content: "# Hello"}, nil
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		_, err = sm.ExecuteCell(ctx, sess.ID, "cell-md")
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})

	t.Run("session not found", func(t *testing.T) {
		sm, _, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		_, err := sm.ExecuteCell(ctx, "nonexistent-session", "cell-1")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("temp table persists across cells", func(t *testing.T) {
		sm, repo, _, engine, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		cellContent := ""
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			return &domain.Cell{ID: id, NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: cellContent}, nil
		}

		engine.queryOnConnFn = func(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
			return conn.QueryContext(ctx, sqlQuery)
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		// Cell 1: create temp table
		cellContent = "CREATE TEMPORARY TABLE tmp_test (x INTEGER)"
		result, err := sm.ExecuteCell(ctx, sess.ID, "cell-create")
		require.NoError(t, err)
		// CREATE TABLE returns no rows, which is fine — the important thing is no error
		assert.Nil(t, result.Error)

		// Cell 2: insert into temp table
		cellContent = "INSERT INTO tmp_test VALUES (42)"
		result, err = sm.ExecuteCell(ctx, sess.ID, "cell-insert")
		require.NoError(t, err)
		assert.Nil(t, result.Error)

		// Cell 3: query temp table
		cellContent = "SELECT x FROM tmp_test"
		result, err = sm.ExecuteCell(ctx, sess.ID, "cell-query")
		require.NoError(t, err)
		assert.Nil(t, result.Error)
		assert.Equal(t, 1, result.RowCount)
		assert.Equal(t, []string{"x"}, result.Columns)
	})
}

// === RunAll ===

func TestSessionManager_RunAll(t *testing.T) {
	t.Run("success with mixed cell types", func(t *testing.T) {
		sm, repo, _, engine, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.ListCellsFn = func(_ context.Context, _ string) ([]domain.Cell, error) {
			return []domain.Cell{
				{ID: "cell-1", NotebookID: "nb-1", CellType: domain.CellTypeMarkdown, Content: "# Title", Position: 0},
				{ID: "cell-2", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1 AS a", Position: 1},
				{ID: "cell-3", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 2 AS b", Position: 2},
			}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			cells := map[string]domain.Cell{
				"cell-2": {ID: "cell-2", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1 AS a"},
				"cell-3": {ID: "cell-3", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 2 AS b"},
			}
			c, ok := cells[id]
			if !ok {
				return nil, domain.ErrNotFound("cell %s not found", id)
			}
			return &c, nil
		}

		engine.queryOnConnFn = func(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
			return conn.QueryContext(ctx, sqlQuery)
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		result, err := sm.RunAll(ctx, sess.ID)
		require.NoError(t, err)
		assert.Equal(t, "nb-1", result.NotebookID)
		assert.Len(t, result.Results, 2, "should only execute SQL cells, skip markdown")
		assert.Nil(t, result.Results[0].Error)
		assert.Nil(t, result.Results[1].Error)
		assert.Greater(t, result.TotalDuration, time.Duration(0))
	})

	t.Run("stops on first error", func(t *testing.T) {
		sm, repo, _, engine, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.ListCellsFn = func(_ context.Context, _ string) ([]domain.Cell, error) {
			return []domain.Cell{
				{ID: "cell-1", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "BAD SQL", Position: 0},
				{ID: "cell-2", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1", Position: 1},
			}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			cells := map[string]domain.Cell{
				"cell-1": {ID: "cell-1", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "BAD SQL"},
				"cell-2": {ID: "cell-2", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1"},
			}
			c := cells[id]
			return &c, nil
		}

		callCount := 0
		engine.queryOnConnFn = func(_ context.Context, _ *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
			callCount++
			if sqlQuery == "BAD SQL" {
				return nil, fmt.Errorf("syntax error")
			}
			return nil, fmt.Errorf("should not reach here")
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		result, err := sm.RunAll(ctx, sess.ID)
		require.NoError(t, err)
		assert.Len(t, result.Results, 1, "should stop after first error")
		require.NotNil(t, result.Results[0].Error)
		assert.Contains(t, *result.Results[0].Error, "syntax error")
		assert.Equal(t, 1, callCount, "engine should only be called once")
	})

	t.Run("session not found", func(t *testing.T) {
		sm, _, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		_, err := sm.RunAll(ctx, "nonexistent-session")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === RunAllAsync ===

func TestSessionManager_RunAllAsync(t *testing.T) {
	t.Run("creates job and executes in background", func(t *testing.T) {
		sm, repo, jobRepo, engine, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.ListCellsFn = func(_ context.Context, _ string) ([]domain.Cell, error) {
			return []domain.Cell{
				{ID: "cell-1", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1 AS val"},
			}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			return &domain.Cell{ID: "cell-1", NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1 AS val"}, nil
		}

		engine.queryOnConnFn = func(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
			return conn.QueryContext(ctx, sqlQuery)
		}

		jobRepo.CreateJobFn = func(_ context.Context, job *domain.NotebookJob) (*domain.NotebookJob, error) {
			job.CreatedAt = time.Now()
			return job, nil
		}

		var wg sync.WaitGroup
		wg.Add(1)

		var finalState domain.JobState
		jobRepo.UpdateJobStateFn = func(_ context.Context, _ string, state domain.JobState, result *string, errMsg *string) error {
			finalState = state
			if state == domain.JobStateComplete || state == domain.JobStateFailed {
				wg.Done()
			}
			return nil
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		job, err := sm.RunAllAsync(ctx, sess.ID)
		require.NoError(t, err)
		assert.NotEmpty(t, job.ID)
		assert.Equal(t, domain.JobStatePending, job.State)

		wg.Wait()
		assert.Equal(t, domain.JobStateComplete, finalState)
	})

	t.Run("session not found", func(t *testing.T) {
		sm, _, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		_, err := sm.RunAllAsync(ctx, "nonexistent-session")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

// === ReapIdle ===

func TestSessionManager_ReapIdle(t *testing.T) {
	t.Run("reaps expired sessions", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		// Manually set the session's lastUsed to be well past the TTL
		sm.mu.Lock()
		s := sm.sessions[sess.ID]
		s.lastUsed = time.Now().Add(-2 * sm.ttl)
		sm.mu.Unlock()

		// Run one reap cycle
		sm.reapOnce()

		// Session should be gone
		err = sm.CloseSession(ctx, sess.ID)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("keeps active sessions", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		// Session was just created so it's fresh — reap should not touch it
		sm.reapOnce()

		// Session should still exist
		err = sm.CloseSession(ctx, sess.ID)
		require.NoError(t, err)
	})
}

// === CloseAll ===

func TestSessionManager_CloseAll(t *testing.T) {
	t.Run("closes all sessions", func(t *testing.T) {
		sm, repo, _, _, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}

		s1, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		s2, err := sm.CreateSession(ctx, "nb-1", "bob")
		require.NoError(t, err)

		sm.CloseAll()

		// Both sessions should be gone
		err = sm.CloseSession(ctx, s1.ID)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)

		err = sm.CloseSession(ctx, s2.ID)
		require.Error(t, err)
		assert.ErrorAs(t, err, &notFound)
	})
}

// === GetJob / ListJobs delegate ===

func TestSessionManager_JobDelegation(t *testing.T) {
	t.Run("GetJob delegates to repo", func(t *testing.T) {
		sm, _, jobRepo, _, _ := setupSessionManager(t)
		ctx := context.Background()

		jobRepo.GetJobFn = func(_ context.Context, id string) (*domain.NotebookJob, error) {
			return &domain.NotebookJob{ID: id, State: domain.JobStateRunning}, nil
		}

		job, err := sm.GetJob(ctx, "job-1")
		require.NoError(t, err)
		assert.Equal(t, "job-1", job.ID)
		assert.Equal(t, domain.JobStateRunning, job.State)
	})

	t.Run("ListJobs delegates to repo", func(t *testing.T) {
		sm, _, jobRepo, _, _ := setupSessionManager(t)
		ctx := context.Background()

		jobRepo.ListJobsFn = func(_ context.Context, notebookID string, _ domain.PageRequest) ([]domain.NotebookJob, int64, error) {
			return []domain.NotebookJob{{ID: "job-1", NotebookID: notebookID}}, 1, nil
		}

		jobs, total, err := sm.ListJobs(ctx, "nb-1", domain.PageRequest{})
		require.NoError(t, err)
		assert.Len(t, jobs, 1)
		assert.Equal(t, int64(1), total)
	})
}

// === Concurrent access ===

func TestSessionManager_ConcurrentExecute(t *testing.T) {
	t.Run("concurrent ExecuteCell on same session serializes safely", func(t *testing.T) {
		sm, repo, _, engine, _ := setupSessionManager(t)
		ctx := context.Background()

		repo.GetNotebookFn = func(_ context.Context, id string) (*domain.Notebook, error) {
			return &domain.Notebook{ID: id, Name: "NB", Owner: "alice"}, nil
		}
		repo.GetCellFn = func(_ context.Context, id string) (*domain.Cell, error) {
			return &domain.Cell{ID: id, NotebookID: "nb-1", CellType: domain.CellTypeSQL, Content: "SELECT 1"}, nil
		}

		engine.queryOnConnFn = func(ctx context.Context, conn *sql.Conn, _ string, sqlQuery string) (*sql.Rows, error) {
			return conn.QueryContext(ctx, sqlQuery)
		}

		sess, err := sm.CreateSession(ctx, "nb-1", "alice")
		require.NoError(t, err)

		const numGoroutines = 10
		var wg sync.WaitGroup
		errors := make([]error, numGoroutines)
		results := make([]*domain.CellExecutionResult, numGoroutines)

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(idx int) {
				defer wg.Done()
				results[idx], errors[idx] = sm.ExecuteCell(ctx, sess.ID, fmt.Sprintf("cell-%d", idx))
			}(i)
		}

		wg.Wait()

		for i, err := range errors {
			require.NoError(t, err, "goroutine %d failed", i)
			assert.Nil(t, results[i].Error, "goroutine %d had SQL error", i)
		}
	})
}
