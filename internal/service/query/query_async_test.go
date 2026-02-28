package query

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"

	_ "github.com/duckdb/duckdb-go/v2"
)

type memQueryJobRepo struct {
	mu   sync.Mutex
	jobs map[string]*domain.QueryJob
}

func newMemQueryJobRepo() *memQueryJobRepo {
	return &memQueryJobRepo{jobs: make(map[string]*domain.QueryJob)}
}

func (r *memQueryJobRepo) Create(_ context.Context, job *domain.QueryJob) (*domain.QueryJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	copyJob := *job
	r.jobs[job.ID] = &copyJob
	return &copyJob, nil
}

func (r *memQueryJobRepo) GetByID(_ context.Context, id string) (*domain.QueryJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return nil, domain.ErrNotFound("job not found")
	}
	copyJob := *job
	return &copyJob, nil
}

func (r *memQueryJobRepo) GetByRequestID(_ context.Context, principalName, requestID string) (*domain.QueryJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, job := range r.jobs {
		if job.PrincipalName == principalName && job.RequestID == requestID {
			copyJob := *job
			return &copyJob, nil
		}
	}
	return nil, domain.ErrNotFound("job not found")
}

func (r *memQueryJobRepo) MarkRunning(_ context.Context, id string, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[id].Status = domain.QueryJobStatusRunning
	r.jobs[id].AttemptCount = attempt
	return nil
}

func (r *memQueryJobRepo) MarkRetrying(_ context.Context, id string, attempt int, nextRetryAt time.Time, message string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job := r.jobs[id]
	job.Status = domain.QueryJobStatusQueued
	job.AttemptCount = attempt
	job.NextRetryAt = &nextRetryAt
	job.ErrorMessage = &message
	return nil
}

func (r *memQueryJobRepo) Heartbeat(_ context.Context, id string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job := r.jobs[id]
	job.LastHeartbeat = &at
	return nil
}

func (r *memQueryJobRepo) MarkSucceeded(_ context.Context, id string, columns []string, rows [][]interface{}, rowCount int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job := r.jobs[id]
	job.Status = domain.QueryJobStatusSucceeded
	job.Columns = columns
	job.Rows = rows
	job.RowCount = rowCount
	return nil
}

func (r *memQueryJobRepo) MarkFailed(_ context.Context, id string, message string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job := r.jobs[id]
	job.Status = domain.QueryJobStatusFailed
	job.ErrorMessage = &message
	return nil
}

func (r *memQueryJobRepo) MarkCanceled(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	job := r.jobs[id]
	job.Status = domain.QueryJobStatusCanceled
	return nil
}

func (r *memQueryJobRepo) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, id)
	return nil
}

func TestQueryService_SubmitAsync_IdempotentByRequestID(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	eng := &testutil.MockSessionEngine{QueryFn: func(ctx context.Context, _ string, q string) (*sql.Rows, error) {
		return db.QueryContext(ctx, q)
	}}

	repo := newMemQueryJobRepo()
	svc := NewQueryService(eng, &testutil.MockAuditRepo{}, nil)
	svc.SetJobRepository(repo)

	first, err := svc.SubmitAsync(context.Background(), "alice", "SELECT 1 AS id", "request-1")
	require.NoError(t, err)
	second, err := svc.SubmitAsync(context.Background(), "alice", "SELECT 1 AS id", "request-1")
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID)
}

func TestQueryService_SubmitAsync_CompletesAndStoresRows(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	eng := &testutil.MockSessionEngine{QueryFn: func(ctx context.Context, _ string, q string) (*sql.Rows, error) {
		return db.QueryContext(ctx, q)
	}}
	repo := newMemQueryJobRepo()
	svc := NewQueryService(eng, &testutil.MockAuditRepo{}, nil)
	svc.SetJobRepository(repo)

	job, err := svc.SubmitAsync(context.Background(), "alice", "SELECT 1 AS id UNION ALL SELECT 2 AS id", "request-2")
	require.NoError(t, err)

	deadline := time.Now().Add(2 * time.Second)
	for {
		current, getErr := svc.GetAsyncJob(context.Background(), "alice", job.ID)
		require.NoError(t, getErr)
		if current.Status == domain.QueryJobStatusSucceeded {
			assert.Equal(t, 2, current.RowCount)
			require.Len(t, current.Rows, 2)
			break
		}
		require.True(t, time.Now().Before(deadline), "job did not complete in time")
		time.Sleep(20 * time.Millisecond)
	}
}

func TestQueryService_SubmitAsync_Disabled(t *testing.T) {
	t.Parallel()

	svc := NewQueryService(&testutil.MockSessionEngine{}, &testutil.MockAuditRepo{}, nil)
	svc.SetJobRepository(newMemQueryJobRepo())
	svc.SetAsyncEnabled(false)

	_, err := svc.SubmitAsync(context.Background(), "alice", "SELECT 1", "req-disabled")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disabled")
}
