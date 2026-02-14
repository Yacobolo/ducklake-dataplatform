package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

// mockReloader implements ScheduleReloader for tests.
type mockReloader struct {
	called bool
}

func (m *mockReloader) Reload(ctx context.Context) error {
	m.called = true
	return nil
}

// newTestService creates a Service with the given mocks and a discard logger.
func newTestService(
	pipelines *testutil.MockPipelineRepo,
	runs *testutil.MockPipelineRunRepo,
	audit *testutil.MockAuditRepo,
	notebooks *testutil.MockNotebookProvider,
) *Service {
	logger := slog.New(slog.DiscardHandler)
	return NewService(pipelines, runs, audit, notebooks, nil, nil, logger)
}

// === CreatePipeline ===

func TestPipelineService_CreatePipeline(t *testing.T) {
	tests := []struct {
		name            string
		principal       string
		req             domain.CreatePipelineRequest
		setupRepo       func(*testutil.MockPipelineRepo)
		wantErr         bool
		errType         interface{}
		wantConcurrency int
		wantAudit       string
		wantReloader    bool
	}{
		{
			name:      "validation_error_empty_name",
			principal: "alice",
			req:       domain.CreatePipelineRequest{Name: ""},
			wantErr:   true,
			errType:   new(*domain.ValidationError),
		},
		{
			name:      "happy_path_default_concurrency",
			principal: "alice",
			req: domain.CreatePipelineRequest{
				Name:        "etl-daily",
				Description: "Daily ETL pipeline",
			},
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.CreatePipelineFn = func(ctx context.Context, p *domain.Pipeline) (*domain.Pipeline, error) {
					return p, nil
				}
			},
			wantConcurrency: 1,
			wantAudit:       "pipeline.create",
			wantReloader:    true,
		},
		{
			name:      "happy_path_explicit_concurrency",
			principal: "bob",
			req: domain.CreatePipelineRequest{
				Name:             "etl-hourly",
				ConcurrencyLimit: 5,
			},
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.CreatePipelineFn = func(ctx context.Context, p *domain.Pipeline) (*domain.Pipeline, error) {
					return p, nil
				}
			},
			wantConcurrency: 5,
			wantAudit:       "pipeline.create",
			wantReloader:    true,
		},
		{
			name:      "repo_error",
			principal: "alice",
			req: domain.CreatePipelineRequest{
				Name: "etl-failing",
			},
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.CreatePipelineFn = func(ctx context.Context, p *domain.Pipeline) (*domain.Pipeline, error) {
					return nil, errors.New("db error")
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			runRepo := &testutil.MockPipelineRunRepo{}
			auditRepo := &testutil.MockAuditRepo{}
			nbProvider := &testutil.MockNotebookProvider{}
			reloader := &mockReloader{}

			if tt.setupRepo != nil {
				tt.setupRepo(pipeRepo)
			}

			svc := newTestService(pipeRepo, runRepo, auditRepo, nbProvider)
			svc.SetScheduleReloader(reloader)

			result, err := svc.CreatePipeline(context.Background(), tt.principal, tt.req)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.req.Name, result.Name)
			assert.Equal(t, tt.wantConcurrency, result.ConcurrencyLimit)
			assert.Equal(t, tt.principal, result.CreatedBy)

			if tt.wantAudit != "" {
				assert.True(t, auditRepo.HasAction(tt.wantAudit), "expected audit action %q", tt.wantAudit)
			}

			assert.Equal(t, tt.wantReloader, reloader.called)
		})
	}
}

// === GetPipeline ===

func TestPipelineService_GetPipeline(t *testing.T) {
	tests := []struct {
		name      string
		pipeName  string
		setupRepo func(*testutil.MockPipelineRepo)
		wantErr   bool
		errType   interface{}
	}{
		{
			name:     "happy_path",
			pipeName: "my-pipeline",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
			},
		},
		{
			name:     "not_found",
			pipeName: "nonexistent",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(pipeRepo)
			}

			svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

			result, err := svc.GetPipeline(context.Background(), tt.pipeName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.pipeName, result.Name)
		})
	}
}

// === ListPipelines ===

func TestPipelineService_ListPipelines(t *testing.T) {
	pipeRepo := &testutil.MockPipelineRepo{
		ListPipelinesFn: func(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error) {
			return []domain.Pipeline{
				{ID: "p1", Name: "pipeline-a"},
				{ID: "p2", Name: "pipeline-b"},
			}, 2, nil
		},
	}

	svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

	pipelines, total, err := svc.ListPipelines(context.Background(), domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, pipelines, 2)
}

// === UpdatePipeline ===

func TestPipelineService_UpdatePipeline(t *testing.T) {
	desc := "updated description"

	tests := []struct {
		name      string
		pipeName  string
		principal string
		req       domain.UpdatePipelineRequest
		setupRepo func(*testutil.MockPipelineRepo)
		wantErr   bool
		errType   interface{}
	}{
		{
			name:      "pipeline_not_found",
			pipeName:  "nonexistent",
			principal: "alice",
			req:       domain.UpdatePipelineRequest{Description: &desc},
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:      "happy_path",
			pipeName:  "etl-daily",
			principal: "alice",
			req:       domain.UpdatePipelineRequest{Description: &desc},
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
				repo.UpdatePipelineFn = func(ctx context.Context, id string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: id, Name: "etl-daily", Description: *req.Description}, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			auditRepo := &testutil.MockAuditRepo{}
			reloader := &mockReloader{}

			if tt.setupRepo != nil {
				tt.setupRepo(pipeRepo)
			}

			svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, auditRepo, &testutil.MockNotebookProvider{})
			svc.SetScheduleReloader(reloader)

			result, err := svc.UpdatePipeline(context.Background(), tt.principal, tt.pipeName, tt.req)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, desc, result.Description)
			assert.True(t, auditRepo.HasAction("pipeline.update"))
			assert.True(t, reloader.called)
		})
	}
}

// === DeletePipeline ===

func TestPipelineService_DeletePipeline(t *testing.T) {
	tests := []struct {
		name      string
		pipeName  string
		principal string
		setupRepo func(*testutil.MockPipelineRepo)
		wantErr   bool
		errType   interface{}
	}{
		{
			name:      "pipeline_not_found",
			pipeName:  "nonexistent",
			principal: "alice",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:      "happy_path",
			pipeName:  "etl-daily",
			principal: "alice",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
				repo.DeletePipelineFn = func(ctx context.Context, id string) error {
					return nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			auditRepo := &testutil.MockAuditRepo{}
			reloader := &mockReloader{}

			if tt.setupRepo != nil {
				tt.setupRepo(pipeRepo)
			}

			svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, auditRepo, &testutil.MockNotebookProvider{})
			svc.SetScheduleReloader(reloader)

			err := svc.DeletePipeline(context.Background(), tt.principal, tt.pipeName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			assert.True(t, auditRepo.HasAction("pipeline.delete"))
			assert.True(t, reloader.called)
		})
	}
}

// === CreateJob ===

func TestPipelineService_CreateJob(t *testing.T) {
	tests := []struct {
		name      string
		pipeName  string
		principal string
		req       domain.CreatePipelineJobRequest
		setupPipe func(*testutil.MockPipelineRepo)
		setupNB   func(*testutil.MockNotebookProvider)
		wantErr   bool
		errType   interface{}
	}{
		{
			name:      "validation_error_empty_name",
			pipeName:  "my-pipe",
			principal: "alice",
			req:       domain.CreatePipelineJobRequest{Name: "", NotebookID: "nb1"},
			wantErr:   true,
			errType:   new(*domain.ValidationError),
		},
		{
			name:      "validation_error_empty_notebook_id",
			pipeName:  "my-pipe",
			principal: "alice",
			req:       domain.CreatePipelineJobRequest{Name: "extract", NotebookID: ""},
			wantErr:   true,
			errType:   new(*domain.ValidationError),
		},
		{
			name:      "pipeline_not_found",
			pipeName:  "nonexistent",
			principal: "alice",
			req:       domain.CreatePipelineJobRequest{Name: "extract", NotebookID: "nb1"},
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:      "notebook_validation_fails",
			pipeName:  "my-pipe",
			principal: "alice",
			req:       domain.CreatePipelineJobRequest{Name: "extract", NotebookID: "bad-nb"},
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
			},
			setupNB: func(nb *testutil.MockNotebookProvider) {
				nb.GetSQLBlocksFn = func(ctx context.Context, notebookID string) ([]string, error) {
					return nil, errors.New("notebook not found")
				}
			},
			wantErr: true,
		},
		{
			name:      "happy_path",
			pipeName:  "my-pipe",
			principal: "alice",
			req:       domain.CreatePipelineJobRequest{Name: "extract", NotebookID: "nb1", JobOrder: 1},
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
				repo.CreateJobFn = func(ctx context.Context, job *domain.PipelineJob) (*domain.PipelineJob, error) {
					return job, nil
				}
			},
			setupNB: func(nb *testutil.MockNotebookProvider) {
				nb.GetSQLBlocksFn = func(ctx context.Context, notebookID string) ([]string, error) {
					return []string{"SELECT 1"}, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			nbProvider := &testutil.MockNotebookProvider{}

			if tt.setupPipe != nil {
				tt.setupPipe(pipeRepo)
			}
			if tt.setupNB != nil {
				tt.setupNB(nbProvider)
			}

			svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, nbProvider)

			result, err := svc.CreateJob(context.Background(), tt.principal, tt.pipeName, tt.req)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.req.Name, result.Name)
			assert.Equal(t, "p1", result.PipelineID)
			assert.Equal(t, tt.req.NotebookID, result.NotebookID)
		})
	}
}

// === ListJobs ===

func TestPipelineService_ListJobs(t *testing.T) {
	tests := []struct {
		name      string
		pipeName  string
		setupRepo func(*testutil.MockPipelineRepo)
		wantErr   bool
		wantCount int
	}{
		{
			name:     "pipeline_not_found",
			pipeName: "nonexistent",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
		},
		{
			name:     "happy_path",
			pipeName: "my-pipe",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
				repo.ListJobsByPipelineFn = func(ctx context.Context, pipelineID string) ([]domain.PipelineJob, error) {
					return []domain.PipelineJob{
						{ID: "j1", Name: "extract", PipelineID: pipelineID},
						{ID: "j2", Name: "load", PipelineID: pipelineID},
					}, nil
				}
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(pipeRepo)
			}

			svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

			jobs, err := svc.ListJobs(context.Background(), tt.pipeName)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, jobs, tt.wantCount)
		})
	}
}

// === DeleteJob ===

func TestPipelineService_DeleteJob(t *testing.T) {
	tests := []struct {
		name      string
		pipeName  string
		jobID     string
		principal string
		setupRepo func(*testutil.MockPipelineRepo)
		wantErr   bool
		errType   interface{}
	}{
		{
			name:      "job_not_found",
			pipeName:  "my-pipe",
			jobID:     "nonexistent",
			principal: "alice",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetJobByIDFn = func(ctx context.Context, id string) (*domain.PipelineJob, error) {
					return nil, domain.ErrNotFound("job %s not found", id)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:      "happy_path",
			pipeName:  "my-pipe",
			jobID:     "j1",
			principal: "alice",
			setupRepo: func(repo *testutil.MockPipelineRepo) {
				repo.GetJobByIDFn = func(ctx context.Context, id string) (*domain.PipelineJob, error) {
					return &domain.PipelineJob{ID: id, PipelineID: "p1", Name: "extract"}, nil
				}
				repo.DeleteJobFn = func(ctx context.Context, id string) error {
					return nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(pipeRepo)
			}

			svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

			err := svc.DeleteJob(context.Background(), tt.principal, tt.pipeName, tt.jobID)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
		})
	}
}

// === TriggerRun ===

func TestPipelineService_TriggerRun(t *testing.T) {
	tests := []struct {
		name        string
		pipeName    string
		principal   string
		params      map[string]string
		triggerType string
		setupPipe   func(*testutil.MockPipelineRepo)
		setupRun    func(*testutil.MockPipelineRunRepo)
		wantErr     bool
		errType     interface{}
		errContains string
	}{
		{
			name:        "pipeline_not_found",
			pipeName:    "nonexistent",
			principal:   "alice",
			triggerType: domain.TriggerTypeManual,
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:        "concurrency_limit_reached",
			pipeName:    "my-pipe",
			principal:   "alice",
			triggerType: domain.TriggerTypeManual,
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name, ConcurrencyLimit: 1}, nil
				}
			},
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.CountActiveRunsFn = func(ctx context.Context, pipelineID string) (int64, error) {
					return 1, nil
				}
			},
			wantErr:     true,
			errType:     new(*domain.ValidationError),
			errContains: "concurrency limit reached",
		},
		{
			name:        "no_jobs",
			pipeName:    "my-pipe",
			principal:   "alice",
			triggerType: domain.TriggerTypeManual,
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name, ConcurrencyLimit: 1}, nil
				}
				repo.ListJobsByPipelineFn = func(ctx context.Context, pipelineID string) ([]domain.PipelineJob, error) {
					return nil, nil
				}
			},
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.CountActiveRunsFn = func(ctx context.Context, pipelineID string) (int64, error) {
					return 0, nil
				}
			},
			wantErr:     true,
			errType:     new(*domain.ValidationError),
			errContains: "pipeline has no jobs",
		},
		{
			name:        "happy_path_creates_run_and_job_runs",
			pipeName:    "etl-daily",
			principal:   "alice",
			params:      map[string]string{"date": "2026-01-01"},
			triggerType: domain.TriggerTypeManual,
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name, ConcurrencyLimit: 2}, nil
				}
				repo.ListJobsByPipelineFn = func(ctx context.Context, pipelineID string) ([]domain.PipelineJob, error) {
					return []domain.PipelineJob{
						{ID: "j1", PipelineID: pipelineID, Name: "extract"},
						{ID: "j2", PipelineID: pipelineID, Name: "load", DependsOn: []string{"extract"}},
					}, nil
				}
			},
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.CountActiveRunsFn = func(ctx context.Context, pipelineID string) (int64, error) {
					return 0, nil
				}
				repo.CreateRunFn = func(ctx context.Context, run *domain.PipelineRun) (*domain.PipelineRun, error) {
					run.CreatedAt = time.Now()
					return run, nil
				}
				repo.CreateJobRunFn = func(ctx context.Context, jr *domain.PipelineJobRun) (*domain.PipelineJobRun, error) {
					jr.CreatedAt = time.Now()
					return jr, nil
				}

				// Background goroutine mocks — prevent panics.
				repo.UpdateRunStartedFn = func(ctx context.Context, id string) error { return nil }
				repo.ListJobRunsByRunFn = func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
					return nil, nil
				}
				repo.UpdateRunFinishedFn = func(ctx context.Context, id string, status string, errMsg *string) error {
					return nil
				}
				repo.UpdateJobRunFinishedFn = func(ctx context.Context, id string, status string, errMsg *string) error {
					return nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			runRepo := &testutil.MockPipelineRunRepo{}
			auditRepo := &testutil.MockAuditRepo{}
			nbProvider := &testutil.MockNotebookProvider{}

			if tt.setupPipe != nil {
				tt.setupPipe(pipeRepo)
			}
			if tt.setupRun != nil {
				tt.setupRun(runRepo)
			}

			svc := newTestService(pipeRepo, runRepo, auditRepo, nbProvider)

			result, err := svc.TriggerRun(context.Background(), tt.principal, tt.pipeName, tt.params, tt.triggerType)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, result)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, "p1", result.PipelineID)
			assert.Equal(t, domain.PipelineRunStatusPending, result.Status)
			assert.Equal(t, tt.triggerType, result.TriggerType)
			assert.Equal(t, tt.principal, result.TriggeredBy)
			assert.Equal(t, tt.params, result.Parameters)

			assert.True(t, auditRepo.HasAction("pipeline.trigger"))

			// Give the background goroutine a moment to complete without blocking.
			runtime.Gosched()
		})
	}
}

// === ListRuns ===

func TestPipelineService_ListRuns(t *testing.T) {
	tests := []struct {
		name      string
		pipeName  string
		filter    domain.PipelineRunFilter
		setupPipe func(*testutil.MockPipelineRepo)
		setupRun  func(*testutil.MockPipelineRunRepo)
		wantErr   bool
		wantCount int
		wantTotal int64
	}{
		{
			name:     "pipeline_not_found",
			pipeName: "nonexistent",
			filter:   domain.PipelineRunFilter{},
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return nil, domain.ErrNotFound("pipeline %s not found", name)
				}
			},
			wantErr: true,
		},
		{
			name:     "happy_path",
			pipeName: "my-pipe",
			filter:   domain.PipelineRunFilter{},
			setupPipe: func(repo *testutil.MockPipelineRepo) {
				repo.GetPipelineByNameFn = func(ctx context.Context, name string) (*domain.Pipeline, error) {
					return &domain.Pipeline{ID: "p1", Name: name}, nil
				}
			},
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.ListRunsFn = func(ctx context.Context, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
					require.NotNil(t, filter.PipelineID)
					assert.Equal(t, "p1", *filter.PipelineID)
					return []domain.PipelineRun{
						{ID: "r1", PipelineID: "p1", Status: domain.PipelineRunStatusSuccess},
					}, 1, nil
				}
			},
			wantCount: 1,
			wantTotal: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeRepo := &testutil.MockPipelineRepo{}
			runRepo := &testutil.MockPipelineRunRepo{}

			if tt.setupPipe != nil {
				tt.setupPipe(pipeRepo)
			}
			if tt.setupRun != nil {
				tt.setupRun(runRepo)
			}

			svc := newTestService(pipeRepo, runRepo, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

			runs, total, err := svc.ListRuns(context.Background(), tt.pipeName, tt.filter)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, runs, tt.wantCount)
			assert.Equal(t, tt.wantTotal, total)
		})
	}
}

// === ListJobRuns ===

func TestPipelineService_ListJobRuns(t *testing.T) {
	tests := []struct {
		name      string
		runID     string
		setupRun  func(*testutil.MockPipelineRunRepo)
		wantErr   bool
		errType   interface{}
		wantCount int
	}{
		{
			name:  "run_not_found",
			runID: "nonexistent",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return nil, domain.ErrNotFound("run %s not found", id)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:  "happy_path",
			runID: "r1",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{ID: id, PipelineID: "p1"}, nil
				}
				repo.ListJobRunsByRunFn = func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
					return []domain.PipelineJobRun{
						{ID: "jr1", RunID: runID, JobName: "extract", Status: domain.PipelineJobRunStatusSuccess},
						{ID: "jr2", RunID: runID, JobName: "load", Status: domain.PipelineJobRunStatusPending},
					}, nil
				}
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runRepo := &testutil.MockPipelineRunRepo{}
			if tt.setupRun != nil {
				tt.setupRun(runRepo)
			}

			svc := newTestService(&testutil.MockPipelineRepo{}, runRepo, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

			jobRuns, err := svc.ListJobRuns(context.Background(), tt.runID)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			assert.Len(t, jobRuns, tt.wantCount)
		})
	}
}

// === CancelRun ===

func TestPipelineService_CancelRun(t *testing.T) {
	tests := []struct {
		name               string
		runID              string
		principal          string
		setupRun           func(*testutil.MockPipelineRunRepo)
		wantErr            bool
		errType            interface{}
		errContains        string
		wantAudit          string
		wantFinishedCalled bool
	}{
		{
			name:      "run_not_found",
			runID:     "nonexistent",
			principal: "alice",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return nil, domain.ErrNotFound("run %s not found", id)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
		{
			name:      "cannot_cancel_success_run",
			runID:     "r1",
			principal: "alice",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{
						ID:         id,
						PipelineID: "p1",
						Status:     domain.PipelineRunStatusSuccess,
					}, nil
				}
			},
			wantErr:     true,
			errType:     new(*domain.ValidationError),
			errContains: "cannot cancel run with status",
		},
		{
			name:      "cannot_cancel_failed_run",
			runID:     "r1",
			principal: "alice",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{
						ID:         id,
						PipelineID: "p1",
						Status:     domain.PipelineRunStatusFailed,
					}, nil
				}
			},
			wantErr:     true,
			errType:     new(*domain.ValidationError),
			errContains: "cannot cancel run with status",
		},
		{
			name:      "cannot_cancel_already_cancelled",
			runID:     "r1",
			principal: "alice",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{
						ID:         id,
						PipelineID: "p1",
						Status:     domain.PipelineRunStatusCancelled,
					}, nil
				}
			},
			wantErr:     true,
			errType:     new(*domain.ValidationError),
			errContains: "cannot cancel run with status",
		},
		{
			name:      "happy_path_cancel_pending_run",
			runID:     "r1",
			principal: "alice",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{
						ID:         id,
						PipelineID: "p1",
						Status:     domain.PipelineRunStatusPending,
					}, nil
				}
				repo.UpdateRunFinishedFn = func(ctx context.Context, id string, status string, errMsg *string) error {
					assert.Equal(t, domain.PipelineRunStatusCancelled, status)
					assert.NotNil(t, errMsg)
					assert.Contains(t, *errMsg, "cancelled by alice")
					return nil
				}
				repo.ListJobRunsByRunFn = func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
					return []domain.PipelineJobRun{
						{ID: "jr1", RunID: runID, Status: domain.PipelineJobRunStatusPending},
						{ID: "jr2", RunID: runID, Status: domain.PipelineJobRunStatusRunning},
					}, nil
				}
				repo.UpdateJobRunFinishedFn = func(ctx context.Context, id string, status string, errMsg *string) error {
					// Only PENDING job runs should be cancelled.
					assert.Equal(t, "jr1", id)
					assert.Equal(t, domain.PipelineJobRunStatusCancelled, status)
					return nil
				}
			},
			wantAudit:          "pipeline.cancel",
			wantFinishedCalled: true,
		},
		{
			name:      "happy_path_cancel_running_run",
			runID:     "r2",
			principal: "bob",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{
						ID:         id,
						PipelineID: "p1",
						Status:     domain.PipelineRunStatusRunning,
					}, nil
				}
				repo.UpdateRunFinishedFn = func(ctx context.Context, id string, status string, errMsg *string) error {
					return nil
				}
				repo.ListJobRunsByRunFn = func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
					return nil, nil // no pending job runs
				}
			},
			wantAudit:          "pipeline.cancel",
			wantFinishedCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runRepo := &testutil.MockPipelineRunRepo{}
			auditRepo := &testutil.MockAuditRepo{}

			if tt.setupRun != nil {
				tt.setupRun(runRepo)
			}

			svc := newTestService(&testutil.MockPipelineRepo{}, runRepo, auditRepo, &testutil.MockNotebookProvider{})

			err := svc.CancelRun(context.Background(), tt.principal, tt.runID)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			if tt.wantAudit != "" {
				assert.True(t, auditRepo.HasAction(tt.wantAudit), "expected audit action %q", tt.wantAudit)
			}
		})
	}
}

// === GetRun ===

func TestPipelineService_GetRun(t *testing.T) {
	tests := []struct {
		name     string
		runID    string
		setupRun func(*testutil.MockPipelineRunRepo)
		wantErr  bool
		errType  interface{}
	}{
		{
			name:  "happy_path",
			runID: "r1",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return &domain.PipelineRun{ID: id, PipelineID: "p1", Status: domain.PipelineRunStatusRunning}, nil
				}
			},
		},
		{
			name:  "not_found",
			runID: "nonexistent",
			setupRun: func(repo *testutil.MockPipelineRunRepo) {
				repo.GetRunByIDFn = func(ctx context.Context, id string) (*domain.PipelineRun, error) {
					return nil, domain.ErrNotFound("run %s not found", id)
				}
			},
			wantErr: true,
			errType: new(*domain.NotFoundError),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runRepo := &testutil.MockPipelineRunRepo{}
			if tt.setupRun != nil {
				tt.setupRun(runRepo)
			}

			svc := newTestService(&testutil.MockPipelineRepo{}, runRepo, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

			result, err := svc.GetRun(context.Background(), tt.runID)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					require.ErrorAs(t, err, tt.errType)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.runID, result.ID)
		})
	}
}

// === SetScheduleReloader ===

func TestPipelineService_SetScheduleReloader(t *testing.T) {
	svc := newTestService(&testutil.MockPipelineRepo{}, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})

	assert.Nil(t, svc.reloader)

	reloader := &mockReloader{}
	svc.SetScheduleReloader(reloader)

	assert.NotNil(t, svc.reloader)
}

// === CreatePipeline without reloader ===

func TestPipelineService_CreatePipeline_NoReloader(t *testing.T) {
	pipeRepo := &testutil.MockPipelineRepo{
		CreatePipelineFn: func(ctx context.Context, p *domain.Pipeline) (*domain.Pipeline, error) {
			return p, nil
		},
	}

	svc := newTestService(pipeRepo, &testutil.MockPipelineRunRepo{}, &testutil.MockAuditRepo{}, &testutil.MockNotebookProvider{})
	// Intentionally do not set reloader.

	result, err := svc.CreatePipeline(context.Background(), "alice", domain.CreatePipelineRequest{
		Name: "test-pipe",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "test-pipe", result.Name)
}
