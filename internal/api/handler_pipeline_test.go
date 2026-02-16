package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mock ===

type mockPipelineService struct {
	createPipelineFn func(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error)
	getPipelineFn    func(ctx context.Context, name string) (*domain.Pipeline, error)
	listPipelinesFn  func(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error)
	updatePipelineFn func(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error)
	deletePipelineFn func(ctx context.Context, principal string, name string) error
	createJobFn      func(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error)
	listJobsFn       func(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error)
	deleteJobFn      func(ctx context.Context, principal string, pipelineName string, jobID string) error
	triggerRunFn     func(ctx context.Context, principal string, pipelineName string, params map[string]string, triggerType string) (*domain.PipelineRun, error)
	listRunsFn       func(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error)
	getRunFn         func(ctx context.Context, runID string) (*domain.PipelineRun, error)
	cancelRunFn      func(ctx context.Context, principal string, runID string) error
	listJobRunsFn    func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error)
}

func (m *mockPipelineService) CreatePipeline(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error) {
	if m.createPipelineFn == nil {
		panic("mockPipelineService.CreatePipeline called but not configured")
	}
	return m.createPipelineFn(ctx, principal, req)
}

func (m *mockPipelineService) GetPipeline(ctx context.Context, name string) (*domain.Pipeline, error) {
	if m.getPipelineFn == nil {
		panic("mockPipelineService.GetPipeline called but not configured")
	}
	return m.getPipelineFn(ctx, name)
}

func (m *mockPipelineService) ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error) {
	if m.listPipelinesFn == nil {
		panic("mockPipelineService.ListPipelines called but not configured")
	}
	return m.listPipelinesFn(ctx, page)
}

func (m *mockPipelineService) UpdatePipeline(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
	if m.updatePipelineFn == nil {
		panic("mockPipelineService.UpdatePipeline called but not configured")
	}
	return m.updatePipelineFn(ctx, principal, name, req)
}

func (m *mockPipelineService) DeletePipeline(ctx context.Context, principal string, name string) error {
	if m.deletePipelineFn == nil {
		panic("mockPipelineService.DeletePipeline called but not configured")
	}
	return m.deletePipelineFn(ctx, principal, name)
}

func (m *mockPipelineService) CreateJob(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
	if m.createJobFn == nil {
		panic("mockPipelineService.CreateJob called but not configured")
	}
	return m.createJobFn(ctx, principal, pipelineName, req)
}

func (m *mockPipelineService) ListJobs(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error) {
	if m.listJobsFn == nil {
		panic("mockPipelineService.ListJobs called but not configured")
	}
	return m.listJobsFn(ctx, pipelineName)
}

func (m *mockPipelineService) DeleteJob(ctx context.Context, principal string, pipelineName string, jobID string) error {
	if m.deleteJobFn == nil {
		panic("mockPipelineService.DeleteJob called but not configured")
	}
	return m.deleteJobFn(ctx, principal, pipelineName, jobID)
}

func (m *mockPipelineService) TriggerRun(ctx context.Context, principal string, pipelineName string, params map[string]string, triggerType string) (*domain.PipelineRun, error) {
	if m.triggerRunFn == nil {
		panic("mockPipelineService.TriggerRun called but not configured")
	}
	return m.triggerRunFn(ctx, principal, pipelineName, params, triggerType)
}

func (m *mockPipelineService) ListRuns(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
	if m.listRunsFn == nil {
		panic("mockPipelineService.ListRuns called but not configured")
	}
	return m.listRunsFn(ctx, pipelineName, filter)
}

func (m *mockPipelineService) GetRun(ctx context.Context, runID string) (*domain.PipelineRun, error) {
	if m.getRunFn == nil {
		panic("mockPipelineService.GetRun called but not configured")
	}
	return m.getRunFn(ctx, runID)
}

func (m *mockPipelineService) CancelRun(ctx context.Context, principal string, runID string) error {
	if m.cancelRunFn == nil {
		panic("mockPipelineService.CancelRun called but not configured")
	}
	return m.cancelRunFn(ctx, principal, runID)
}

func (m *mockPipelineService) ListJobRuns(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
	if m.listJobRunsFn == nil {
		panic("mockPipelineService.ListJobRuns called but not configured")
	}
	return m.listJobRunsFn(ctx, runID)
}

// === Helpers ===

// pipelineTestCtx returns a context with an admin principal injected.
func pipelineTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var pipelineFixedTime = time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

func samplePipeline() domain.Pipeline {
	cron := "0 * * * *"
	return domain.Pipeline{
		ID:               "pipe-1",
		Name:             "etl-daily",
		Description:      "Daily ETL pipeline",
		ScheduleCron:     &cron,
		IsPaused:         false,
		ConcurrencyLimit: 2,
		CreatedBy:        "test-user",
		CreatedAt:        pipelineFixedTime,
		UpdatedAt:        pipelineFixedTime,
	}
}

func sampleJob() domain.PipelineJob {
	timeout := int64(300)
	return domain.PipelineJob{
		ID:                "job-1",
		PipelineID:        "pipe-1",
		Name:              "extract",
		ComputeEndpointID: pipelineStrPtr("ep-1"),
		DependsOn:         []string{},
		NotebookID:        "nb-1",
		TimeoutSeconds:    &timeout,
		RetryCount:        1,
		JobOrder:          0,
		CreatedAt:         pipelineFixedTime,
	}
}

func sampleRun() domain.PipelineRun {
	started := pipelineFixedTime.Add(time.Second)
	return domain.PipelineRun{
		ID:          "run-1",
		PipelineID:  "pipe-1",
		Status:      domain.PipelineRunStatusRunning,
		TriggerType: domain.TriggerTypeManual,
		TriggeredBy: "test-user",
		Parameters:  map[string]string{"env": "prod"},
		StartedAt:   &started,
		CreatedAt:   pipelineFixedTime,
	}
}

func sampleJobRun() domain.PipelineJobRun {
	started := pipelineFixedTime.Add(2 * time.Second)
	return domain.PipelineJobRun{
		ID:           "jr-1",
		RunID:        "run-1",
		JobID:        "job-1",
		JobName:      "extract",
		Status:       domain.PipelineJobRunStatusRunning,
		StartedAt:    &started,
		RetryAttempt: 0,
		CreatedAt:    pipelineFixedTime,
	}
}

func pipelineStrPtr(s string) *string { return &s }

// === Tests ===

func TestHandler_CreatePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     CreatePipelineJSONRequestBody
		svcFn    func(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error)
		assertFn func(t *testing.T, resp CreatePipelineResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			body: CreatePipelineJSONRequestBody{Name: "etl-daily", Description: pipelineStrPtr("Daily ETL pipeline")},
			svcFn: func(_ context.Context, _ string, _ domain.CreatePipelineRequest) (*domain.Pipeline, error) {
				p := samplePipeline()
				return &p, nil
			},
			assertFn: func(t *testing.T, resp CreatePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreatePipeline201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "etl-daily", *created.Body.Name)
				assert.Equal(t, "pipe-1", *created.Body.Id)
			},
		},
		{
			name: "validation error returns 400",
			body: CreatePipelineJSONRequestBody{Name: "bad"},
			svcFn: func(_ context.Context, _ string, _ domain.CreatePipelineRequest) (*domain.Pipeline, error) {
				return nil, domain.ErrValidation("name is invalid")
			},
			assertFn: func(t *testing.T, resp CreatePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreatePipeline400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "name is invalid")
			},
		},
		{
			name: "conflict error returns 409",
			body: CreatePipelineJSONRequestBody{Name: "etl-daily"},
			svcFn: func(_ context.Context, _ string, _ domain.CreatePipelineRequest) (*domain.Pipeline, error) {
				return nil, domain.ErrConflict("pipeline etl-daily already exists")
			},
			assertFn: func(t *testing.T, resp CreatePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreatePipeline409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
				assert.Contains(t, conflict.Body.Message, "already exists")
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CreatePipelineJSONRequestBody{Name: "fail"},
			svcFn: func(_ context.Context, _ string, _ domain.CreatePipelineRequest) (*domain.Pipeline, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreatePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreatePipeline400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{createPipelineFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			body := tt.body
			resp, err := handler.CreatePipeline(pipelineTestCtx(), CreatePipelineRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetPipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		svcFn    func(ctx context.Context, name string) (*domain.Pipeline, error)
		assertFn func(t *testing.T, resp GetPipelineResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			pipeName: "etl-daily",
			svcFn: func(_ context.Context, name string) (*domain.Pipeline, error) {
				p := samplePipeline()
				p.Name = name
				return &p, nil
			},
			assertFn: func(t *testing.T, resp GetPipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetPipeline200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "etl-daily", *ok200.Body.Name)
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "nonexistent",
			svcFn: func(_ context.Context, name string) (*domain.Pipeline, error) {
				return nil, domain.ErrNotFound("pipeline %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetPipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetPipeline404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
				assert.Contains(t, notFound.Body.Message, "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{getPipelineFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.GetPipeline(pipelineTestCtx(), GetPipelineRequestObject{PipelineName: tt.pipeName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdatePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		body     UpdatePipelineJSONRequestBody
		svcFn    func(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error)
		assertFn func(t *testing.T, resp UpdatePipelineResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			pipeName: "etl-daily",
			body:     UpdatePipelineJSONRequestBody{Description: pipelineStrPtr("updated desc")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
				p := samplePipeline()
				p.Description = "updated desc"
				return &p, nil
			},
			assertFn: func(t *testing.T, resp UpdatePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdatePipeline200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "updated desc", *ok200.Body.Description)
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "nonexistent",
			body:     UpdatePipelineJSONRequestBody{Description: pipelineStrPtr("x")},
			svcFn: func(_ context.Context, _ string, name string, _ domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
				return nil, domain.ErrNotFound("pipeline %s not found", name)
			},
			assertFn: func(t *testing.T, resp UpdatePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdatePipeline404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{updatePipelineFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			body := tt.body
			resp, err := handler.UpdatePipeline(pipelineTestCtx(), UpdatePipelineRequestObject{
				PipelineName: tt.pipeName,
				Body:         &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeletePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		svcFn    func(ctx context.Context, principal string, name string) error
		assertFn func(t *testing.T, resp DeletePipelineResponseObject, err error)
	}{
		{
			name:     "happy path returns 204",
			pipeName: "etl-daily",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeletePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeletePipeline204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) error {
				return domain.ErrNotFound("pipeline %s not found", name)
			},
			assertFn: func(t *testing.T, resp DeletePipelineResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeletePipeline404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{deletePipelineFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.DeletePipeline(pipelineTestCtx(), DeletePipelineRequestObject{PipelineName: tt.pipeName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListPipelines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListPipelinesParams
		svcFn    func(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error)
		assertFn func(t *testing.T, resp ListPipelinesResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListPipelinesParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Pipeline, int64, error) {
				return []domain.Pipeline{samplePipeline()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListPipelinesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListPipelines200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "etl-daily", *(*ok200.Body.Data)[0].Name)
				assert.Nil(t, ok200.Body.NextPageToken, "no next page for single result")
			},
		},
		{
			name:   "empty list returns 200 with empty data",
			params: ListPipelinesParams{},
			svcFn: func(_ context.Context, _ domain.PageRequest) ([]domain.Pipeline, int64, error) {
				return []domain.Pipeline{}, 0, nil
			},
			assertFn: func(t *testing.T, resp ListPipelinesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListPipelines200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				assert.Empty(t, *ok200.Body.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{listPipelinesFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.ListPipelines(pipelineTestCtx(), ListPipelinesRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreatePipelineJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		body     CreatePipelineJobJSONRequestBody
		svcFn    func(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error)
		assertFn func(t *testing.T, resp CreatePipelineJobResponseObject, err error)
	}{
		{
			name:     "happy path returns 201",
			pipeName: "etl-daily",
			body:     CreatePipelineJobJSONRequestBody{Name: "extract", NotebookId: pipelineStrPtr("nb-1")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
				j := sampleJob()
				return &j, nil
			},
			assertFn: func(t *testing.T, resp CreatePipelineJobResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreatePipelineJob201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "extract", *created.Body.Name)
				assert.Equal(t, "job-1", *created.Body.Id)
			},
		},
		{
			name:     "validation error returns 400",
			pipeName: "etl-daily",
			body:     CreatePipelineJobJSONRequestBody{Name: "", NotebookId: pipelineStrPtr("nb-1")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
				return nil, domain.ErrValidation("name is required")
			},
			assertFn: func(t *testing.T, resp CreatePipelineJobResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreatePipelineJob400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "name is required")
			},
		},
		{
			name:     "pipeline not found maps to 400",
			pipeName: "nonexistent",
			body:     CreatePipelineJobJSONRequestBody{Name: "extract", NotebookId: pipelineStrPtr("nb-1")},
			svcFn: func(_ context.Context, _ string, pipelineName string, _ domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
				return nil, domain.ErrNotFound("pipeline %s not found", pipelineName)
			},
			assertFn: func(t *testing.T, resp CreatePipelineJobResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreatePipelineJob400JSONResponse)
				require.True(t, ok, "expected 400 response for not-found pipeline, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "nonexistent")
			},
		},
		{
			name:     "conflict error returns 409",
			pipeName: "etl-daily",
			body:     CreatePipelineJobJSONRequestBody{Name: "extract", NotebookId: pipelineStrPtr("nb-1")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
				return nil, domain.ErrConflict("job extract already exists")
			},
			assertFn: func(t *testing.T, resp CreatePipelineJobResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreatePipelineJob409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{createJobFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			body := tt.body
			resp, err := handler.CreatePipelineJob(pipelineTestCtx(), CreatePipelineJobRequestObject{
				PipelineName: tt.pipeName,
				Body:         &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListPipelineJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		svcFn    func(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error)
		assertFn func(t *testing.T, resp ListPipelineJobsResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			pipeName: "etl-daily",
			svcFn: func(_ context.Context, _ string) ([]domain.PipelineJob, error) {
				return []domain.PipelineJob{sampleJob()}, nil
			},
			assertFn: func(t *testing.T, resp ListPipelineJobsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListPipelineJobs200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "extract", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "nonexistent",
			svcFn: func(_ context.Context, name string) ([]domain.PipelineJob, error) {
				return nil, domain.ErrNotFound("pipeline %s not found", name)
			},
			assertFn: func(t *testing.T, resp ListPipelineJobsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(ListPipelineJobs404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{listJobsFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.ListPipelineJobs(pipelineTestCtx(), ListPipelineJobsRequestObject{PipelineName: tt.pipeName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeletePipelineJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		jobID    string
		svcFn    func(ctx context.Context, principal string, pipelineName string, jobID string) error
		assertFn func(t *testing.T, resp DeletePipelineJobResponseObject, err error)
	}{
		{
			name:     "happy path returns 204",
			pipeName: "etl-daily",
			jobID:    "job-1",
			svcFn: func(_ context.Context, _ string, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeletePipelineJobResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeletePipelineJob204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "etl-daily",
			jobID:    "nonexistent",
			svcFn: func(_ context.Context, _ string, _ string, jobID string) error {
				return domain.ErrNotFound("job %s not found", jobID)
			},
			assertFn: func(t *testing.T, resp DeletePipelineJobResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeletePipelineJob404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{deleteJobFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.DeletePipelineJob(pipelineTestCtx(), DeletePipelineJobRequestObject{
				PipelineName: tt.pipeName,
				JobId:        tt.jobID,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_TriggerPipelineRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		body     *TriggerPipelineRunJSONRequestBody
		svcFn    func(ctx context.Context, principal string, pipelineName string, params map[string]string, triggerType string) (*domain.PipelineRun, error)
		assertFn func(t *testing.T, resp TriggerPipelineRunResponseObject, err error)
	}{
		{
			name:     "happy path returns 201",
			pipeName: "etl-daily",
			body:     &TriggerPipelineRunJSONRequestBody{Parameters: &map[string]string{"env": "prod"}},
			svcFn: func(_ context.Context, _ string, _ string, _ map[string]string, _ string) (*domain.PipelineRun, error) {
				r := sampleRun()
				return &r, nil
			},
			assertFn: func(t *testing.T, resp TriggerPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(TriggerPipelineRun201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "run-1", *created.Body.Id)
				assert.Equal(t, PipelineRunStatus(domain.PipelineRunStatusRunning), *created.Body.Status)
			},
		},
		{
			name:     "nil body is accepted",
			pipeName: "etl-daily",
			body:     nil,
			svcFn: func(_ context.Context, _ string, _ string, params map[string]string, triggerType string) (*domain.PipelineRun, error) {
				assert.Nil(t, params)
				assert.Equal(t, domain.TriggerTypeManual, triggerType)
				r := sampleRun()
				r.Parameters = nil
				return &r, nil
			},
			assertFn: func(t *testing.T, resp TriggerPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(TriggerPipelineRun201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
			},
		},
		{
			name:     "validation error returns 400",
			pipeName: "etl-daily",
			body:     &TriggerPipelineRunJSONRequestBody{},
			svcFn: func(_ context.Context, _ string, _ string, _ map[string]string, _ string) (*domain.PipelineRun, error) {
				return nil, domain.ErrValidation("pipeline is paused")
			},
			assertFn: func(t *testing.T, resp TriggerPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(TriggerPipelineRun400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "paused")
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "nonexistent",
			body:     &TriggerPipelineRunJSONRequestBody{},
			svcFn: func(_ context.Context, _ string, name string, _ map[string]string, _ string) (*domain.PipelineRun, error) {
				return nil, domain.ErrNotFound("pipeline %s not found", name)
			},
			assertFn: func(t *testing.T, resp TriggerPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(TriggerPipelineRun404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{triggerRunFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.TriggerPipelineRun(pipelineTestCtx(), TriggerPipelineRunRequestObject{
				PipelineName: tt.pipeName,
				Body:         tt.body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListPipelineRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeName string
		params   ListPipelineRunsParams
		svcFn    func(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error)
		assertFn func(t *testing.T, resp ListPipelineRunsResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			pipeName: "etl-daily",
			params:   ListPipelineRunsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
				return []domain.PipelineRun{sampleRun()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListPipelineRunsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListPipelineRuns200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "run-1", *(*ok200.Body.Data)[0].Id)
			},
		},
		{
			name:     "not found returns 404",
			pipeName: "nonexistent",
			params:   ListPipelineRunsParams{},
			svcFn: func(_ context.Context, name string, _ domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
				return nil, 0, domain.ErrNotFound("pipeline %s not found", name)
			},
			assertFn: func(t *testing.T, resp ListPipelineRunsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(ListPipelineRuns404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name:     "status filter is forwarded",
			pipeName: "etl-daily",
			params: func() ListPipelineRunsParams {
				s := ListPipelineRunsParamsStatus("RUNNING")
				return ListPipelineRunsParams{Status: &s}
			}(),
			svcFn: func(_ context.Context, _ string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
				require.NotNil(t, filter.Status)
				assert.Equal(t, "RUNNING", *filter.Status)
				return []domain.PipelineRun{sampleRun()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListPipelineRunsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(ListPipelineRuns200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{listRunsFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.ListPipelineRuns(pipelineTestCtx(), ListPipelineRunsRequestObject{
				PipelineName: tt.pipeName,
				Params:       tt.params,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetPipelineRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		runID    string
		svcFn    func(ctx context.Context, runID string) (*domain.PipelineRun, error)
		assertFn func(t *testing.T, resp GetPipelineRunResponseObject, err error)
	}{
		{
			name:  "happy path returns 200",
			runID: "run-1",
			svcFn: func(_ context.Context, _ string) (*domain.PipelineRun, error) {
				r := sampleRun()
				return &r, nil
			},
			assertFn: func(t *testing.T, resp GetPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetPipelineRun200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "run-1", *ok200.Body.Id)
				assert.Equal(t, PipelineRunStatus(domain.PipelineRunStatusRunning), *ok200.Body.Status)
			},
		},
		{
			name:  "not found returns 404",
			runID: "nonexistent",
			svcFn: func(_ context.Context, runID string) (*domain.PipelineRun, error) {
				return nil, domain.ErrNotFound("run %s not found", runID)
			},
			assertFn: func(t *testing.T, resp GetPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetPipelineRun404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{getRunFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.GetPipelineRun(pipelineTestCtx(), GetPipelineRunRequestObject{RunId: tt.runID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CancelPipelineRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		runID    string
		cancelFn func(ctx context.Context, principal string, runID string) error
		getRunFn func(ctx context.Context, runID string) (*domain.PipelineRun, error)
		assertFn func(t *testing.T, resp CancelPipelineRunResponseObject, err error)
	}{
		{
			name:  "happy path returns 200 with cancelled run",
			runID: "run-1",
			cancelFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			getRunFn: func(_ context.Context, _ string) (*domain.PipelineRun, error) {
				r := sampleRun()
				r.Status = domain.PipelineRunStatusCancelled
				return &r, nil
			},
			assertFn: func(t *testing.T, resp CancelPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(CancelPipelineRun200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, PipelineRunStatus(domain.PipelineRunStatusCancelled), *ok200.Body.Status)
			},
		},
		{
			name:  "validation error returns 400",
			runID: "run-1",
			cancelFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrValidation("run is already completed")
			},
			getRunFn: nil, // should not be called
			assertFn: func(t *testing.T, resp CancelPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CancelPipelineRun400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "already completed")
			},
		},
		{
			name:  "not found returns 404",
			runID: "nonexistent",
			cancelFn: func(_ context.Context, _ string, runID string) error {
				return domain.ErrNotFound("run %s not found", runID)
			},
			getRunFn: nil, // should not be called
			assertFn: func(t *testing.T, resp CancelPipelineRunResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(CancelPipelineRun404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{
				cancelRunFn: tt.cancelFn,
				getRunFn:    tt.getRunFn,
			}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.CancelPipelineRun(pipelineTestCtx(), CancelPipelineRunRequestObject{RunId: tt.runID})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_ListPipelineJobRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		runID    string
		svcFn    func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error)
		assertFn func(t *testing.T, resp ListPipelineJobRunsResponseObject, err error)
	}{
		{
			name:  "happy path returns 200",
			runID: "run-1",
			svcFn: func(_ context.Context, _ string) ([]domain.PipelineJobRun, error) {
				return []domain.PipelineJobRun{sampleJobRun()}, nil
			},
			assertFn: func(t *testing.T, resp ListPipelineJobRunsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListPipelineJobRuns200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "jr-1", *(*ok200.Body.Data)[0].Id)
				assert.Equal(t, "extract", *(*ok200.Body.Data)[0].JobName)
				assert.Equal(t, PipelineJobRunStatus(domain.PipelineJobRunStatusRunning), *(*ok200.Body.Data)[0].Status)
			},
		},
		{
			name:  "not found returns 404",
			runID: "nonexistent",
			svcFn: func(_ context.Context, runID string) ([]domain.PipelineJobRun, error) {
				return nil, domain.ErrNotFound("run %s not found", runID)
			},
			assertFn: func(t *testing.T, resp ListPipelineJobRunsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(ListPipelineJobRuns404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockPipelineService{listJobRunsFn: tt.svcFn}
			handler := &APIHandler{pipelines: svc}
			resp, err := handler.ListPipelineJobRuns(pipelineTestCtx(), ListPipelineJobRunsRequestObject{RunId: tt.runID})
			tt.assertFn(t, resp, err)
		})
	}
}

// === Principal propagation ===

func TestHandler_CreatePipeline_PassesPrincipal(t *testing.T) {
	t.Parallel()

	var capturedPrincipal string
	svc := &mockPipelineService{
		createPipelineFn: func(_ context.Context, principal string, _ domain.CreatePipelineRequest) (*domain.Pipeline, error) {
			capturedPrincipal = principal
			p := samplePipeline()
			return &p, nil
		},
	}
	handler := &APIHandler{pipelines: svc}
	body := CreatePipelineJSONRequestBody{Name: "test"}
	_, err := handler.CreatePipeline(pipelineTestCtx(), CreatePipelineRequestObject{Body: &body})
	require.NoError(t, err)
	assert.Equal(t, "test-user", capturedPrincipal)
}

func TestHandler_DeletePipeline_PassesPrincipal(t *testing.T) {
	t.Parallel()

	var capturedPrincipal string
	svc := &mockPipelineService{
		deletePipelineFn: func(_ context.Context, principal string, _ string) error {
			capturedPrincipal = principal
			return nil
		},
	}
	handler := &APIHandler{pipelines: svc}
	_, err := handler.DeletePipeline(pipelineTestCtx(), DeletePipelineRequestObject{PipelineName: "test"})
	require.NoError(t, err)
	assert.Equal(t, "test-user", capturedPrincipal)
}

func TestHandler_TriggerPipelineRun_PassesPrincipalAndTriggerType(t *testing.T) {
	t.Parallel()

	var capturedPrincipal, capturedTriggerType string
	svc := &mockPipelineService{
		triggerRunFn: func(_ context.Context, principal string, _ string, _ map[string]string, triggerType string) (*domain.PipelineRun, error) {
			capturedPrincipal = principal
			capturedTriggerType = triggerType
			r := sampleRun()
			return &r, nil
		},
	}
	handler := &APIHandler{pipelines: svc}
	body := TriggerPipelineRunJSONRequestBody{}
	_, err := handler.TriggerPipelineRun(pipelineTestCtx(), TriggerPipelineRunRequestObject{
		PipelineName: "etl-daily",
		Body:         &body,
	})
	require.NoError(t, err)
	assert.Equal(t, "test-user", capturedPrincipal)
	assert.Equal(t, domain.TriggerTypeManual, capturedTriggerType)
}
