package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// pipelineService defines the pipeline operations used by the API handler.
type pipelineService interface {
	CreatePipeline(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error)
	GetPipeline(ctx context.Context, name string) (*domain.Pipeline, error)
	ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error)
	UpdatePipeline(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error)
	DeletePipeline(ctx context.Context, principal string, name string) error
	CreateJob(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error)
	ListJobs(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error)
	DeleteJob(ctx context.Context, principal string, pipelineName string, jobID string) error
	TriggerRun(ctx context.Context, principal string, pipelineName string, params map[string]string, triggerType string) (*domain.PipelineRun, error)
	ListRuns(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error)
	GetRun(ctx context.Context, runID string) (*domain.PipelineRun, error)
	CancelRun(ctx context.Context, principal string, runID string) error
	ListJobRuns(ctx context.Context, runID string) ([]domain.PipelineJobRun, error)
}

// === Pipelines ===

// ListPipelines implements the endpoint for listing all pipelines.
func (h *APIHandler) ListPipelines(ctx context.Context, req ListPipelinesRequestObject) (ListPipelinesResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	pipelines, total, err := h.pipelines.ListPipelines(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]Pipeline, len(pipelines))
	for i, p := range pipelines {
		data[i] = pipelineToAPI(p)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListPipelines200JSONResponse{
		Body:    PaginatedPipelines{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListPipelines200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePipeline implements the endpoint for creating a new pipeline.
func (h *APIHandler) CreatePipeline(ctx context.Context, req CreatePipelineRequestObject) (CreatePipelineResponseObject, error) {
	domReq := domain.CreatePipelineRequest{
		Name: req.Body.Name,
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	if req.Body.ScheduleCron != nil {
		domReq.ScheduleCron = req.Body.ScheduleCron
	}
	if req.Body.IsPaused != nil {
		domReq.IsPaused = *req.Body.IsPaused
	}
	if req.Body.ConcurrencyLimit != nil {
		domReq.ConcurrencyLimit = int(*req.Body.ConcurrencyLimit)
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.pipelines.CreatePipeline(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreatePipeline403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreatePipeline400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreatePipeline409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreatePipeline400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreatePipeline201JSONResponse{
		Body:    pipelineToAPI(*result),
		Headers: CreatePipeline201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetPipeline implements the endpoint for retrieving a pipeline by name.
func (h *APIHandler) GetPipeline(ctx context.Context, req GetPipelineRequestObject) (GetPipelineResponseObject, error) {
	result, err := h.pipelines.GetPipeline(ctx, req.PipelineName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetPipeline404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetPipeline200JSONResponse{
		Body:    pipelineToAPI(*result),
		Headers: GetPipeline200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdatePipeline implements the endpoint for updating a pipeline.
func (h *APIHandler) UpdatePipeline(ctx context.Context, req UpdatePipelineRequestObject) (UpdatePipelineResponseObject, error) {
	domReq := domain.UpdatePipelineRequest{
		Description:  req.Body.Description,
		ScheduleCron: req.Body.ScheduleCron,
		IsPaused:     req.Body.IsPaused,
	}
	if req.Body.ConcurrencyLimit != nil {
		v := int(*req.Body.ConcurrencyLimit)
		domReq.ConcurrencyLimit = &v
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.pipelines.UpdatePipeline(ctx, principal, req.PipelineName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdatePipeline403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdatePipeline404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdatePipeline200JSONResponse{
		Body:    pipelineToAPI(*result),
		Headers: UpdatePipeline200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePipeline implements the endpoint for deleting a pipeline.
func (h *APIHandler) DeletePipeline(ctx context.Context, req DeletePipelineRequestObject) (DeletePipelineResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.pipelines.DeletePipeline(ctx, principal, req.PipelineName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeletePipeline403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeletePipeline404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeletePipeline204Response{
		Headers: DeletePipeline204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Jobs ===

// ListPipelineJobs implements the endpoint for listing jobs in a pipeline.
func (h *APIHandler) ListPipelineJobs(ctx context.Context, req ListPipelineJobsRequestObject) (ListPipelineJobsResponseObject, error) {
	jobs, err := h.pipelines.ListJobs(ctx, req.PipelineName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListPipelineJobs404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]PipelineJob, len(jobs))
	for i, j := range jobs {
		data[i] = pipelineJobToAPI(j)
	}
	return ListPipelineJobs200JSONResponse{
		Body:    data,
		Headers: ListPipelineJobs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePipelineJob implements the endpoint for creating a job in a pipeline.
func (h *APIHandler) CreatePipelineJob(ctx context.Context, req CreatePipelineJobRequestObject) (CreatePipelineJobResponseObject, error) {
	domReq := domain.CreatePipelineJobRequest{
		Name:       req.Body.Name,
		NotebookID: req.Body.NotebookId,
	}
	if req.Body.ComputeEndpointId != nil {
		domReq.ComputeEndpointID = req.Body.ComputeEndpointId
	}
	if req.Body.DependsOn != nil {
		domReq.DependsOn = *req.Body.DependsOn
	}
	if req.Body.TimeoutSeconds != nil {
		domReq.TimeoutSeconds = req.Body.TimeoutSeconds
	}
	if req.Body.RetryCount != nil {
		domReq.RetryCount = int(*req.Body.RetryCount)
	}
	if req.Body.JobOrder != nil {
		domReq.JobOrder = int(*req.Body.JobOrder)
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.pipelines.CreateJob(ctx, principal, req.PipelineName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreatePipelineJob403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreatePipelineJob400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreatePipelineJob400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreatePipelineJob409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreatePipelineJob400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreatePipelineJob201JSONResponse{
		Body:    pipelineJobToAPI(*result),
		Headers: CreatePipelineJob201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePipelineJob implements the endpoint for deleting a pipeline job.
func (h *APIHandler) DeletePipelineJob(ctx context.Context, req DeletePipelineJobRequestObject) (DeletePipelineJobResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.pipelines.DeleteJob(ctx, principal, req.PipelineName, req.JobId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeletePipelineJob403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeletePipelineJob404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeletePipelineJob204Response{
		Headers: DeletePipelineJob204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Runs ===

// TriggerPipelineRun implements the endpoint for triggering a pipeline run.
func (h *APIHandler) TriggerPipelineRun(ctx context.Context, req TriggerPipelineRunRequestObject) (TriggerPipelineRunResponseObject, error) {
	var params map[string]string
	if req.Body != nil && req.Body.Parameters != nil {
		params = *req.Body.Parameters
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.pipelines.TriggerRun(ctx, principal, req.PipelineName, params, domain.TriggerTypeManual)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return TriggerPipelineRun403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return TriggerPipelineRun400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return TriggerPipelineRun404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return TriggerPipelineRun409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return TriggerPipelineRun400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return TriggerPipelineRun201JSONResponse{
		Body:    pipelineRunToAPI(*result),
		Headers: TriggerPipelineRun201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListPipelineRuns implements the endpoint for listing runs of a pipeline.
func (h *APIHandler) ListPipelineRuns(ctx context.Context, req ListPipelineRunsRequestObject) (ListPipelineRunsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.PipelineRunFilter{
		Page: page,
	}
	if req.Params.Status != nil {
		s := string(*req.Params.Status)
		filter.Status = &s
	}

	runs, total, err := h.pipelines.ListRuns(ctx, req.PipelineName, filter)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListPipelineRuns404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]PipelineRun, len(runs))
	for i, r := range runs {
		data[i] = pipelineRunToAPI(r)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListPipelineRuns200JSONResponse{
		Body:    PaginatedPipelineRuns{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListPipelineRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetPipelineRun implements the endpoint for retrieving a pipeline run.
func (h *APIHandler) GetPipelineRun(ctx context.Context, req GetPipelineRunRequestObject) (GetPipelineRunResponseObject, error) {
	result, err := h.pipelines.GetRun(ctx, req.RunId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetPipelineRun404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetPipelineRun200JSONResponse{
		Body:    pipelineRunToAPI(*result),
		Headers: GetPipelineRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelPipelineRun implements the endpoint for cancelling a pipeline run.
func (h *APIHandler) CancelPipelineRun(ctx context.Context, req CancelPipelineRunRequestObject) (CancelPipelineRunResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.pipelines.CancelRun(ctx, principal, req.RunId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CancelPipelineRun403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CancelPipelineRun400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CancelPipelineRun404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CancelPipelineRun409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	// Re-fetch the run to return updated state.
	result, err := h.pipelines.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, err
	}
	return CancelPipelineRun200JSONResponse{
		Body:    pipelineRunToAPI(*result),
		Headers: CancelPipelineRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Job Runs ===

// ListPipelineJobRuns implements the endpoint for listing job runs of a pipeline run.
func (h *APIHandler) ListPipelineJobRuns(ctx context.Context, req ListPipelineJobRunsRequestObject) (ListPipelineJobRunsResponseObject, error) {
	jobRuns, err := h.pipelines.ListJobRuns(ctx, req.RunId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListPipelineJobRuns404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]PipelineJobRun, len(jobRuns))
	for i, jr := range jobRuns {
		data[i] = pipelineJobRunToAPI(jr)
	}
	return ListPipelineJobRuns200JSONResponse{
		Body:    data,
		Headers: ListPipelineJobRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Mappers ===

func pipelineToAPI(p domain.Pipeline) Pipeline {
	ct := p.CreatedAt
	ut := p.UpdatedAt
	isPaused := p.IsPaused
	concLimit := int32(p.ConcurrencyLimit)
	return Pipeline{
		Id:               &p.ID,
		Name:             &p.Name,
		Description:      &p.Description,
		ScheduleCron:     p.ScheduleCron,
		IsPaused:         &isPaused,
		ConcurrencyLimit: &concLimit,
		CreatedBy:        &p.CreatedBy,
		CreatedAt:        &ct,
		UpdatedAt:        &ut,
	}
}

func pipelineJobToAPI(j domain.PipelineJob) PipelineJob {
	ct := j.CreatedAt
	order := int32(j.JobOrder)
	retryCount := int32(j.RetryCount)
	resp := PipelineJob{
		Id:         &j.ID,
		PipelineId: &j.PipelineID,
		Name:       &j.Name,
		NotebookId: &j.NotebookID,
		JobOrder:   &order,
		RetryCount: &retryCount,
		CreatedAt:  &ct,
	}
	if j.ComputeEndpointID != nil {
		resp.ComputeEndpointId = j.ComputeEndpointID
	}
	if len(j.DependsOn) > 0 {
		resp.DependsOn = &j.DependsOn
	}
	if j.TimeoutSeconds != nil {
		resp.TimeoutSeconds = j.TimeoutSeconds
	}
	return resp
}

func pipelineRunToAPI(r domain.PipelineRun) PipelineRun {
	ct := r.CreatedAt
	status := PipelineRunStatus(r.Status)
	triggerType := PipelineRunTriggerType(r.TriggerType)
	resp := PipelineRun{
		Id:          &r.ID,
		PipelineId:  &r.PipelineID,
		Status:      &status,
		TriggerType: &triggerType,
		TriggeredBy: &r.TriggeredBy,
		CreatedAt:   &ct,
	}
	if len(r.Parameters) > 0 {
		resp.Parameters = &r.Parameters
	}
	if r.GitCommitHash != nil {
		resp.GitCommitHash = r.GitCommitHash
	}
	if r.StartedAt != nil {
		resp.StartedAt = r.StartedAt
	}
	if r.FinishedAt != nil {
		resp.FinishedAt = r.FinishedAt
	}
	if r.ErrorMessage != nil {
		resp.ErrorMessage = r.ErrorMessage
	}
	return resp
}

func pipelineJobRunToAPI(jr domain.PipelineJobRun) PipelineJobRun {
	ct := jr.CreatedAt
	status := PipelineJobRunStatus(jr.Status)
	retryAttempt := int32(jr.RetryAttempt)
	resp := PipelineJobRun{
		Id:           &jr.ID,
		RunId:        &jr.RunID,
		JobId:        &jr.JobID,
		JobName:      &jr.JobName,
		Status:       &status,
		RetryAttempt: &retryAttempt,
		CreatedAt:    &ct,
	}
	if jr.StartedAt != nil {
		resp.StartedAt = jr.StartedAt
	}
	if jr.FinishedAt != nil {
		resp.FinishedAt = jr.FinishedAt
	}
	if jr.ErrorMessage != nil {
		resp.ErrorMessage = jr.ErrorMessage
	}
	return resp
}
