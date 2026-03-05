package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
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

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
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

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
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
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
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
		Body:    PipelineJobList{Data: &data},
		Headers: ListPipelineJobs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePipelineJob implements the endpoint for creating a job in a pipeline.
func (h *APIHandler) CreatePipelineJob(ctx context.Context, req CreatePipelineJobRequestObject) (CreatePipelineJobResponseObject, error) {
	domReq := domain.CreatePipelineJobRequest{
		Name: req.Body.Name,
	}
	if req.Body.NotebookId != nil {
		domReq.NotebookID = *req.Body.NotebookId
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
	if req.Body.JobType != nil {
		domReq.JobType = string(*req.Body.JobType)
	}
	if req.Body.ModelSelector != nil {
		domReq.ModelSelector = *req.Body.ModelSelector
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
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
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
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

// === Pipeline Mappers ===

func pipelineToAPI(p domain.Pipeline) Pipeline {
	ct := p.CreatedAt
	ut := p.UpdatedAt
	isPaused := p.IsPaused
	concLimit := int32(p.ConcurrencyLimit) //nolint:gosec // ConcurrencyLimit is validated to be non-negative and small
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
	order := int32(j.JobOrder)        //nolint:gosec // JobOrder is a small non-negative index
	retryCount := int32(j.RetryCount) //nolint:gosec // RetryCount is a small non-negative integer
	resp := PipelineJob{
		Id:         &j.ID,
		PipelineId: &j.PipelineID,
		Name:       &j.Name,
		JobOrder:   &order,
		RetryCount: &retryCount,
		CreatedAt:  &ct,
	}
	if j.NotebookID != "" {
		resp.NotebookId = &j.NotebookID
	}
	if j.JobType != "" {
		jt := PipelineJobJobType(j.JobType)
		resp.JobType = &jt
	}
	if j.ModelSelector != "" {
		resp.ModelSelector = &j.ModelSelector
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
