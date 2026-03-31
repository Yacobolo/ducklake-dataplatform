package api

import (
	"context"

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
	TriggerRun(ctx context.Context, principal string, pipelineName string, params map[string]string, triggerType string) (*domain.PipelineRun, error)
	ListRuns(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error)
	GetRun(ctx context.Context, runID string) (*domain.PipelineRun, error)
	CancelRun(ctx context.Context, principal string, runID string) error
	ListJobRuns(ctx context.Context, runID string) ([]domain.PipelineJobRun, error)
}

// === Pipelines ===

// ListPipelines implements the endpoint for listing all pipelines.
func (h *APIHandler) ListPipelines(ctx context.Context, req GenListPipelinesRequest) (GenListPipelinesResponse, error) {
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
	return GenListPipelines200JSONResponse{
		Body:    PaginatedPipelines{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListPipelines200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePipeline implements the endpoint for creating a new pipeline.
func (h *APIHandler) CreatePipeline(ctx context.Context, req GenCreatePipelineRequest) (GenCreatePipelineResponse, error) {
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
	domReq.FolderID = req.Body.FolderId

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.pipelines.CreatePipeline(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreatePipelineResponse]("createPipeline", err, domainErrorResponder[GenCreatePipelineResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreatePipelineResponse {
				return CreatePipeline400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreatePipelineResponse { return CreatePipeline403JSONResponse{resp} },
			Conflict:  func(resp ConflictJSONResponse) GenCreatePipelineResponse { return CreatePipeline409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreatePipeline400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreatePipeline201JSONResponse{
		Body:    pipelineToAPI(*result),
		Headers: GenCreatePipeline201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetPipeline implements the endpoint for retrieving a pipeline by name.
func (h *APIHandler) GetPipeline(ctx context.Context, req GenGetPipelineRequest) (GenGetPipelineResponse, error) {
	result, err := h.pipelines.GetPipeline(ctx, req.PipelineName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetPipelineResponse]("getPipeline", err, domainErrorResponder[GenGetPipelineResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetPipelineResponse {
				return GenGetPipeline404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetPipeline200JSONResponse{
		Body:    pipelineToAPI(*result),
		Headers: GenGetPipeline200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdatePipeline implements the endpoint for updating a pipeline.
func (h *APIHandler) UpdatePipeline(ctx context.Context, req GenUpdatePipelineRequest) (GenUpdatePipelineResponse, error) {
	domReq := domain.UpdatePipelineRequest{
		Description:  req.Body.Description,
		ScheduleCron: req.Body.ScheduleCron,
		IsPaused:     req.Body.IsPaused,
		FolderID:     req.Body.FolderId,
	}
	if req.Body.ConcurrencyLimit != nil {
		v := int(*req.Body.ConcurrencyLimit)
		domReq.ConcurrencyLimit = &v
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.pipelines.UpdatePipeline(ctx, principal, req.PipelineName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdatePipelineResponse]("updatePipeline", err, domainErrorResponder[GenUpdatePipelineResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdatePipelineResponse { return UpdatePipeline403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenUpdatePipelineResponse { return UpdatePipeline404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdatePipeline200JSONResponse{
		Body:    pipelineToAPI(*result),
		Headers: GenUpdatePipeline200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePipeline implements the endpoint for deleting a pipeline.
func (h *APIHandler) DeletePipeline(ctx context.Context, req GenDeletePipelineRequest) (GenDeletePipelineResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.pipelines.DeletePipeline(ctx, principal, req.PipelineName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeletePipelineResponse]("deletePipeline", err, domainErrorResponder[GenDeletePipelineResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeletePipelineResponse { return DeletePipeline403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeletePipelineResponse { return DeletePipeline404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeletePipeline204Response{
		Headers: GenDeletePipeline204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Jobs ===

// ListPipelineJobs implements the endpoint for listing jobs in a pipeline.
func (h *APIHandler) ListPipelineJobs(ctx context.Context, req GenListPipelineJobsRequest) (GenListPipelineJobsResponse, error) {
	jobs, err := h.pipelines.ListJobs(ctx, req.PipelineName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListPipelineJobsResponse]("listPipelineJobs", err, domainErrorResponder[GenListPipelineJobsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListPipelineJobsResponse {
				return GenListPipelineJobs404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]PipelineJob, len(jobs))
	for i, j := range jobs {
		data[i] = pipelineJobToAPI(j)
	}
	return GenListPipelineJobs200JSONResponse{
		Body:    PipelineJobList{Data: data},
		Headers: GenListPipelineJobs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePipelineJob implements the endpoint for creating a job in a pipeline.
func (h *APIHandler) CreatePipelineJob(ctx context.Context, req GenCreatePipelineJobRequest) (GenCreatePipelineJobResponse, error) {
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
		domReq.TimeoutSeconds = int32PtrToInt64Ptr(req.Body.TimeoutSeconds)
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
		if resp, ok := respondDomainErrorForOperation[GenCreatePipelineJobResponse]("createPipelineJob", err, domainErrorResponder[GenCreatePipelineJobResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreatePipelineJobResponse {
				return CreatePipelineJob400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreatePipelineJobResponse {
				return CreatePipelineJob403JSONResponse{resp}
			},
			NotFound: func(NotFoundJSONResponse) GenCreatePipelineJobResponse {
				return CreatePipelineJob400JSONResponse{badRequestErrorResponse(err)}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreatePipelineJobResponse {
				return CreatePipelineJob409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CreatePipelineJob400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreatePipelineJob201JSONResponse{
		Body:    pipelineJobToAPI(*result),
		Headers: GenCreatePipelineJob201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePipelineJob implements the endpoint for deleting a pipeline job.
func (h *APIHandler) DeletePipelineJob(ctx context.Context, req GenDeletePipelineJobRequest) (GenDeletePipelineJobResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.pipelines.DeleteJob(ctx, principal, req.PipelineName, req.JobId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeletePipelineJobResponse]("deletePipelineJob", err, domainErrorResponder[GenDeletePipelineJobResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeletePipelineJobResponse {
				return DeletePipelineJob403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeletePipelineJobResponse {
				return DeletePipelineJob404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeletePipelineJob204Response{
		Headers: GenDeletePipelineJob204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Runs ===

// TriggerPipelineRun implements the endpoint for triggering a pipeline run.
func (h *APIHandler) TriggerPipelineRun(ctx context.Context, req GenTriggerPipelineRunRequest) (GenTriggerPipelineRunResponse, error) {
	var params map[string]string
	if req.Body != nil && req.Body.Parameters != nil {
		params = recordToStringMap(req.Body.Parameters)
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.pipelines.TriggerRun(ctx, principal, req.PipelineName, params, domain.TriggerTypeManual)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenTriggerPipelineRunResponse]("triggerPipelineRun", err, domainErrorResponder[GenTriggerPipelineRunResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenTriggerPipelineRunResponse {
				return TriggerPipelineRun400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenTriggerPipelineRunResponse {
				return TriggerPipelineRun403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenTriggerPipelineRunResponse {
				return TriggerPipelineRun404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenTriggerPipelineRunResponse {
				return TriggerPipelineRun409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return TriggerPipelineRun400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenTriggerPipelineRun200JSONResponse{
		Body:    pipelineRunToAPI(*result),
		Headers: GenTriggerPipelineRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListPipelineRuns implements the endpoint for listing runs of a pipeline.
func (h *APIHandler) ListPipelineRuns(ctx context.Context, req GenListPipelineRunsRequest) (GenListPipelineRunsResponse, error) {
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
		if resp, ok := respondDomainErrorForOperation[GenListPipelineRunsResponse]("listPipelineRuns", err, domainErrorResponder[GenListPipelineRunsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListPipelineRunsResponse {
				return GenListPipelineRuns404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]PipelineRun, len(runs))
	for i, r := range runs {
		data[i] = pipelineRunToAPI(r)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListPipelineRuns200JSONResponse{
		Body:    PaginatedPipelineRuns{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListPipelineRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetPipelineRun implements the endpoint for retrieving a pipeline run.
func (h *APIHandler) GetPipelineRun(ctx context.Context, req GenGetPipelineRunRequest) (GenGetPipelineRunResponse, error) {
	result, err := h.pipelines.GetRun(ctx, req.RunId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetPipelineRunResponse]("getPipelineRun", err, domainErrorResponder[GenGetPipelineRunResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetPipelineRunResponse {
				return GenGetPipelineRun404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetPipelineRun200JSONResponse{
		Body:    pipelineRunToAPI(*result),
		Headers: GenGetPipelineRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelPipelineRun implements the endpoint for cancelling a pipeline run.
func (h *APIHandler) CancelPipelineRun(ctx context.Context, req GenCancelPipelineRunRequest) (GenCancelPipelineRunResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.pipelines.CancelRun(ctx, principal, req.RunId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCancelPipelineRunResponse]("cancelPipelineRun", err, domainErrorResponder[GenCancelPipelineRunResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCancelPipelineRunResponse {
				return CancelPipelineRun400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCancelPipelineRunResponse {
				return CancelPipelineRun403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCancelPipelineRunResponse {
				return CancelPipelineRun404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCancelPipelineRunResponse {
				return CancelPipelineRun409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
func (h *APIHandler) ListPipelineJobRuns(ctx context.Context, req GenListPipelineJobRunsRequest) (GenListPipelineJobRunsResponse, error) {
	jobRuns, err := h.pipelines.ListJobRuns(ctx, req.RunId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListPipelineJobRunsResponse]("listPipelineJobRuns", err, domainErrorResponder[GenListPipelineJobRunsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListPipelineJobRunsResponse {
				return GenListPipelineJobRuns404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]PipelineJobRun, len(jobRuns))
	for i, jr := range jobRuns {
		data[i] = pipelineJobRunToAPI(jr)
	}
	return GenListPipelineJobRuns200JSONResponse{
		Body:    PipelineJobRunList{Data: data},
		Headers: GenListPipelineJobRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Pipeline Mappers ===

func pipelineToAPI(p domain.Pipeline) Pipeline {
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
		FolderId:         optStr(p.FolderID),
		CreatedAt:        formatTimePtr(&p.CreatedAt),
		UpdatedAt:        formatTimePtr(&p.UpdatedAt),
	}
}

func pipelineJobToAPI(j domain.PipelineJob) PipelineJob {
	order := int32(j.JobOrder)        //nolint:gosec // JobOrder is a small non-negative index
	retryCount := int32(j.RetryCount) //nolint:gosec // RetryCount is a small non-negative integer
	resp := PipelineJob{
		Id:         &j.ID,
		PipelineId: &j.PipelineID,
		Name:       &j.Name,
		JobOrder:   &order,
		RetryCount: &retryCount,
		CreatedAt:  formatTimePtr(&j.CreatedAt),
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
		resp.TimeoutSeconds = safeInt64ToInt32Ptr(j.TimeoutSeconds)
	}
	return resp
}

func pipelineRunToAPI(r domain.PipelineRun) PipelineRun {
	status := PipelineRunStatus(r.Status)
	triggerType := PipelineRunTriggerType(r.TriggerType)
	resp := PipelineRun{
		Id:          &r.ID,
		PipelineId:  &r.PipelineID,
		Status:      &status,
		TriggerType: &triggerType,
		TriggeredBy: &r.TriggeredBy,
		CreatedAt:   formatTimePtr(&r.CreatedAt),
	}
	if len(r.Parameters) > 0 {
		resp.Parameters = stringMapToRecord(r.Parameters)
	}
	if r.GitCommitHash != nil {
		resp.GitCommitHash = r.GitCommitHash
	}
	if r.StartedAt != nil {
		resp.StartedAt = formatTimePtr(r.StartedAt)
	}
	if r.FinishedAt != nil {
		resp.FinishedAt = formatTimePtr(r.FinishedAt)
	}
	if r.ErrorMessage != nil {
		resp.ErrorMessage = r.ErrorMessage
	}
	return resp
}

func pipelineJobRunToAPI(jr domain.PipelineJobRun) PipelineJobRun {
	status := PipelineJobRunStatus(jr.Status)
	retryAttempt := int32(jr.RetryAttempt) //nolint:gosec // RetryAttempt is a small non-negative integer
	resp := PipelineJobRun{
		Id:           &jr.ID,
		RunId:        &jr.RunID,
		JobId:        &jr.JobID,
		JobName:      &jr.JobName,
		Status:       &status,
		RetryAttempt: &retryAttempt,
		CreatedAt:    formatTimePtr(&jr.CreatedAt),
	}
	if jr.StartedAt != nil {
		resp.StartedAt = formatTimePtr(jr.StartedAt)
	}
	if jr.FinishedAt != nil {
		resp.FinishedAt = formatTimePtr(jr.FinishedAt)
	}
	if jr.ErrorMessage != nil {
		resp.ErrorMessage = jr.ErrorMessage
	}
	return resp
}
