package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
)

// computeEndpointService defines the compute endpoint operations used by the API handler.
type computeEndpointService interface {
	List(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error)
	Create(ctx context.Context, principal string, req domain.CreateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	GetByName(ctx context.Context, principal, name string) (*domain.ComputeEndpoint, error)
	Update(ctx context.Context, principal string, name string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error)
	Delete(ctx context.Context, principal string, name string) error
	ListAssignments(ctx context.Context, principal, endpointName string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error)
	Assign(ctx context.Context, principal string, endpointName string, req domain.CreateComputeAssignmentRequest) (*domain.ComputeAssignment, error)
	Unassign(ctx context.Context, principal string, assignmentID string) error
	HealthCheck(ctx context.Context, principal string, endpointName string) (*domain.ComputeEndpointHealthResult, error)
	ListAvailableTargets(ctx context.Context, principal string, workloadType string) ([]domain.ComputeTarget, error)
	GetRoutingDefaults(ctx context.Context, principal string) (*domain.ComputeRoutingDefaults, error)
	UpdateRoutingDefaults(ctx context.Context, principal string, defaults domain.ComputeRoutingDefaults) (*domain.ComputeRoutingDefaults, error)
}

// === Compute Endpoints ===

// ListComputeEndpoints implements the endpoint for listing all compute endpoints.
func (h *APIHandler) ListComputeEndpoints(ctx context.Context, req GenListComputeEndpointsRequest) (GenListComputeEndpointsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	principal := principalFromCtx(ctx)
	eps, total, err := h.computeEndpoints.List(ctx, principal, page)
	if err != nil {
		return nil, err
	}

	data := make([]ComputeEndpoint, len(eps))
	for i, ep := range eps {
		data[i] = computeEndpointToAPI(ep)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListComputeEndpoints200JSONResponse{
		Body:    PaginatedComputeEndpoints{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListComputeEndpoints200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateComputeEndpoint implements the endpoint for creating a new compute endpoint.
func (h *APIHandler) CreateComputeEndpoint(ctx context.Context, req GenCreateComputeEndpointRequest) (GenCreateComputeEndpointResponse, error) {
	domReq := domain.CreateComputeEndpointRequest{
		Name: req.Body.Name,
		URL:  req.Body.Url,
		Type: string(req.Body.Type),
	}
	if req.Body.SelectionPolicy != nil {
		domReq.SelectionPolicy = string(*req.Body.SelectionPolicy)
	}
	if req.Body.WorkloadClass != nil {
		domReq.WorkloadClass = string(*req.Body.WorkloadClass)
	}
	if req.Body.ReadinessStatus != nil {
		domReq.ReadinessStatus = string(*req.Body.ReadinessStatus)
	}
	if req.Body.Size != nil {
		domReq.Size = string(*req.Body.Size)
	}
	if req.Body.MaxMemoryGb != nil {
		domReq.MaxMemoryGB = int32PtrToInt64Ptr(req.Body.MaxMemoryGb)
	}
	if req.Body.MaxConcurrency != nil {
		domReq.MaxConcurrency = req.Body.MaxConcurrency
	}
	if req.Body.MaxResultSizeMb != nil {
		domReq.MaxResultSizeMB = req.Body.MaxResultSizeMb
	}
	if req.Body.RecommendedForLargeQueries != nil {
		domReq.RecommendedForLargeQueries = *req.Body.RecommendedForLargeQueries
	}
	if req.Body.IsDraining != nil {
		domReq.IsDraining = *req.Body.IsDraining
	}
	if req.Body.AuthToken != nil {
		domReq.AuthToken = *req.Body.AuthToken
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.computeEndpoints.Create(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateComputeEndpoint403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateComputeEndpoint400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateComputeEndpoint409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateComputeEndpoint400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return GenCreateComputeEndpoint201JSONResponse{
		Body:    computeEndpointToAPI(*result),
		Headers: GenCreateComputeEndpoint201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetComputeEndpoint implements the endpoint for retrieving a compute endpoint by name.
func (h *APIHandler) GetComputeEndpoint(ctx context.Context, req GenGetComputeEndpointRequest) (GenGetComputeEndpointResponse, error) {
	principal := principalFromCtx(ctx)
	result, err := h.computeEndpoints.GetByName(ctx, principal, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetComputeEndpoint404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetComputeEndpoint200JSONResponse{
		Body:    computeEndpointToAPI(*result),
		Headers: GenGetComputeEndpoint200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateComputeEndpoint implements the endpoint for updating a compute endpoint.
func (h *APIHandler) UpdateComputeEndpoint(ctx context.Context, req GenUpdateComputeEndpointRequest) (GenUpdateComputeEndpointResponse, error) {
	domReq := domain.UpdateComputeEndpointRequest{
		URL:             req.Body.Url,
		MaxMemoryGB:     int32PtrToInt64Ptr(req.Body.MaxMemoryGb),
		MaxConcurrency:  req.Body.MaxConcurrency,
		MaxResultSizeMB: req.Body.MaxResultSizeMb,
		AuthToken:       req.Body.AuthToken,
	}
	if req.Body.Size != nil {
		s := string(*req.Body.Size)
		domReq.Size = &s
	}
	if req.Body.SelectionPolicy != nil {
		s := string(*req.Body.SelectionPolicy)
		domReq.SelectionPolicy = &s
	}
	if req.Body.WorkloadClass != nil {
		s := string(*req.Body.WorkloadClass)
		domReq.WorkloadClass = &s
	}
	if req.Body.ReadinessStatus != nil {
		s := string(*req.Body.ReadinessStatus)
		domReq.ReadinessStatus = &s
	}
	if req.Body.RecommendedForLargeQueries != nil {
		domReq.RecommendedForLargeQueries = req.Body.RecommendedForLargeQueries
	}
	if req.Body.IsDraining != nil {
		domReq.IsDraining = req.Body.IsDraining
	}
	if req.Body.Status != nil {
		s := string(*req.Body.Status)
		domReq.Status = &s
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.computeEndpoints.Update(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateComputeEndpoint403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateComputeEndpoint404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenUpdateComputeEndpoint200JSONResponse{
		Body:    computeEndpointToAPI(*result),
		Headers: GenUpdateComputeEndpoint200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetComputeEndpointStatusSummary returns persisted operational summary for a compute endpoint.
func (h *APIHandler) GetComputeEndpointStatusSummary(ctx context.Context, req GetComputeEndpointStatusSummaryRequestObject) (GetComputeEndpointStatusSummaryResponseObject, error) {
	principal := principalFromCtx(ctx)
	result, err := h.computeEndpoints.GetByName(ctx, principal, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetComputeEndpointStatusSummary404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetComputeEndpointStatusSummary200JSONResponse{
		Body:    computeEndpointStatusSummaryToAPI(*result),
		Headers: GetComputeEndpointStatusSummary200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DrainComputeEndpoint marks an endpoint as draining.
func (h *APIHandler) DrainComputeEndpoint(ctx context.Context, req DrainComputeEndpointRequestObject) (DrainComputeEndpointResponseObject, error) {
	principal := principalFromCtx(ctx)
	draining := true
	result, err := h.computeEndpoints.Update(ctx, principal, req.EndpointName, domain.UpdateComputeEndpointRequest{IsDraining: &draining})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DrainComputeEndpoint403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DrainComputeEndpoint404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DrainComputeEndpoint200JSONResponse{Body: computeEndpointToAPI(*result), Headers: DrainComputeEndpoint200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// ResumeComputeEndpoint clears drain state on an endpoint.
func (h *APIHandler) ResumeComputeEndpoint(ctx context.Context, req ResumeComputeEndpointRequestObject) (ResumeComputeEndpointResponseObject, error) {
	principal := principalFromCtx(ctx)
	draining := false
	result, err := h.computeEndpoints.Update(ctx, principal, req.EndpointName, domain.UpdateComputeEndpointRequest{IsDraining: &draining})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ResumeComputeEndpoint403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return ResumeComputeEndpoint404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return ResumeComputeEndpoint200JSONResponse{Body: computeEndpointToAPI(*result), Headers: ResumeComputeEndpoint200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// DeleteComputeEndpoint implements the endpoint for deleting a compute endpoint.
func (h *APIHandler) DeleteComputeEndpoint(ctx context.Context, req GenDeleteComputeEndpointRequest) (GenDeleteComputeEndpointResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.computeEndpoints.Delete(ctx, principal, req.EndpointName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteComputeEndpoint403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteComputeEndpoint404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteComputeEndpoint204Response{}, nil
}

// ListComputeAssignments implements the endpoint for listing assignments for a compute endpoint.
func (h *APIHandler) ListComputeAssignments(ctx context.Context, req GenListComputeAssignmentsRequest) (GenListComputeAssignmentsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	principal := principalFromCtx(ctx)
	assignments, total, err := h.computeEndpoints.ListAssignments(ctx, principal, req.EndpointName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenListComputeAssignments404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]ComputeAssignment, len(assignments))
	for i, a := range assignments {
		data[i] = computeAssignmentToAPI(a)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListComputeAssignments200JSONResponse{
		Body:    PaginatedComputeAssignments{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListComputeAssignments200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateComputeAssignment implements the endpoint for assigning a principal to a compute endpoint.
func (h *APIHandler) CreateComputeAssignment(ctx context.Context, req GenCreateComputeAssignmentRequest) (GenCreateComputeAssignmentResponse, error) {
	domReq := domain.CreateComputeAssignmentRequest{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
	}
	if req.Body.IsDefault != nil {
		domReq.IsDefault = *req.Body.IsDefault
	}
	if req.Body.FallbackLocal != nil {
		domReq.FallbackLocal = *req.Body.FallbackLocal
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.computeEndpoints.Assign(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateComputeAssignment403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateComputeAssignment400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateComputeAssignment409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateComputeAssignment400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return GenCreateComputeAssignment201JSONResponse{
		Body:    computeAssignmentToAPI(*result),
		Headers: GenCreateComputeAssignment201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetComputeEndpointHealth implements the endpoint for checking compute endpoint health.
func (h *APIHandler) GetComputeEndpointHealth(ctx context.Context, req GenGetComputeEndpointHealthRequest) (GenGetComputeEndpointHealthResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	result, err := h.computeEndpoints.HealthCheck(ctx, principal, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GetComputeEndpointHealth403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetComputeEndpointHealth404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GetComputeEndpointHealth502JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: 502, Message: err.Error()}, Headers: GetComputeEndpointHealth502ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	var uptimeSeconds *int32
	if result.UptimeSeconds != nil {
		v := safeIntToInt32(*result.UptimeSeconds)
		uptimeSeconds = &v
	}
	var memoryUsedMb *int32
	if result.MemoryUsedMb != nil {
		v := safeIntToInt32(*result.MemoryUsedMb)
		memoryUsedMb = &v
	}
	var maxMemoryGb *int32
	if result.MaxMemoryGb != nil {
		v := safeIntToInt32(*result.MaxMemoryGb)
		maxMemoryGb = &v
	}
	return GenGetComputeEndpointHealth200JSONResponse{
		Body: ComputeEndpointHealth{
			Status:                result.Status,
			UptimeSeconds:         uptimeSeconds,
			DuckdbVersion:         result.DuckdbVersion,
			MemoryUsedMb:          memoryUsedMb,
			MaxMemoryGb:           maxMemoryGb,
			ActiveQueries:         result.ActiveQueries,
			QueuedJobs:            result.QueuedJobs,
			RunningJobs:           result.RunningJobs,
			CompletedJobs:         result.CompletedJobs,
			StoredJobs:            result.StoredJobs,
			CleanedJobs:           result.CleanedJobs,
			QueryResultTtlSeconds: intPtrToInt32Ptr(result.QueryResultTTLSeconds),
			EndpointName:          &req.EndpointName,
		},
		Headers: GenGetComputeEndpointHealth200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListComputeTargets implements the endpoint for listing principal-visible compute targets.
func (h *APIHandler) ListComputeTargets(ctx context.Context, req ListComputeTargetsRequestObject) (ListComputeTargetsResponseObject, error) {
	principal := principalFromCtx(ctx)
	workloadType := ""
	if req.Params.WorkloadType != nil {
		workloadType = string(*req.Params.WorkloadType)
	}

	targets, listErr := h.computeEndpoints.ListAvailableTargets(ctx, principal, workloadType)
	if listErr != nil {
		message := listErr.Error()
		//nolint:nilerr // Strict handlers return typed error responses with a nil Go error.
		return ListComputeTargets500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: message}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	data := make([]ComputeTarget, 0, len(targets))
	for _, target := range targets {
		item := ComputeTarget{
			DisplayName:              target.DisplayName,
			IsDefault:                target.IsDefault,
			Mode:                     ComputeTargetMode(target.Mode),
			SelectableForInteractive: target.SelectableForInteractive,
			SelectableForScheduled:   target.SelectableForScheduled,
			Status:                   target.Status,
			SuitabilityLabels:        &target.SuitabilityLabels,
		}
		item.AvailabilityReason = target.AvailabilityReason
		if target.EndpointName != nil {
			item.EndpointName = target.EndpointName
		}
		if target.EndpointType != nil {
			endpointType := ComputeTargetEndpointType(*target.EndpointType)
			item.EndpointType = &endpointType
		}
		data = append(data, item)
	}

	return ListComputeTargets200JSONResponse{
		Body:    ComputeTargetList{Data: &data},
		Headers: ListComputeTargets200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetComputeRoutingDefaults implements the endpoint for reading routing defaults.
func (h *APIHandler) GetComputeRoutingDefaults(ctx context.Context, _ GetComputeRoutingDefaultsRequestObject) (GetComputeRoutingDefaultsResponseObject, error) {
	principal := principalFromCtx(ctx)
	defaults, err := h.computeEndpoints.GetRoutingDefaults(ctx, principal)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GetComputeRoutingDefaults403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GetComputeRoutingDefaults500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	return GetComputeRoutingDefaults200JSONResponse{
		Body:    computeRoutingDefaultsToAPI(*defaults),
		Headers: GetComputeRoutingDefaults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateComputeRoutingDefaults implements the endpoint for updating routing defaults.
func (h *APIHandler) UpdateComputeRoutingDefaults(ctx context.Context, req UpdateComputeRoutingDefaultsRequestObject) (UpdateComputeRoutingDefaultsResponseObject, error) {
	principal := principalFromCtx(ctx)
	defaults := domain.ComputeRoutingDefaults{}
	if req.Body.InteractiveMode != nil {
		defaults.InteractiveMode = string(*req.Body.InteractiveMode)
	}
	if req.Body.ScheduledMode != nil {
		defaults.ScheduledMode = string(*req.Body.ScheduledMode)
	}
	if req.Body.NotebookMode != nil {
		defaults.NotebookMode = string(*req.Body.NotebookMode)
	}

	updated, err := h.computeEndpoints.UpdateRoutingDefaults(ctx, principal, defaults)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateComputeRoutingDefaults403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateComputeRoutingDefaults400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return UpdateComputeRoutingDefaults500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	return UpdateComputeRoutingDefaults200JSONResponse{
		Body:    computeRoutingDefaultsToAPI(*updated),
		Headers: UpdateComputeRoutingDefaults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteComputeAssignment implements the endpoint for removing a compute assignment.
func (h *APIHandler) DeleteComputeAssignment(ctx context.Context, req GenDeleteComputeAssignmentRequest) (GenDeleteComputeAssignmentResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.computeEndpoints.Unassign(ctx, principal, req.AssignmentId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteComputeAssignment403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteComputeAssignment404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteComputeAssignment204Response{}, nil
}

// === Compute Endpoint Mappers ===

// computeEndpointToAPI converts a domain ComputeEndpoint to the API type.
// IMPORTANT: Never expose auth_token in API responses.
func computeEndpointToAPI(ep domain.ComputeEndpoint) ComputeEndpoint {
	ct := ep.CreatedAt
	ut := ep.UpdatedAt
	t := ComputeEndpointType(ep.Type)
	st := ComputeEndpointStatus(ep.Status)
	resp := ComputeEndpoint{
		Id:         &ep.ID,
		ExternalId: &ep.ExternalID,
		Name:       &ep.Name,
		Url:        &ep.URL,
		Type:       &t,
		Status:     &st,
		Owner:      &ep.Owner,
		CreatedAt:  formatTimePtr(&ct),
		UpdatedAt:  formatTimePtr(&ut),
	}
	policy := ComputeEndpointSelectionPolicy(ep.SelectionPolicy)
	workloadClass := ComputeEndpointWorkloadClass(ep.WorkloadClass)
	readinessStatus := ComputeEndpointReadinessStatus(ep.ReadinessStatus)
	resp.SelectionPolicy = &policy
	resp.WorkloadClass = &workloadClass
	resp.ReadinessStatus = &readinessStatus
	if ep.Size != "" {
		s := ComputeEndpointSize(ep.Size)
		resp.Size = &s
	}
	if ep.MaxMemoryGB != nil {
		resp.MaxMemoryGb = safeInt64ToInt32Ptr(ep.MaxMemoryGB)
	}
	if ep.MaxConcurrency != nil {
		resp.MaxConcurrency = ep.MaxConcurrency
	}
	if ep.MaxResultSizeMB != nil {
		resp.MaxResultSizeMb = ep.MaxResultSizeMB
	}
	resp.RecommendedForLargeQueries = &ep.RecommendedForLargeQueries
	resp.IsDraining = &ep.IsDraining
	resp.LastHealthStatus = ep.LastHealthStatus
	resp.LastHealthCheckedAt = ep.LastHealthCheckedAt
	resp.ActiveQueries = ep.ActiveQueries
	resp.QueuedJobs = ep.QueuedJobs
	resp.RunningJobs = ep.RunningJobs
	resp.CompletedJobs = ep.CompletedJobs
	resp.StoredJobs = ep.StoredJobs
	resp.CleanedJobs = ep.CleanedJobs
	resp.QueryResultTtlSeconds = ep.QueryResultTTLSeconds
	return resp
}

func computeEndpointStatusSummaryToAPI(ep domain.ComputeEndpoint) ComputeEndpointStatusSummary {
	return ComputeEndpointStatusSummary(computeEndpointToAPI(ep))
}

func computeAssignmentToAPI(a domain.ComputeAssignment) ComputeAssignment {
	ct := a.CreatedAt
	pt := ComputeAssignmentPrincipalType(a.PrincipalType)
	return ComputeAssignment{
		Id:            &a.ID,
		PrincipalId:   &a.PrincipalID,
		PrincipalType: &pt,
		EndpointId:    &a.EndpointID,
		EndpointName:  optStr(a.EndpointName),
		IsDefault:     &a.IsDefault,
		FallbackLocal: &a.FallbackLocal,
		CreatedAt:     formatTimePtr(&ct),
	}
}

func computeRoutingDefaultsToAPI(defaults domain.ComputeRoutingDefaults) ComputeRoutingDefaults {
	return ComputeRoutingDefaults{
		InteractiveMode: ComputeRoutingDefaultsInteractiveMode(defaults.InteractiveMode),
		ScheduledMode:   ComputeRoutingDefaultsScheduledMode(defaults.ScheduledMode),
		NotebookMode:    ComputeRoutingDefaultsNotebookMode(defaults.NotebookMode),
	}
}

func intPtrToInt32Ptr(value *int) *int32 {
	if value == nil {
		return nil
	}
	v := safeIntToInt32(*value)
	return &v
}
