package api

import (
	"context"

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
		if resp, ok := respondDomainErrorForOperation[GenListComputeEndpointsResponse]("listComputeEndpoints", err, domainErrorResponder[GenListComputeEndpointsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListComputeEndpointsResponse {
				return GenListComputeEndpoints403JSONResponse{GenForbiddenJSONResponse(resp)}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListComputeEndpointsResponse {
				return GenListComputeEndpoints500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
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
	if req.Body.Size != nil {
		domReq.Size = string(*req.Body.Size)
	}
	if req.Body.MaxMemoryGb != nil {
		domReq.MaxMemoryGB = int32PtrToInt64Ptr(req.Body.MaxMemoryGb)
	}
	if req.Body.AuthToken != nil {
		domReq.AuthToken = *req.Body.AuthToken
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.computeEndpoints.Create(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateComputeEndpointResponse]("createComputeEndpoint", err, domainErrorResponder[GenCreateComputeEndpointResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateComputeEndpointResponse {
				return CreateComputeEndpoint400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateComputeEndpointResponse {
				return CreateComputeEndpoint403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateComputeEndpointResponse {
				return CreateComputeEndpoint409JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenCreateComputeEndpointResponse {
				return GenCreateComputeEndpoint500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
		if resp, ok := respondDomainErrorForOperation[GenGetComputeEndpointResponse]("getComputeEndpoint", err, domainErrorResponder[GenGetComputeEndpointResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetComputeEndpointResponse {
				return GenGetComputeEndpoint404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetComputeEndpointResponse {
				return GenGetComputeEndpoint403JSONResponse{GenForbiddenJSONResponse(resp)}
			},
			Internal: func(resp InternalErrorJSONResponse) GenGetComputeEndpointResponse {
				return GenGetComputeEndpoint500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetComputeEndpoint200JSONResponse{
		Body:    computeEndpointToAPI(*result),
		Headers: GenGetComputeEndpoint200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateComputeEndpoint implements the endpoint for updating a compute endpoint.
func (h *APIHandler) UpdateComputeEndpoint(ctx context.Context, req GenUpdateComputeEndpointRequest) (GenUpdateComputeEndpointResponse, error) {
	domReq := domain.UpdateComputeEndpointRequest{
		URL:         req.Body.Url,
		MaxMemoryGB: int32PtrToInt64Ptr(req.Body.MaxMemoryGb),
		AuthToken:   req.Body.AuthToken,
	}
	if req.Body.Size != nil {
		s := string(*req.Body.Size)
		domReq.Size = &s
	}
	if req.Body.Status != nil {
		s := string(*req.Body.Status)
		domReq.Status = &s
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.computeEndpoints.Update(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateComputeEndpointResponse]("updateComputeEndpoint", err, domainErrorResponder[GenUpdateComputeEndpointResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateComputeEndpointResponse {
				return UpdateComputeEndpoint403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateComputeEndpointResponse {
				return UpdateComputeEndpoint404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenUpdateComputeEndpointResponse {
				return GenUpdateComputeEndpoint500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateComputeEndpoint200JSONResponse{
		Body:    computeEndpointToAPI(*result),
		Headers: GenUpdateComputeEndpoint200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteComputeEndpoint implements the endpoint for deleting a compute endpoint.
func (h *APIHandler) DeleteComputeEndpoint(ctx context.Context, req GenDeleteComputeEndpointRequest) (GenDeleteComputeEndpointResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.computeEndpoints.Delete(ctx, principal, req.EndpointName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteComputeEndpointResponse]("deleteComputeEndpoint", err, domainErrorResponder[GenDeleteComputeEndpointResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteComputeEndpointResponse {
				return DeleteComputeEndpoint403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteComputeEndpointResponse {
				return DeleteComputeEndpoint404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenDeleteComputeEndpointResponse {
				return GenDeleteComputeEndpoint500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteComputeEndpoint204Response{}, nil
}

// GetComputeRoutingDefaults implements the endpoint for retrieving global compute routing defaults.
func (h *APIHandler) GetComputeRoutingDefaults(ctx context.Context, _ GenGetComputeRoutingDefaultsRequest) (GenGetComputeRoutingDefaultsResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.GetRoutingDefaults(ctx, cp.Name)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetComputeRoutingDefaultsResponse]("getComputeRoutingDefaults", err, domainErrorResponder[GenGetComputeRoutingDefaultsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetComputeRoutingDefaultsResponse {
				return GenGetComputeRoutingDefaults400JSONResponse{GenBadRequestJSONResponse(resp)}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetComputeRoutingDefaultsResponse {
				return GenGetComputeRoutingDefaults403JSONResponse{GenForbiddenJSONResponse(resp)}
			},
			Internal: func(resp InternalErrorJSONResponse) GenGetComputeRoutingDefaultsResponse {
				return GenGetComputeRoutingDefaults500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetComputeRoutingDefaults200JSONResponse{
		Body: ComputeRoutingDefaults{
			InteractiveMode: optStr(result.InteractiveMode),
			ScheduledMode:   optStr(result.ScheduledMode),
			NotebookMode:    optStr(result.NotebookMode),
		},
		Headers: GenGetComputeRoutingDefaults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateComputeRoutingDefaults implements the endpoint for updating global compute routing defaults.
func (h *APIHandler) UpdateComputeRoutingDefaults(ctx context.Context, req GenUpdateComputeRoutingDefaultsRequest) (GenUpdateComputeRoutingDefaultsResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	defaults := domain.ComputeRoutingDefaults{
		InteractiveMode: derefString(req.Body.InteractiveMode),
		ScheduledMode:   derefString(req.Body.ScheduledMode),
		NotebookMode:    derefString(req.Body.NotebookMode),
	}
	result, err := h.computeEndpoints.UpdateRoutingDefaults(ctx, cp.Name, defaults)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateComputeRoutingDefaultsResponse]("updateComputeRoutingDefaults", err, domainErrorResponder[GenUpdateComputeRoutingDefaultsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateComputeRoutingDefaultsResponse {
				return UpdateComputeRoutingDefaults400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateComputeRoutingDefaultsResponse {
				return UpdateComputeRoutingDefaults403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateComputeRoutingDefaults200JSONResponse{
		Body: ComputeRoutingDefaults{
			InteractiveMode: optStr(result.InteractiveMode),
			ScheduledMode:   optStr(result.ScheduledMode),
			NotebookMode:    optStr(result.NotebookMode),
		},
		Headers: GenUpdateComputeRoutingDefaults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListComputeAssignments implements the endpoint for listing assignments for a compute endpoint.
func (h *APIHandler) ListComputeAssignments(ctx context.Context, req GenListComputeAssignmentsRequest) (GenListComputeAssignmentsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	principal := principalFromCtx(ctx)
	assignments, total, err := h.computeEndpoints.ListAssignments(ctx, principal, req.EndpointName, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListComputeAssignmentsResponse]("listComputeAssignments", err, domainErrorResponder[GenListComputeAssignmentsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListComputeAssignmentsResponse {
				return GenListComputeAssignments404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListComputeAssignmentsResponse {
				return GenListComputeAssignments403JSONResponse{GenForbiddenJSONResponse(resp)}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListComputeAssignmentsResponse {
				return GenListComputeAssignments500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
		if resp, ok := respondDomainErrorForOperation[GenCreateComputeAssignmentResponse]("createComputeAssignment", err, domainErrorResponder[GenCreateComputeAssignmentResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateComputeAssignmentResponse {
				return CreateComputeAssignment400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateComputeAssignmentResponse {
				return CreateComputeAssignment403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateComputeAssignmentResponse {
				return CreateComputeAssignment409JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenCreateComputeAssignmentResponse {
				return GenCreateComputeAssignment500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
		if resp, ok := respondDomainErrorForOperation[GenGetComputeEndpointHealthResponse]("getComputeEndpointHealth", err, domainErrorResponder[GenGetComputeEndpointHealthResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetComputeEndpointHealthResponse {
				return GetComputeEndpointHealth400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetComputeEndpointHealthResponse {
				return GetComputeEndpointHealth403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetComputeEndpointHealthResponse {
				return GenGetComputeEndpointHealth404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return GetComputeEndpointHealth502JSONResponse{
			GenInternalErrorJSONResponse{
				Body:    errorBodyWithCode(502, err),
				Headers: GenGetComputeEndpointHealth502ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
			},
		}, nil
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
			Status:        result.Status,
			UptimeSeconds: uptimeSeconds,
			DuckdbVersion: result.DuckdbVersion,
			MemoryUsedMb:  memoryUsedMb,
			MaxMemoryGb:   maxMemoryGb,
			EndpointName:  &req.EndpointName,
		},
		Headers: GenGetComputeEndpointHealth200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteComputeAssignment implements the endpoint for removing a compute assignment.
func (h *APIHandler) DeleteComputeAssignment(ctx context.Context, req GenDeleteComputeAssignmentRequest) (GenDeleteComputeAssignmentResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.computeEndpoints.Unassign(ctx, principal, req.AssignmentId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteComputeAssignmentResponse]("deleteComputeAssignment", err, domainErrorResponder[GenDeleteComputeAssignmentResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteComputeAssignmentResponse {
				return DeleteComputeAssignment403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteComputeAssignmentResponse {
				return DeleteComputeAssignment404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenDeleteComputeAssignmentResponse {
				return GenDeleteComputeAssignment500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
	if ep.Size != "" {
		s := ComputeEndpointSize(ep.Size)
		resp.Size = &s
	}
	if ep.MaxMemoryGB != nil {
		resp.MaxMemoryGb = safeInt64ToInt32Ptr(ep.MaxMemoryGB)
	}
	return resp
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
