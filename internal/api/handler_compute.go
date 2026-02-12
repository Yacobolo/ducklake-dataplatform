package api

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// === Compute Endpoints ===

func (h *APIHandler) ListComputeEndpoints(ctx context.Context, req ListComputeEndpointsRequestObject) (ListComputeEndpointsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	eps, total, err := h.computeEndpoints.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]ComputeEndpoint, len(eps))
	for i, ep := range eps {
		data[i] = computeEndpointToAPI(ep)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListComputeEndpoints200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateComputeEndpoint(ctx context.Context, req CreateComputeEndpointRequestObject) (CreateComputeEndpointResponseObject, error) {
	domReq := domain.CreateComputeEndpointRequest{
		Name: req.Body.Name,
		URL:  req.Body.Url,
		Type: string(req.Body.Type),
	}
	if req.Body.Size != nil {
		domReq.Size = string(*req.Body.Size)
	}
	if req.Body.MaxMemoryGb != nil {
		domReq.MaxMemoryGB = req.Body.MaxMemoryGb
	}
	if req.Body.AuthToken != nil {
		domReq.AuthToken = *req.Body.AuthToken
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.Create(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateComputeEndpoint403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateComputeEndpoint400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateComputeEndpoint409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateComputeEndpoint400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateComputeEndpoint201JSONResponse(computeEndpointToAPI(*result)), nil
}

func (h *APIHandler) GetComputeEndpoint(ctx context.Context, req GetComputeEndpointRequestObject) (GetComputeEndpointResponseObject, error) {
	result, err := h.computeEndpoints.GetByName(ctx, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetComputeEndpoint404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetComputeEndpoint200JSONResponse(computeEndpointToAPI(*result)), nil
}

func (h *APIHandler) UpdateComputeEndpoint(ctx context.Context, req UpdateComputeEndpointRequestObject) (UpdateComputeEndpointResponseObject, error) {
	domReq := domain.UpdateComputeEndpointRequest{
		URL:         req.Body.Url,
		MaxMemoryGB: req.Body.MaxMemoryGb,
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.Update(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateComputeEndpoint403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateComputeEndpoint404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateComputeEndpoint200JSONResponse(computeEndpointToAPI(*result)), nil
}

func (h *APIHandler) DeleteComputeEndpoint(ctx context.Context, req DeleteComputeEndpointRequestObject) (DeleteComputeEndpointResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.computeEndpoints.Delete(ctx, principal, req.EndpointName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteComputeEndpoint403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteComputeEndpoint404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteComputeEndpoint204Response{}, nil
}

func (h *APIHandler) ListComputeAssignments(ctx context.Context, req ListComputeAssignmentsRequestObject) (ListComputeAssignmentsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	assignments, total, err := h.computeEndpoints.ListAssignments(ctx, req.EndpointName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListComputeAssignments404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}

	data := make([]ComputeAssignment, len(assignments))
	for i, a := range assignments {
		data[i] = computeAssignmentToAPI(a)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListComputeAssignments200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateComputeAssignment(ctx context.Context, req CreateComputeAssignmentRequestObject) (CreateComputeAssignmentResponseObject, error) {
	domReq := domain.CreateComputeAssignmentRequest{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
	}
	if req.Body.IsDefault != nil {
		domReq.IsDefault = *req.Body.IsDefault
	} else {
		domReq.IsDefault = true
	}
	if req.Body.FallbackLocal != nil {
		domReq.FallbackLocal = *req.Body.FallbackLocal
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.Assign(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateComputeAssignment403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateComputeAssignment400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateComputeAssignment409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateComputeAssignment400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateComputeAssignment201JSONResponse(computeAssignmentToAPI(*result)), nil
}

func (h *APIHandler) GetComputeEndpointHealth(ctx context.Context, req GetComputeEndpointHealthRequestObject) (GetComputeEndpointHealthResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	result, err := h.computeEndpoints.HealthCheck(ctx, principal, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GetComputeEndpointHealth403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GetComputeEndpointHealth404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return GetComputeEndpointHealth502JSONResponse{Code: 502, Message: err.Error()}, nil
		}
	}

	return GetComputeEndpointHealth200JSONResponse{
		Status:        result.Status,
		UptimeSeconds: result.UptimeSeconds,
		DuckdbVersion: result.DuckdbVersion,
		MemoryUsedMb:  result.MemoryUsedMb,
		MaxMemoryGb:   result.MaxMemoryGb,
		EndpointName:  &req.EndpointName,
	}, nil
}

func (h *APIHandler) DeleteComputeAssignment(ctx context.Context, req DeleteComputeAssignmentRequestObject) (DeleteComputeAssignmentResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.computeEndpoints.Unassign(ctx, principal, req.AssignmentId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteComputeAssignment403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteComputeAssignment404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteComputeAssignment204Response{}, nil
}

// === Compute Endpoint Mappers ===

// computeEndpointToAPI converts a domain ComputeEndpoint to the API type.
// IMPORTANT: Never expose auth_token in API responses.
func computeEndpointToAPI(ep domain.ComputeEndpoint) ComputeEndpoint {
	ct := ep.CreatedAt
	ut := ep.UpdatedAt
	t := ComputeEndpointType(ep.Type)
	st := ComputeEndpointStatus(ep.Status)
	extID, _ := uuid.Parse(ep.ExternalID)
	resp := ComputeEndpoint{
		Id:         &ep.ID,
		ExternalId: &extID,
		Name:       &ep.Name,
		Url:        &ep.URL,
		Type:       &t,
		Status:     &st,
		Owner:      &ep.Owner,
		CreatedAt:  &ct,
		UpdatedAt:  &ut,
	}
	if ep.Size != "" {
		s := ComputeEndpointSize(ep.Size)
		resp.Size = &s
	}
	if ep.MaxMemoryGB != nil {
		resp.MaxMemoryGb = ep.MaxMemoryGB
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
		CreatedAt:     &ct,
	}
}
