package api

import (
	"context"
	"errors"
	"math"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/semantic"
)

// semanticService defines semantic layer operations used by the API handler.
type semanticService interface {
	CreateSemanticModel(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error)
	GetSemanticModel(ctx context.Context, projectName, name string) (*domain.SemanticModel, error)
	ListSemanticModels(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.SemanticModel, int64, error)
	UpdateSemanticModel(ctx context.Context, projectName, name string, req domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error)
	DeleteSemanticModel(ctx context.Context, projectName, name string) error

	CreateMetric(ctx context.Context, principal, projectName, semanticModelName string, req domain.CreateSemanticMetricRequest) (*domain.SemanticMetric, error)
	ListMetrics(ctx context.Context, projectName, semanticModelName string) ([]domain.SemanticMetric, error)
	UpdateMetric(ctx context.Context, projectName, semanticModelName, metricName string, req domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error)
	DeleteMetric(ctx context.Context, projectName, semanticModelName, metricName string) error

	CreatePreAggregation(ctx context.Context, principal, projectName, semanticModelName string, req domain.CreateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error)
	ListPreAggregations(ctx context.Context, projectName, semanticModelName string) ([]domain.SemanticPreAggregation, error)
	UpdatePreAggregation(ctx context.Context, projectName, semanticModelName, preAggName string, req domain.UpdateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error)
	DeletePreAggregation(ctx context.Context, projectName, semanticModelName, preAggName string) error

	CreateRelationship(ctx context.Context, principal string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error)
	ListRelationships(ctx context.Context, page domain.PageRequest) ([]domain.SemanticRelationship, int64, error)
	UpdateRelationship(ctx context.Context, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error)
	DeleteRelationship(ctx context.Context, relationshipName string) error

	ExplainMetricQuery(ctx context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error)
	RunMetricQuery(ctx context.Context, principal string, req semantic.MetricQueryRequest) (*semantic.MetricQueryResult, error)
}

// ListSemanticModels lists semantic models.
func (h *APIHandler) ListSemanticModels(ctx context.Context, req ListSemanticModelsRequestObject) (ListSemanticModelsResponseObject, error) {
	if isNilService(h.semantics) {
		empty := []SemanticModel{}
		return ListSemanticModels200JSONResponse(PaginatedSemanticModels{Data: &empty, NextPageToken: nil}), nil
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	models, total, err := h.semantics.ListSemanticModels(ctx, req.Params.ProjectName, page)
	if err != nil {
		return nil, err
	}

	data := make([]SemanticModel, len(models))
	for i, m := range models {
		data[i] = semanticModelToAPI(m)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListSemanticModels200JSONResponse(PaginatedSemanticModels{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

// CreateSemanticModel creates a semantic model.
func (h *APIHandler) CreateSemanticModel(ctx context.Context, req CreateSemanticModelRequestObject) (CreateSemanticModelResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.CreateSemanticModel(ctx, cp.Name, domain.CreateSemanticModelRequest{
		ProjectName:          req.Body.ProjectName,
		Name:                 req.Body.Name,
		Description:          valOrEmpty(req.Body.Description),
		BaseModelRef:         req.Body.BaseModelRef,
		DefaultTimeDimension: valOrEmpty(req.Body.DefaultTimeDimension),
		Tags:                 sliceOrEmpty(req.Body.Tags),
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateSemanticModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateSemanticModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateSemanticModel409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return CreateSemanticModel201JSONResponse(semanticModelToAPI(*result)), nil
}

// GetSemanticModel retrieves a semantic model.
func (h *APIHandler) GetSemanticModel(ctx context.Context, req GetSemanticModelRequestObject) (GetSemanticModelResponseObject, error) {
	result, err := h.semantics.GetSemanticModel(ctx, req.ProjectName, req.SemanticModelName)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return GetSemanticModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}
	return GetSemanticModel200JSONResponse(semanticModelToAPI(*result)), nil
}

// UpdateSemanticModel updates a semantic model.
func (h *APIHandler) UpdateSemanticModel(ctx context.Context, req UpdateSemanticModelRequestObject) (UpdateSemanticModelResponseObject, error) {
	domReq := domain.UpdateSemanticModelRequest{
		Description:          req.Body.Description,
		Owner:                req.Body.Owner,
		BaseModelRef:         req.Body.BaseModelRef,
		DefaultTimeDimension: req.Body.DefaultTimeDimension,
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}

	result, err := h.semantics.UpdateSemanticModel(ctx, req.ProjectName, req.SemanticModelName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateSemanticModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateSemanticModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateSemanticModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return UpdateSemanticModel200JSONResponse(semanticModelToAPI(*result)), nil
}

// DeleteSemanticModel deletes a semantic model.
func (h *APIHandler) DeleteSemanticModel(ctx context.Context, req DeleteSemanticModelRequestObject) (DeleteSemanticModelResponseObject, error) {
	if err := h.semantics.DeleteSemanticModel(ctx, req.ProjectName, req.SemanticModelName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteSemanticModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteSemanticModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return DeleteSemanticModel204Response{}, nil
}

// ListSemanticMetrics lists metrics under a semantic model.
func (h *APIHandler) ListSemanticMetrics(ctx context.Context, req ListSemanticMetricsRequestObject) (ListSemanticMetricsResponseObject, error) {
	if isNilService(h.semantics) {
		empty := []SemanticMetric{}
		return ListSemanticMetrics200JSONResponse(SemanticMetricList{Data: &empty}), nil
	}

	items, err := h.semantics.ListMetrics(ctx, req.ProjectName, req.SemanticModelName)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListSemanticMetrics404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}
	data := make([]SemanticMetric, len(items))
	for i, item := range items {
		data[i] = semanticMetricToAPI(item)
	}
	return ListSemanticMetrics200JSONResponse(SemanticMetricList{Data: &data}), nil
}

// CreateSemanticMetric creates a metric under a semantic model.
func (h *APIHandler) CreateSemanticMetric(ctx context.Context, req CreateSemanticMetricRequestObject) (CreateSemanticMetricResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	domReq := domain.CreateSemanticMetricRequest{
		SemanticModelID:    "",
		Name:               req.Body.Name,
		Description:        valOrEmpty(req.Body.Description),
		MetricType:         string(req.Body.MetricType),
		Expression:         req.Body.Expression,
		DefaultTimeGrain:   valOrEmpty(req.Body.DefaultTimeGrain),
		Format:             valOrEmpty(req.Body.Format),
		CertificationState: certificationOrDefault(req.Body.CertificationState),
	}
	if req.Body.ExpressionMode != nil {
		domReq.ExpressionMode = string(*req.Body.ExpressionMode)
	}

	result, err := h.semantics.CreateMetric(ctx, cp.Name, req.ProjectName, req.SemanticModelName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateSemanticMetric403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateSemanticMetric404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateSemanticMetric400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateSemanticMetric409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return CreateSemanticMetric201JSONResponse(semanticMetricToAPI(*result)), nil
}

// UpdateSemanticMetric updates a metric under a semantic model.
func (h *APIHandler) UpdateSemanticMetric(ctx context.Context, req UpdateSemanticMetricRequestObject) (UpdateSemanticMetricResponseObject, error) {
	domReq := domain.UpdateSemanticMetricRequest{
		Description:      req.Body.Description,
		Expression:       req.Body.Expression,
		DefaultTimeGrain: req.Body.DefaultTimeGrain,
		Format:           req.Body.Format,
		Owner:            req.Body.Owner,
	}
	if req.Body.MetricType != nil {
		s := string(*req.Body.MetricType)
		domReq.MetricType = &s
	}
	if req.Body.ExpressionMode != nil {
		s := string(*req.Body.ExpressionMode)
		domReq.ExpressionMode = &s
	}
	if req.Body.CertificationState != nil {
		s := string(*req.Body.CertificationState)
		domReq.CertificationState = &s
	}

	result, err := h.semantics.UpdateMetric(ctx, req.ProjectName, req.SemanticModelName, req.MetricName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateSemanticMetric403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateSemanticMetric404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateSemanticMetric400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	return UpdateSemanticMetric200JSONResponse(semanticMetricToAPI(*result)), nil
}

// DeleteSemanticMetric deletes a metric under a semantic model.
func (h *APIHandler) DeleteSemanticMetric(ctx context.Context, req DeleteSemanticMetricRequestObject) (DeleteSemanticMetricResponseObject, error) {
	if err := h.semantics.DeleteMetric(ctx, req.ProjectName, req.SemanticModelName, req.MetricName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteSemanticMetric403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteSemanticMetric404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteSemanticMetric204Response{}, nil
}

// ListSemanticPreAggregations lists pre-aggregations under a semantic model.
func (h *APIHandler) ListSemanticPreAggregations(ctx context.Context, req ListSemanticPreAggregationsRequestObject) (ListSemanticPreAggregationsResponseObject, error) {
	if isNilService(h.semantics) {
		empty := []SemanticPreAggregation{}
		return ListSemanticPreAggregations200JSONResponse(SemanticPreAggregationList{Data: &empty}), nil
	}

	items, err := h.semantics.ListPreAggregations(ctx, req.ProjectName, req.SemanticModelName)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return ListSemanticPreAggregations404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}
	data := make([]SemanticPreAggregation, len(items))
	for i, item := range items {
		data[i] = semanticPreAggregationToAPI(item)
	}
	return ListSemanticPreAggregations200JSONResponse(SemanticPreAggregationList{Data: &data}), nil
}

// CreateSemanticPreAggregation creates a pre-aggregation under a semantic model.
func (h *APIHandler) CreateSemanticPreAggregation(ctx context.Context, req CreateSemanticPreAggregationRequestObject) (CreateSemanticPreAggregationResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.CreatePreAggregation(ctx, cp.Name, req.ProjectName, req.SemanticModelName, domain.CreateSemanticPreAggregationRequest{
		SemanticModelID: "",
		Name:            req.Body.Name,
		MetricSet:       sliceOrEmpty(req.Body.MetricSet),
		DimensionSet:    sliceOrEmpty(req.Body.DimensionSet),
		Grain:           valOrEmpty(req.Body.Grain),
		TargetRelation:  req.Body.TargetRelation,
		RefreshPolicy:   valOrEmpty(req.Body.RefreshPolicy),
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateSemanticPreAggregation403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateSemanticPreAggregation404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateSemanticPreAggregation400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateSemanticPreAggregation409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateSemanticPreAggregation201JSONResponse(semanticPreAggregationToAPI(*result)), nil
}

// UpdateSemanticPreAggregation updates a pre-aggregation under a semantic model.
func (h *APIHandler) UpdateSemanticPreAggregation(ctx context.Context, req UpdateSemanticPreAggregationRequestObject) (UpdateSemanticPreAggregationResponseObject, error) {
	domReq := domain.UpdateSemanticPreAggregationRequest{Grain: req.Body.Grain, TargetRelation: req.Body.TargetRelation, RefreshPolicy: req.Body.RefreshPolicy}
	if req.Body.MetricSet != nil {
		domReq.MetricSet = *req.Body.MetricSet
	}
	if req.Body.DimensionSet != nil {
		domReq.DimensionSet = *req.Body.DimensionSet
	}
	result, err := h.semantics.UpdatePreAggregation(ctx, req.ProjectName, req.SemanticModelName, req.PreAggregationName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateSemanticPreAggregation403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateSemanticPreAggregation404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateSemanticPreAggregation400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateSemanticPreAggregation200JSONResponse(semanticPreAggregationToAPI(*result)), nil
}

// DeleteSemanticPreAggregation deletes a pre-aggregation under a semantic model.
func (h *APIHandler) DeleteSemanticPreAggregation(ctx context.Context, req DeleteSemanticPreAggregationRequestObject) (DeleteSemanticPreAggregationResponseObject, error) {
	if err := h.semantics.DeletePreAggregation(ctx, req.ProjectName, req.SemanticModelName, req.PreAggregationName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteSemanticPreAggregation403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteSemanticPreAggregation404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteSemanticPreAggregation204Response{}, nil
}

// ListSemanticRelationships lists semantic relationships.
func (h *APIHandler) ListSemanticRelationships(ctx context.Context, req ListSemanticRelationshipsRequestObject) (ListSemanticRelationshipsResponseObject, error) {
	if isNilService(h.semantics) {
		empty := []SemanticRelationship{}
		return ListSemanticRelationships200JSONResponse(PaginatedSemanticRelationships{Data: &empty, NextPageToken: nil}), nil
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	rels, total, err := h.semantics.ListRelationships(ctx, page)
	if err != nil {
		return nil, err
	}
	data := make([]SemanticRelationship, len(rels))
	for i, rel := range rels {
		data[i] = semanticRelationshipToAPI(rel)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListSemanticRelationships200JSONResponse(PaginatedSemanticRelationships{Data: &data, NextPageToken: optStr(nextToken)}), nil
}

// CreateSemanticRelationship creates a semantic relationship.
func (h *APIHandler) CreateSemanticRelationship(ctx context.Context, req CreateSemanticRelationshipRequestObject) (CreateSemanticRelationshipResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.CreateRelationship(ctx, cp.Name, domain.CreateSemanticRelationshipRequest{
		Name:             req.Body.Name,
		FromSemanticID:   req.Body.FromSemanticId,
		ToSemanticID:     req.Body.ToSemanticId,
		RelationshipType: string(req.Body.RelationshipType),
		JoinSQL:          req.Body.JoinSql,
		IsDefault:        boolOrFalse(req.Body.IsDefault),
		Cost:             intOrZero(req.Body.Cost),
		MaxHops:          intOrZero(req.Body.MaxHops),
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateSemanticRelationship403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateSemanticRelationship400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateSemanticRelationship409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateSemanticRelationship201JSONResponse(semanticRelationshipToAPI(*result)), nil
}

// UpdateSemanticRelationship updates a semantic relationship.
func (h *APIHandler) UpdateSemanticRelationship(ctx context.Context, req UpdateSemanticRelationshipRequestObject) (UpdateSemanticRelationshipResponseObject, error) {
	domReq := domain.UpdateSemanticRelationshipRequest{JoinSQL: req.Body.JoinSql}
	if req.Body.RelationshipType != nil {
		s := string(*req.Body.RelationshipType)
		domReq.RelationshipType = &s
	}
	if req.Body.IsDefault != nil {
		domReq.IsDefault = req.Body.IsDefault
	}
	if req.Body.Cost != nil {
		v := int(*req.Body.Cost)
		domReq.Cost = &v
	}
	if req.Body.MaxHops != nil {
		v := int(*req.Body.MaxHops)
		domReq.MaxHops = &v
	}

	result, err := h.semantics.UpdateRelationship(ctx, req.RelationshipName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateSemanticRelationship403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateSemanticRelationship404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateSemanticRelationship400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateSemanticRelationship200JSONResponse(semanticRelationshipToAPI(*result)), nil
}

// DeleteSemanticRelationship deletes a semantic relationship.
func (h *APIHandler) DeleteSemanticRelationship(ctx context.Context, req DeleteSemanticRelationshipRequestObject) (DeleteSemanticRelationshipResponseObject, error) {
	if err := h.semantics.DeleteRelationship(ctx, req.RelationshipName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteSemanticRelationship403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteSemanticRelationship404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteSemanticRelationship204Response{}, nil
}

// CheckMetricFreshness resolves a metric and returns its current freshness metadata.
func (h *APIHandler) CheckMetricFreshness(ctx context.Context, req CheckMetricFreshnessRequestObject) (CheckMetricFreshnessResponseObject, error) {
	if isNilService(h.semantics) {
		return CheckMetricFreshness404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: "semantic service is not configured"}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	models, _, err := h.semantics.ListSemanticModels(ctx, req.Params.ProjectName, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return nil, err
	}

	type metricMatch struct {
		projectName       string
		semanticModelName string
	}

	matches := make([]metricMatch, 0, 1)
	for _, model := range models {
		if req.Params.SemanticModelName != nil && model.Name != *req.Params.SemanticModelName {
			continue
		}
		metrics, listErr := h.semantics.ListMetrics(ctx, model.ProjectName, model.Name)
		if listErr != nil {
			return nil, listErr
		}
		for _, metric := range metrics {
			if metric.Name == req.MetricName {
				matches = append(matches, metricMatch{projectName: model.ProjectName, semanticModelName: model.Name})
			}
		}
	}

	if len(matches) == 0 {
		err = domain.ErrNotFound("metric %q not found", req.MetricName)
		return CheckMetricFreshness404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	if len(matches) > 1 {
		err = domain.ErrValidation("metric %q is ambiguous; provide project_name and semantic_model_name", req.MetricName)
		return CheckMetricFreshness400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	match := matches[0]
	plan, err := h.semantics.ExplainMetricQuery(ctx, semantic.MetricQueryRequest{
		ProjectName:       match.projectName,
		SemanticModelName: match.semanticModelName,
		Metrics:           []string{req.MetricName},
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CheckMetricFreshness400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CheckMetricFreshness404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	basis := append([]string(nil), plan.FreshnessBasis...)
	checkedAt := time.Now().UTC()
	return CheckMetricFreshness200JSONResponse(MetricFreshnessStatus{
		MetricName:             &req.MetricName,
		ProjectName:            &match.projectName,
		SemanticModelName:      &match.semanticModelName,
		FreshnessStatus:        optStr(plan.FreshnessStatus),
		FreshnessBasis:         &basis,
		SelectedPreAggregation: plan.SelectedPreAggregation,
		CheckedAt:              &checkedAt,
	}), nil
}

// ExplainMetricQuery compiles a semantic metric query without executing it.
func (h *APIHandler) ExplainMetricQuery(ctx context.Context, req ExplainMetricQueryRequestObject) (ExplainMetricQueryResponseObject, error) {
	plan, err := h.semantics.ExplainMetricQuery(ctx, semanticReqToService(req.Body))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return ExplainMetricQuery400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return ExplainMetricQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	apiPlan := metricQueryPlanToAPI(*plan)
	return ExplainMetricQuery200JSONResponse(MetricQueryExplainResponse{Plan: &apiPlan}), nil
}

// RunMetricQuery compiles and executes a semantic metric query.
func (h *APIHandler) RunMetricQuery(ctx context.Context, req RunMetricQueryRequestObject) (RunMetricQueryResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.RunMetricQuery(ctx, cp.Name, semanticReqToService(req.Body))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return RunMetricQuery403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return RunMetricQuery400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return RunMetricQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	apiPlan := metricQueryPlanToAPI(result.Plan)
	apiResult := QueryResult{
		Columns:  &result.Result.Columns,
		Rows:     &result.Result.Rows,
		RowCount: ptrInt64(int64(result.Result.RowCount)),
	}
	return RunMetricQuery200JSONResponse(MetricQueryRunResponse{Plan: &apiPlan, Result: &apiResult}), nil
}

func semanticModelToAPI(m domain.SemanticModel) SemanticModel {
	return SemanticModel{
		Id:                   optStr(m.ID),
		ProjectName:          optStr(m.ProjectName),
		Name:                 optStr(m.Name),
		Description:          optStr(m.Description),
		Owner:                optStr(m.Owner),
		BaseModelRef:         optStr(m.BaseModelRef),
		DefaultTimeDimension: optStr(m.DefaultTimeDimension),
		Tags:                 &m.Tags,
		CreatedBy:            optStr(m.CreatedBy),
		CreatedAt:            optTime(m.CreatedAt),
		UpdatedAt:            optTime(m.UpdatedAt),
	}
}

func semanticMetricToAPI(m domain.SemanticMetric) SemanticMetric {
	return SemanticMetric{
		Id:                 optStr(m.ID),
		SemanticModelId:    optStr(m.SemanticModelID),
		Name:               optStr(m.Name),
		Description:        optStr(m.Description),
		MetricType:         metricTypePtr(m.MetricType),
		ExpressionMode:     expressionModePtr(m.ExpressionMode),
		Expression:         optStr(m.Expression),
		DefaultTimeGrain:   optStr(m.DefaultTimeGrain),
		Format:             optStr(m.Format),
		Owner:              optStr(m.Owner),
		CertificationState: optCertificationState(m.CertificationState),
		CreatedBy:          optStr(m.CreatedBy),
		CreatedAt:          optTime(m.CreatedAt),
		UpdatedAt:          optTime(m.UpdatedAt),
	}
}

func semanticRelationshipToAPI(r domain.SemanticRelationship) SemanticRelationship {
	return SemanticRelationship{
		Id:               optStr(r.ID),
		Name:             optStr(r.Name),
		FromSemanticId:   optStr(r.FromSemanticID),
		ToSemanticId:     optStr(r.ToSemanticID),
		RelationshipType: relationshipTypePtr(r.RelationshipType),
		JoinSql:          optStr(r.JoinSQL),
		IsDefault:        &r.IsDefault,
		Cost:             ptrI32(intToI32Safe(r.Cost)),
		MaxHops:          ptrI32(intToI32Safe(r.MaxHops)),
		CreatedBy:        optStr(r.CreatedBy),
		CreatedAt:        optTime(r.CreatedAt),
		UpdatedAt:        optTime(r.UpdatedAt),
	}
}

func semanticPreAggregationToAPI(p domain.SemanticPreAggregation) SemanticPreAggregation {
	return SemanticPreAggregation{
		Id:              optStr(p.ID),
		SemanticModelId: optStr(p.SemanticModelID),
		Name:            optStr(p.Name),
		MetricSet:       &p.MetricSet,
		DimensionSet:    &p.DimensionSet,
		Grain:           optStr(p.Grain),
		TargetRelation:  optStr(p.TargetRelation),
		RefreshPolicy:   optStr(p.RefreshPolicy),
		CreatedBy:       optStr(p.CreatedBy),
		CreatedAt:       optTime(p.CreatedAt),
		UpdatedAt:       optTime(p.UpdatedAt),
	}
}

func metricQueryPlanToAPI(plan semantic.MetricQueryPlan) MetricQueryPlan {
	joinPath := make([]MetricQueryJoinStep, 0, len(plan.JoinPath))
	for _, step := range plan.JoinPath {
		joinPath = append(joinPath, MetricQueryJoinStep{
			RelationshipName: optStr(step.RelationshipName),
			FromModel:        optStr(step.FromModel),
			ToModel:          optStr(step.ToModel),
			JoinSql:          optStr(step.JoinSQL),
		})
	}

	return MetricQueryPlan{
		BaseModelName:          optStr(plan.BaseModelName),
		BaseRelation:           optStr(plan.BaseRelation),
		Metrics:                &plan.Metrics,
		Dimensions:             &plan.Dimensions,
		JoinPath:               &joinPath,
		SelectedPreAggregation: plan.SelectedPreAggregation,
		GeneratedSql:           optStr(plan.GeneratedSQL),
		FreshnessStatus:        optStr(plan.FreshnessStatus),
		FreshnessBasis:         &plan.FreshnessBasis,
	}
}

func semanticReqToService(req *MetricQueryRequest) semantic.MetricQueryRequest {
	if req == nil {
		return semantic.MetricQueryRequest{}
	}
	out := semantic.MetricQueryRequest{
		ProjectName:       req.ProjectName,
		SemanticModelName: req.SemanticModelName,
		Metrics:           req.Metrics,
	}
	if req.Dimensions != nil {
		out.Dimensions = *req.Dimensions
	}
	if req.Filters != nil {
		out.Filters = *req.Filters
	}
	if req.OrderBy != nil {
		out.OrderBy = *req.OrderBy
	}
	if req.Limit != nil {
		v := int(*req.Limit)
		out.Limit = &v
	}
	return out
}

func valOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func sliceOrEmpty(v *[]string) []string {
	if v == nil {
		return nil
	}
	return *v
}

func boolOrFalse(v *bool) bool {
	if v == nil {
		return false
	}
	return *v
}

func intOrZero(v *int32) int {
	if v == nil {
		return 0
	}
	return int(*v)
}

func optTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	t = t.UTC()
	return &t
}

func ptrI32(v int32) *int32 {
	return &v
}

func intToI32Safe(v int) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v)
}

func ptrInt64(v int64) *int64 {
	return &v
}

func optCertificationState(v string) *SemanticMetricCertificationState {
	if v == "" {
		return nil
	}
	s := SemanticMetricCertificationState(v)
	return &s
}

func metricTypePtr(v string) *SemanticMetricMetricType {
	if v == "" {
		return nil
	}
	t := SemanticMetricMetricType(v)
	return &t
}

func expressionModePtr(v string) *SemanticMetricExpressionMode {
	if v == "" {
		return nil
	}
	t := SemanticMetricExpressionMode(v)
	return &t
}

func relationshipTypePtr(v string) *SemanticRelationshipRelationshipType {
	if v == "" {
		return nil
	}
	t := SemanticRelationshipRelationshipType(v)
	return &t
}

func certificationOrDefault(v *CreateSemanticMetricRequestCertificationState) string {
	if v == nil {
		return domain.CertificationDraft
	}
	return string(*v)
}
