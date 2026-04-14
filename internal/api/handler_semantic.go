package api

import (
	"context"
	"math"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/semantic"
)

// semanticService defines semantic layer operations used by the API handler.
type semanticService interface {
	CreateSemanticModel(ctx context.Context, principal string, req domain.CreateSemanticModelRequest) (*domain.SemanticModel, error)
	GetSemanticModel(ctx context.Context, workspaceID, semanticModelID string) (*domain.SemanticModel, error)
	GetSemanticModelByName(ctx context.Context, workspaceID, name string) (*domain.SemanticModel, error)
	ListSemanticModels(ctx context.Context, workspaceID string, page domain.PageRequest) ([]domain.SemanticModel, int64, error)
	ListAllSemanticModels(ctx context.Context) ([]domain.SemanticModel, error)
	UpdateSemanticModel(ctx context.Context, workspaceID, semanticModelID string, req domain.UpdateSemanticModelRequest) (*domain.SemanticModel, error)
	DeleteSemanticModel(ctx context.Context, workspaceID, semanticModelID string) error

	CreateMetric(ctx context.Context, principal, workspaceID, semanticModelID string, req domain.CreateSemanticMetricRequest) (*domain.SemanticMetric, error)
	ListMetrics(ctx context.Context, workspaceID, semanticModelID string) ([]domain.SemanticMetric, error)
	GetMetric(ctx context.Context, workspaceID, semanticModelID, metricName string) (*domain.SemanticMetric, error)
	UpdateMetric(ctx context.Context, workspaceID, semanticModelID, metricName string, req domain.UpdateSemanticMetricRequest) (*domain.SemanticMetric, error)
	DeleteMetric(ctx context.Context, workspaceID, semanticModelID, metricName string) error

	CreatePreAggregation(ctx context.Context, principal, workspaceID, semanticModelID string, req domain.CreateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error)
	ListPreAggregations(ctx context.Context, workspaceID, semanticModelID string) ([]domain.SemanticPreAggregation, error)
	GetPreAggregation(ctx context.Context, workspaceID, semanticModelID, preAggName string) (*domain.SemanticPreAggregation, error)
	UpdatePreAggregation(ctx context.Context, workspaceID, semanticModelID, preAggName string, req domain.UpdateSemanticPreAggregationRequest) (*domain.SemanticPreAggregation, error)
	DeletePreAggregation(ctx context.Context, workspaceID, semanticModelID, preAggName string) error

	CreateRelationshipForModel(ctx context.Context, principal, workspaceID, semanticModelID string, req domain.CreateSemanticRelationshipRequest) (*domain.SemanticRelationship, error)
	ListRelationshipsForModel(ctx context.Context, workspaceID, semanticModelID string) ([]domain.SemanticRelationship, error)
	GetRelationshipForModel(ctx context.Context, workspaceID, semanticModelID, relationshipName string) (*domain.SemanticRelationship, error)
	UpdateRelationshipForModel(ctx context.Context, workspaceID, semanticModelID, relationshipName string, req domain.UpdateSemanticRelationshipRequest) (*domain.SemanticRelationship, error)
	DeleteRelationshipForModel(ctx context.Context, workspaceID, semanticModelID, relationshipName string) error

	ExplainMetricQuery(ctx context.Context, req semantic.MetricQueryRequest) (*semantic.MetricQueryPlan, error)
	RunMetricQuery(ctx context.Context, principal string, req semantic.MetricQueryRequest) (*semantic.MetricQueryResult, error)
}

// ListSemanticModels lists semantic models.
func (h *APIHandler) ListSemanticModels(ctx context.Context, req GenListSemanticModelsRequest) (GenListSemanticModelsResponse, error) {
	if isNilService(h.semantics) {
		empty := []SemanticModel{}
		return GenListSemanticModels200JSONResponse{Body: semanticModelsPageToGen(empty, nil), Headers: GenListSemanticModels200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	models, total, err := h.semantics.ListSemanticModels(ctx, req.WorkspaceId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListSemanticModelsResponse]("listSemanticModels", err, domainErrorResponder[GenListSemanticModelsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListSemanticModelsResponse {
				return ListSemanticModels403JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListSemanticModelsResponse {
				return GenListSemanticModels500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]SemanticModel, len(models))
	for i, m := range models {
		data[i] = semanticModelToAPI(m)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListSemanticModels200JSONResponse{Body: semanticModelsPageToGen(data, optStr(nextToken)), Headers: GenListSemanticModels200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// CreateSemanticModel creates a semantic model.
func (h *APIHandler) CreateSemanticModel(ctx context.Context, req GenCreateSemanticModelRequest) (GenCreateSemanticModelResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.CreateSemanticModel(ctx, cp.Name, domain.CreateSemanticModelRequest{
		WorkspaceID:          req.WorkspaceId,
		Name:                 req.Body.Name,
		Description:          valOrEmpty(req.Body.Description),
		BaseRelationRef:      req.Body.BaseRelationRef,
		DefaultTimeDimension: valOrEmpty(req.Body.DefaultTimeDimension),
		Tags:                 sliceOrEmpty(req.Body.Tags),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateSemanticModelResponse]("createSemanticModel", err, domainErrorResponder[GenCreateSemanticModelResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateSemanticModelResponse {
				return CreateSemanticModel400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateSemanticModelResponse {
				return CreateSemanticModel403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateSemanticModelResponse {
				return CreateSemanticModel409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenCreateSemanticModel201JSONResponse{Body: semanticModelToAPI(*result), Headers: GenCreateSemanticModel201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// GetSemanticModel retrieves a semantic model.
func (h *APIHandler) GetSemanticModel(ctx context.Context, req GenGetSemanticModelRequest) (GenGetSemanticModelResponse, error) {
	result, err := h.semantics.GetSemanticModel(ctx, req.WorkspaceId, req.SemanticModelId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetSemanticModelResponse]("getSemanticModel", err, domainErrorResponder[GenGetSemanticModelResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetSemanticModelResponse {
				return GenGetSemanticModel404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetSemanticModel200JSONResponse{Body: semanticModelToAPI(*result), Headers: GenGetSemanticModel200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// UpdateSemanticModel updates a semantic model.
func (h *APIHandler) UpdateSemanticModel(ctx context.Context, req GenUpdateSemanticModelRequest) (GenUpdateSemanticModelResponse, error) {
	domReq := domain.UpdateSemanticModelRequest{
		Description:          req.Body.Description,
		Owner:                req.Body.Owner,
		BaseRelationRef:      req.Body.BaseRelationRef,
		DefaultTimeDimension: req.Body.DefaultTimeDimension,
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}

	result, err := h.semantics.UpdateSemanticModel(ctx, req.WorkspaceId, req.SemanticModelId, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateSemanticModelResponse]("updateSemanticModel", err, domainErrorResponder[GenUpdateSemanticModelResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateSemanticModelResponse {
				return UpdateSemanticModel400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateSemanticModelResponse {
				return UpdateSemanticModel403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateSemanticModelResponse {
				return UpdateSemanticModel404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenUpdateSemanticModel200JSONResponse{Body: semanticModelToAPI(*result), Headers: GenUpdateSemanticModel200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// DeleteSemanticModel deletes a semantic model.
func (h *APIHandler) DeleteSemanticModel(ctx context.Context, req GenDeleteSemanticModelRequest) (GenDeleteSemanticModelResponse, error) {
	if err := h.semantics.DeleteSemanticModel(ctx, req.WorkspaceId, req.SemanticModelId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteSemanticModelResponse]("deleteSemanticModel", err, domainErrorResponder[GenDeleteSemanticModelResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteSemanticModelResponse {
				return DeleteSemanticModel403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteSemanticModelResponse {
				return DeleteSemanticModel404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenDeleteSemanticModel204Response{}, nil
}

// ListSemanticMetrics lists metrics under a semantic model.
func (h *APIHandler) ListSemanticMetrics(ctx context.Context, req GenListSemanticMetricsRequest) (GenListSemanticMetricsResponse, error) {
	if isNilService(h.semantics) {
		empty := []SemanticMetric{}
		return GenListSemanticMetrics200JSONResponse{Body: semanticMetricListToGen(empty), Headers: GenListSemanticMetrics200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
	}

	items, err := h.semantics.ListMetrics(ctx, req.WorkspaceId, req.SemanticModelId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListSemanticMetricsResponse]("listSemanticMetrics", err, domainErrorResponder[GenListSemanticMetricsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListSemanticMetricsResponse {
				return GenListSemanticMetrics404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]SemanticMetric, len(items))
	for i, item := range items {
		data[i] = semanticMetricToAPI(item)
	}
	return GenListSemanticMetrics200JSONResponse{Body: semanticMetricListToGen(data), Headers: GenListSemanticMetrics200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// GetSemanticMetric gets a metric under a semantic model.
func (h *APIHandler) GetSemanticMetric(ctx context.Context, req GenGetSemanticMetricRequest) (GenGetSemanticMetricResponse, error) {
	item, err := h.semantics.GetMetric(ctx, req.WorkspaceId, req.SemanticModelId, req.MetricName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetSemanticMetricResponse]("getSemanticMetric", err, domainErrorResponder[GenGetSemanticMetricResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetSemanticMetricResponse {
				return GenGetSemanticMetric404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetSemanticMetric200JSONResponse{Body: semanticMetricToAPI(*item), Headers: GenGetSemanticMetric200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// CreateSemanticMetric creates a metric under a semantic model.
func (h *APIHandler) CreateSemanticMetric(ctx context.Context, req GenCreateSemanticMetricRequest) (GenCreateSemanticMetricResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	domReq := domain.CreateSemanticMetricRequest{
		SemanticModelID:    "",
		Name:               req.Body.Name,
		Description:        valOrEmpty(req.Body.Description),
		Label:              valOrEmpty(req.Body.Label),
		MetricType:         string(req.Body.MetricType),
		Expression:         req.Body.Expression,
		RelationshipNames:  sliceOrEmpty(req.Body.RelationshipNames),
		FilterSQL:          valOrEmpty(req.Body.FilterSql),
		DefaultTimeGrain:   valOrEmpty(req.Body.DefaultTimeGrain),
		Format:             valOrEmpty(req.Body.Format),
		CertificationState: certificationOrDefault(req.Body.CertificationState),
	}
	if req.Body.ExpressionMode != nil {
		domReq.ExpressionMode = string(*req.Body.ExpressionMode)
	}

	result, err := h.semantics.CreateMetric(ctx, cp.Name, req.WorkspaceId, req.SemanticModelId, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateSemanticMetricResponse]("createSemanticMetric", err, domainErrorResponder[GenCreateSemanticMetricResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateSemanticMetricResponse {
				return CreateSemanticMetric400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateSemanticMetricResponse {
				return CreateSemanticMetric403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateSemanticMetricResponse {
				return CreateSemanticMetric404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateSemanticMetricResponse {
				return CreateSemanticMetric409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenCreateSemanticMetric201JSONResponse{Body: semanticMetricToAPI(*result), Headers: GenCreateSemanticMetric201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// UpdateSemanticMetric updates a metric under a semantic model.
func (h *APIHandler) UpdateSemanticMetric(ctx context.Context, req GenUpdateSemanticMetricRequest) (GenUpdateSemanticMetricResponse, error) {
	domReq := domain.UpdateSemanticMetricRequest{
		Description: req.Body.Description,
		Label:       req.Body.Label,
		Expression:  req.Body.Expression,
		FilterSQL:   req.Body.FilterSql,
		Owner:       req.Body.Owner,
	}
	if req.Body.RelationshipNames != nil {
		domReq.RelationshipNames = *req.Body.RelationshipNames
	}
	domReq.DefaultTimeGrain = req.Body.DefaultTimeGrain
	domReq.Format = req.Body.Format
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

	result, err := h.semantics.UpdateMetric(ctx, req.WorkspaceId, req.SemanticModelId, req.MetricName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateSemanticMetricResponse]("updateSemanticMetric", err, domainErrorResponder[GenUpdateSemanticMetricResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateSemanticMetricResponse {
				return UpdateSemanticMetric400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateSemanticMetricResponse {
				return UpdateSemanticMetric403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateSemanticMetricResponse {
				return UpdateSemanticMetric404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenUpdateSemanticMetric200JSONResponse{Body: semanticMetricToAPI(*result), Headers: GenUpdateSemanticMetric200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// DeleteSemanticMetric deletes a metric under a semantic model.
func (h *APIHandler) DeleteSemanticMetric(ctx context.Context, req GenDeleteSemanticMetricRequest) (GenDeleteSemanticMetricResponse, error) {
	if err := h.semantics.DeleteMetric(ctx, req.WorkspaceId, req.SemanticModelId, req.MetricName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteSemanticMetricResponse]("deleteSemanticMetric", err, domainErrorResponder[GenDeleteSemanticMetricResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteSemanticMetricResponse {
				return DeleteSemanticMetric403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteSemanticMetricResponse {
				return DeleteSemanticMetric404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteSemanticMetric204Response{}, nil
}

// ListSemanticPreAggregations lists pre-aggregations under a semantic model.
func (h *APIHandler) ListSemanticPreAggregations(ctx context.Context, req GenListSemanticPreAggregationsRequest) (GenListSemanticPreAggregationsResponse, error) {
	if isNilService(h.semantics) {
		empty := []SemanticPreAggregation{}
		return GenListSemanticPreAggregations200JSONResponse{Body: semanticPreAggregationListToGen(empty), Headers: GenListSemanticPreAggregations200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
	}

	items, err := h.semantics.ListPreAggregations(ctx, req.WorkspaceId, req.SemanticModelId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListSemanticPreAggregationsResponse]("listSemanticPreAggregations", err, domainErrorResponder[GenListSemanticPreAggregationsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListSemanticPreAggregationsResponse {
				return GenListSemanticPreAggregations404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]SemanticPreAggregation, len(items))
	for i, item := range items {
		data[i] = semanticPreAggregationToAPI(item)
	}
	return GenListSemanticPreAggregations200JSONResponse{Body: semanticPreAggregationListToGen(data), Headers: GenListSemanticPreAggregations200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// GetSemanticPreAggregation gets a pre-aggregation under a semantic model.
func (h *APIHandler) GetSemanticPreAggregation(ctx context.Context, req GenGetSemanticPreAggregationRequest) (GenGetSemanticPreAggregationResponse, error) {
	item, err := h.semantics.GetPreAggregation(ctx, req.WorkspaceId, req.SemanticModelId, req.PreAggregationName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetSemanticPreAggregationResponse]("getSemanticPreAggregation", err, domainErrorResponder[GenGetSemanticPreAggregationResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetSemanticPreAggregationResponse {
				return GenGetSemanticPreAggregation404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetSemanticPreAggregation200JSONResponse{Body: semanticPreAggregationToAPI(*item), Headers: GenGetSemanticPreAggregation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// CreateSemanticPreAggregation creates a pre-aggregation under a semantic model.
func (h *APIHandler) CreateSemanticPreAggregation(ctx context.Context, req GenCreateSemanticPreAggregationRequest) (GenCreateSemanticPreAggregationResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.CreatePreAggregation(ctx, cp.Name, req.WorkspaceId, req.SemanticModelId, domain.CreateSemanticPreAggregationRequest{
		SemanticModelID: "",
		Name:            req.Body.Name,
		MetricSet:       sliceOrEmpty(req.Body.MetricSet),
		DimensionSet:    sliceOrEmpty(req.Body.DimensionSet),
		Grain:           valOrEmpty(req.Body.Grain),
		TargetRelation:  req.Body.TargetRelation,
		RefreshPolicy:   valOrEmpty(req.Body.RefreshPolicy),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateSemanticPreAggregationResponse]("createSemanticPreAggregation", err, domainErrorResponder[GenCreateSemanticPreAggregationResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateSemanticPreAggregationResponse {
				return CreateSemanticPreAggregation400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateSemanticPreAggregationResponse {
				return CreateSemanticPreAggregation403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateSemanticPreAggregationResponse {
				return CreateSemanticPreAggregation404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateSemanticPreAggregationResponse {
				return CreateSemanticPreAggregation409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateSemanticPreAggregation201JSONResponse{Body: semanticPreAggregationToAPI(*result), Headers: GenCreateSemanticPreAggregation201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// UpdateSemanticPreAggregation updates a pre-aggregation under a semantic model.
func (h *APIHandler) UpdateSemanticPreAggregation(ctx context.Context, req GenUpdateSemanticPreAggregationRequest) (GenUpdateSemanticPreAggregationResponse, error) {
	domReq := domain.UpdateSemanticPreAggregationRequest{Grain: req.Body.Grain, TargetRelation: req.Body.TargetRelation, RefreshPolicy: req.Body.RefreshPolicy}
	if req.Body.MetricSet != nil {
		domReq.MetricSet = *req.Body.MetricSet
	}
	if req.Body.DimensionSet != nil {
		domReq.DimensionSet = *req.Body.DimensionSet
	}
	result, err := h.semantics.UpdatePreAggregation(ctx, req.WorkspaceId, req.SemanticModelId, req.PreAggregationName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateSemanticPreAggregationResponse]("updateSemanticPreAggregation", err, domainErrorResponder[GenUpdateSemanticPreAggregationResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateSemanticPreAggregationResponse {
				return UpdateSemanticPreAggregation400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateSemanticPreAggregationResponse {
				return UpdateSemanticPreAggregation403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateSemanticPreAggregationResponse {
				return UpdateSemanticPreAggregation404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateSemanticPreAggregation200JSONResponse{Body: semanticPreAggregationToAPI(*result), Headers: GenUpdateSemanticPreAggregation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// DeleteSemanticPreAggregation deletes a pre-aggregation under a semantic model.
func (h *APIHandler) DeleteSemanticPreAggregation(ctx context.Context, req GenDeleteSemanticPreAggregationRequest) (GenDeleteSemanticPreAggregationResponse, error) {
	if err := h.semantics.DeletePreAggregation(ctx, req.WorkspaceId, req.SemanticModelId, req.PreAggregationName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteSemanticPreAggregationResponse]("deleteSemanticPreAggregation", err, domainErrorResponder[GenDeleteSemanticPreAggregationResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteSemanticPreAggregationResponse {
				return DeleteSemanticPreAggregation403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteSemanticPreAggregationResponse {
				return DeleteSemanticPreAggregation404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteSemanticPreAggregation204Response{}, nil
}

// ListSemanticModelRelationships lists semantic relationships for a semantic model.
func (h *APIHandler) ListSemanticModelRelationships(ctx context.Context, req GenListSemanticModelRelationshipsRequest) (GenListSemanticModelRelationshipsResponse, error) {
	if isNilService(h.semantics) {
		empty := []SemanticRelationship{}
		return GenListSemanticModelRelationships200JSONResponse{Body: semanticRelationshipListToGen(empty), Headers: GenListSemanticModelRelationships200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
	}

	rels, err := h.semantics.ListRelationshipsForModel(ctx, req.WorkspaceId, req.SemanticModelId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListSemanticModelRelationshipsResponse]("listSemanticModelRelationships", err, domainErrorResponder[GenListSemanticModelRelationshipsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListSemanticModelRelationshipsResponse {
				return ListSemanticModelRelationships403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListSemanticModelRelationshipsResponse {
				return GenListSemanticModelRelationships404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListSemanticModelRelationshipsResponse {
				return GenListSemanticModelRelationships500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]SemanticRelationship, len(rels))
	for i, rel := range rels {
		data[i] = semanticRelationshipToAPI(rel)
	}
	return GenListSemanticModelRelationships200JSONResponse{Body: semanticRelationshipListToGen(data), Headers: GenListSemanticModelRelationships200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// CreateSemanticModelRelationship creates a semantic relationship for a semantic model.
func (h *APIHandler) CreateSemanticModelRelationship(ctx context.Context, req GenCreateSemanticModelRelationshipRequest) (GenCreateSemanticModelRelationshipResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.CreateRelationshipForModel(ctx, cp.Name, req.WorkspaceId, req.SemanticModelId, domain.CreateSemanticRelationshipRequest{
		Name:             req.Body.Name,
		FromSemanticID:   req.Body.FromSemanticId,
		ToSemanticID:     req.Body.ToSemanticId,
		RelationshipType: string(req.Body.RelationshipType),
		JoinSQL:          req.Body.JoinSql,
		Cost:             intOrZero(req.Body.Cost),
		MaxHops:          intOrZero(req.Body.MaxHops),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateSemanticModelRelationshipResponse]("createSemanticModelRelationship", err, domainErrorResponder[GenCreateSemanticModelRelationshipResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateSemanticModelRelationshipResponse {
				return CreateSemanticModelRelationship400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateSemanticModelRelationshipResponse {
				return CreateSemanticModelRelationship403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateSemanticModelRelationshipResponse {
				return CreateSemanticModelRelationship409JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateSemanticModelRelationshipResponse {
				return CreateSemanticModelRelationship404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateSemanticModelRelationship201JSONResponse{Body: semanticRelationshipToAPI(*result), Headers: GenCreateSemanticModelRelationship201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// GetSemanticModelRelationship gets a relationship for a semantic model.
func (h *APIHandler) GetSemanticModelRelationship(ctx context.Context, req GenGetSemanticModelRelationshipRequest) (GenGetSemanticModelRelationshipResponse, error) {
	item, err := h.semantics.GetRelationshipForModel(ctx, req.WorkspaceId, req.SemanticModelId, req.RelationshipName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetSemanticModelRelationshipResponse]("getSemanticModelRelationship", err, domainErrorResponder[GenGetSemanticModelRelationshipResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetSemanticModelRelationshipResponse {
				return GenGetSemanticModelRelationship404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetSemanticModelRelationship200JSONResponse{Body: semanticRelationshipToAPI(*item), Headers: GenGetSemanticModelRelationship200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// UpdateSemanticModelRelationship updates a semantic relationship for a semantic model.
func (h *APIHandler) UpdateSemanticModelRelationship(ctx context.Context, req GenUpdateSemanticModelRelationshipRequest) (GenUpdateSemanticModelRelationshipResponse, error) {
	domReq := domain.UpdateSemanticRelationshipRequest{}
	if req.Body != nil {
		domReq.JoinSQL = req.Body.JoinSql
	}
	if req.Body != nil && req.Body.RelationshipType != nil {
		s := string(*req.Body.RelationshipType)
		domReq.RelationshipType = &s
	}
	if req.Body != nil && req.Body.Cost != nil {
		v := int(*req.Body.Cost)
		domReq.Cost = &v
	}
	if req.Body != nil && req.Body.MaxHops != nil {
		v := int(*req.Body.MaxHops)
		domReq.MaxHops = &v
	}

	result, err := h.semantics.UpdateRelationshipForModel(ctx, req.WorkspaceId, req.SemanticModelId, req.RelationshipName, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateSemanticModelRelationshipResponse]("updateSemanticModelRelationship", err, domainErrorResponder[GenUpdateSemanticModelRelationshipResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateSemanticModelRelationshipResponse {
				return UpdateSemanticModelRelationship400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateSemanticModelRelationshipResponse {
				return UpdateSemanticModelRelationship403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateSemanticModelRelationshipResponse {
				return UpdateSemanticModelRelationship404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateSemanticModelRelationship200JSONResponse{Body: semanticRelationshipToAPI(*result), Headers: GenUpdateSemanticModelRelationship200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// DeleteSemanticModelRelationship deletes a semantic relationship for a semantic model.
func (h *APIHandler) DeleteSemanticModelRelationship(ctx context.Context, req GenDeleteSemanticModelRelationshipRequest) (GenDeleteSemanticModelRelationshipResponse, error) {
	if err := h.semantics.DeleteRelationshipForModel(ctx, req.WorkspaceId, req.SemanticModelId, req.RelationshipName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteSemanticModelRelationshipResponse]("deleteSemanticModelRelationship", err, domainErrorResponder[GenDeleteSemanticModelRelationshipResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteSemanticModelRelationshipResponse {
				return DeleteSemanticModelRelationship403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteSemanticModelRelationshipResponse {
				return DeleteSemanticModelRelationship404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteSemanticModelRelationship204Response{}, nil
}

// CheckMetricFreshness resolves a metric and returns its current freshness metadata.
func (h *APIHandler) CheckMetricFreshness(ctx context.Context, req GenCheckMetricFreshnessRequest) (GenCheckMetricFreshnessResponse, error) {
	if isNilService(h.semantics) {
		return GenCheckMetricFreshness404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: "semantic service is not configured"}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	models, err := h.semantics.ListAllSemanticModels(ctx)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCheckMetricFreshnessResponse]("checkMetricFreshness", err, domainErrorResponder[GenCheckMetricFreshnessResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCheckMetricFreshnessResponse {
				return CheckMetricFreshness400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCheckMetricFreshnessResponse {
				return GenCheckMetricFreshness404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenCheckMetricFreshnessResponse {
				return GenCheckMetricFreshness500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	type metricMatch struct {
		semanticModelID   string
		semanticModelName string
	}

	matches := make([]metricMatch, 0, 1)
	for _, model := range models {
		if req.Params.SemanticModelId != nil && model.ID != *req.Params.SemanticModelId {
			continue
		}
		metrics, listErr := h.semantics.ListMetrics(ctx, model.WorkspaceID, model.ID)
		if listErr != nil {
			if resp, ok := respondDomainErrorForOperation[GenCheckMetricFreshnessResponse]("checkMetricFreshness", listErr, domainErrorResponder[GenCheckMetricFreshnessResponse]{
				BadRequest: func(resp BadRequestJSONResponse) GenCheckMetricFreshnessResponse {
					return CheckMetricFreshness400JSONResponse{resp}
				},
				NotFound: func(resp NotFoundJSONResponse) GenCheckMetricFreshnessResponse {
					return GenCheckMetricFreshness404JSONResponse{resp}
				},
				Internal: func(resp InternalErrorJSONResponse) GenCheckMetricFreshnessResponse {
					return GenCheckMetricFreshness500JSONResponse{resp}
				},
			}); ok {
				return resp, nil
			}
			return nil, listErr
		}
		for _, metric := range metrics {
			if metric.Name == req.MetricName {
				matches = append(matches, metricMatch{semanticModelID: model.ID, semanticModelName: model.Name})
			}
		}
	}

	if len(matches) == 0 {
		err = domain.ErrNotFound("metric %q not found", req.MetricName)
		return GenCheckMetricFreshness404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	if len(matches) > 1 {
		err = domain.ErrValidation("metric %q is ambiguous; provide semantic_model_id", req.MetricName)
		return CheckMetricFreshness400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	match := matches[0]
	plan, err := h.semantics.ExplainMetricQuery(ctx, semantic.MetricQueryRequest{
		SemanticModelID: match.semanticModelID,
		Metrics:         []string{req.MetricName},
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCheckMetricFreshnessResponse]("checkMetricFreshness", err, domainErrorResponder[GenCheckMetricFreshnessResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCheckMetricFreshnessResponse {
				return CheckMetricFreshness400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCheckMetricFreshnessResponse {
				return GenCheckMetricFreshness404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	basis := append([]string(nil), plan.FreshnessBasis...)
	checkedAt := time.Now().UTC().Format(time.RFC3339)
	return GenCheckMetricFreshness200JSONResponse{Body: MetricFreshnessStatus{
		MetricName:             &req.MetricName,
		SemanticModelId:        &match.semanticModelID,
		SemanticModelName:      &match.semanticModelName,
		FreshnessStatus:        optStr(plan.FreshnessStatus),
		FreshnessBasis:         &basis,
		SelectedPreAggregation: plan.SelectedPreAggregation,
		CheckedAt:              &checkedAt,
	}, Headers: GenCheckMetricFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// ExplainMetricQuery compiles a semantic metric query without executing it.
func (h *APIHandler) ExplainMetricQuery(ctx context.Context, req GenExplainMetricQueryRequest) (GenExplainMetricQueryResponse, error) {
	plan, err := h.semantics.ExplainMetricQuery(ctx, semanticReqToService(req.WorkspaceId, req.SemanticModelId, req.Body))
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenExplainMetricQueryResponse]("explainMetricQuery", err, domainErrorResponder[GenExplainMetricQueryResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenExplainMetricQueryResponse {
				return ExplainMetricQuery400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenExplainMetricQueryResponse {
				return ExplainMetricQuery404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	apiPlan := metricQueryPlanToAPI(*plan)
	return ExplainMetricQuery200JSONResponse{Body: MetricQueryExplainResponse{Plan: &apiPlan}, Headers: ExplainMetricQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// RunMetricQuery compiles and executes a semantic metric query.
func (h *APIHandler) RunMetricQuery(ctx context.Context, req GenRunMetricQueryRequest) (GenRunMetricQueryResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.semantics.RunMetricQuery(ctx, cp.Name, semanticReqToService(req.WorkspaceId, req.SemanticModelId, req.Body))
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenRunMetricQueryResponse]("runMetricQuery", err, domainErrorResponder[GenRunMetricQueryResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenRunMetricQueryResponse {
				return RunMetricQuery400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenRunMetricQueryResponse { return RunMetricQuery403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenRunMetricQueryResponse { return RunMetricQuery404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	apiPlan := metricQueryPlanToAPI(result.Plan)
	rowCount := safeIntToInt32(result.Result.RowCount)
	apiResult := QueryResult{
		Columns:  tabularColumns(result.Result.Columns, result.Result.Rows),
		Rows:     rowsToAnyMaps(result.Result.Columns, result.Result.Rows),
		RowCount: &rowCount,
	}
	return RunMetricQuery200JSONResponse{Body: MetricQueryRunResponse{Plan: &apiPlan, Result: &apiResult}, Headers: RunMetricQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

func semanticModelToAPI(m domain.SemanticModel) SemanticModel {
	return SemanticModel{
		Id:                   optStr(m.ID),
		WorkspaceId:          optStr(m.WorkspaceID),
		Name:                 optStr(m.Name),
		Description:          optStr(m.Description),
		Owner:                optStr(m.Owner),
		BaseRelationRef:      optStr(m.BaseRelationRef),
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
		Label:              optStr(m.Label),
		MetricType:         metricTypePtr(m.MetricType),
		ExpressionMode:     expressionModePtr(m.ExpressionMode),
		Expression:         optStr(m.Expression),
		RelationshipNames:  &m.RelationshipNames,
		FilterSql:          optStr(m.FilterSQL),
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

func semanticModelsPageToGen(items []SemanticModel, nextPageToken *string) PaginatedSemanticModels {
	data := make([]SemanticModel, len(items))
	copy(data, items)
	return PaginatedSemanticModels{Data: data, NextPageToken: nextPageToken}
}

func semanticMetricListToGen(items []SemanticMetric) SemanticMetricList {
	data := make([]SemanticMetric, len(items))
	copy(data, items)
	return SemanticMetricList{Data: data}
}

func semanticPreAggregationListToGen(items []SemanticPreAggregation) SemanticPreAggregationList {
	data := make([]SemanticPreAggregation, len(items))
	copy(data, items)
	return SemanticPreAggregationList{Data: data}
}

func semanticRelationshipListToGen(items []SemanticRelationship) SemanticRelationshipList {
	data := make([]SemanticRelationship, len(items))
	copy(data, items)
	return SemanticRelationshipList{Data: data}
}

func metricQueryPlanToAPI(plan semantic.MetricQueryPlan) MetricQueryPlan {
	joinPath := make([]MetricQueryJoinStep, 0, len(plan.JoinPath))
	for _, step := range plan.JoinPath {
		joinPath = append(joinPath, MetricQueryJoinStep{
			RelationshipName: optStr(step.RelationshipName),
			FromModel:        optStr(step.FromModel),
			ToModel:          optStr(step.ToModel),
			RelationshipType: optStr(step.RelationshipType),
			JoinSql:          optStr(step.JoinSQL),
		})
	}

	return MetricQueryPlan{
		BaseModelName:          optStr(plan.BaseModelName),
		BaseRelation:           optStr(plan.BaseRelation),
		Metrics:                &plan.Metrics,
		Dimensions:             &plan.Dimensions,
		TimeGrain:              plan.TimeGrain,
		JoinPath:               &joinPath,
		SelectedPreAggregation: plan.SelectedPreAggregation,
		GeneratedSql:           optStr(plan.GeneratedSQL),
		FreshnessStatus:        optStr(plan.FreshnessStatus),
		FreshnessBasis:         &plan.FreshnessBasis,
	}
}

func semanticReqToService(workspaceID, semanticModelID string, req *GenSchemaMetricQueryRequest) semantic.MetricQueryRequest {
	if req == nil {
		return semantic.MetricQueryRequest{WorkspaceID: workspaceID, SemanticModelID: semanticModelID}
	}
	out := semantic.MetricQueryRequest{
		WorkspaceID:    workspaceID,
		SemanticModelID: semanticModelID,
		Metrics:        req.Metrics,
	}
	if req.RelationshipNames != nil {
		out.RelationshipNames = *req.RelationshipNames
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
	out.TimeGrain = req.TimeGrain
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

func intOrZero(v *int32) int {
	if v == nil {
		return 0
	}
	return int(*v)
}

func optTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	return formatTimePtr(&t)
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

func optCertificationState(v string) *CreateSemanticMetricRequestCertificationState {
	if v == "" {
		return nil
	}
	s := CreateSemanticMetricRequestCertificationState(v)
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
