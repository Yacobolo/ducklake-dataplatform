package api

import (
	"context"
	"errors"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/model"
)

// modelService defines the model operations used by the API handler.
type modelService interface {
	CreateModel(ctx context.Context, principal string, req domain.CreateModelRequest) (*domain.Model, error)
	GetModel(ctx context.Context, projectName, name string) (*domain.Model, error)
	ListModels(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.Model, int64, error)
	UpdateModel(ctx context.Context, principal, projectName, name string, req domain.UpdateModelRequest) (*domain.Model, error)
	DeleteModel(ctx context.Context, principal, projectName, name string) error
	GetDAG(ctx context.Context, projectName *string) ([][]model.DAGNode, error)
	TriggerRun(ctx context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error)
	GetRun(ctx context.Context, runID string) (*domain.ModelRun, error)
	ListRuns(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error)
	ListRunSteps(ctx context.Context, runID string) ([]domain.ModelRunStep, error)
	CancelRun(ctx context.Context, principal, runID string) error
	CreateTest(ctx context.Context, principal, projectName, modelName string, req domain.CreateModelTestRequest) (*domain.ModelTest, error)
	ListTests(ctx context.Context, projectName, modelName string) ([]domain.ModelTest, error)
	DeleteTest(ctx context.Context, principal, projectName, modelName, testID string) error
	ListTestResults(ctx context.Context, runID, stepID string) ([]domain.ModelTestResult, error)
	CheckFreshness(ctx context.Context, projectName, modelName string) (*domain.FreshnessStatus, error)
	PromoteNotebook(ctx context.Context, principal string, req domain.PromoteNotebookRequest) (*domain.Model, error)
}

// === Models ===

// ListModels implements the endpoint for listing transformation models.
func (h *APIHandler) ListModels(ctx context.Context, req ListModelsRequestObject) (ListModelsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	models, total, err := h.models.ListModels(ctx, req.Params.ProjectName, page)
	if err != nil {
		return nil, err
	}

	data := make([]Model, len(models))
	for i, m := range models {
		data[i] = modelToAPI(m)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListModels200JSONResponse{
		Body:    PaginatedModels{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListModels200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateModel implements the endpoint for creating a new transformation model.
func (h *APIHandler) CreateModel(ctx context.Context, req CreateModelRequestObject) (CreateModelResponseObject, error) {
	domReq := domain.CreateModelRequest{
		ProjectName: req.Body.ProjectName,
		Name:        req.Body.Name,
		SQL:         req.Body.Sql,
	}
	if req.Body.Materialization != nil {
		domReq.Materialization = string(*req.Body.Materialization)
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}
	if req.Body.Config != nil {
		domReq.Config = domainModelConfig(*req.Body.Config)
	}
	if req.Body.Contract != nil {
		contract := domainModelContract(*req.Body.Contract)
		domReq.Contract = &contract
	}
	if req.Body.FreshnessPolicy != nil {
		freshness := domainFreshnessPolicy(*req.Body.FreshnessPolicy)
		domReq.Freshness = &freshness
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.CreateModel(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateModel409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateModel201JSONResponse{
		Body:    modelToAPI(*result),
		Headers: CreateModel201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModel implements the endpoint for retrieving a model by project and name.
func (h *APIHandler) GetModel(ctx context.Context, req GetModelRequestObject) (GetModelResponseObject, error) {
	result, err := h.models.GetModel(ctx, req.ProjectName, req.ModelName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetModel200JSONResponse{
		Body:    modelToAPI(*result),
		Headers: GetModel200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateModel implements the endpoint for updating a transformation model.
func (h *APIHandler) UpdateModel(ctx context.Context, req UpdateModelRequestObject) (UpdateModelResponseObject, error) {
	domReq := domain.UpdateModelRequest{
		SQL:         req.Body.Sql,
		Description: req.Body.Description,
	}
	if req.Body.Materialization != nil {
		s := string(*req.Body.Materialization)
		domReq.Materialization = &s
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}
	if req.Body.Config != nil {
		cfg := domainModelConfig(*req.Body.Config)
		domReq.Config = &cfg
	}
	if req.Body.Contract != nil {
		contract := domainModelContract(*req.Body.Contract)
		domReq.Contract = &contract
	}
	if req.Body.FreshnessPolicy != nil {
		freshness := domainFreshnessPolicy(*req.Body.FreshnessPolicy)
		domReq.Freshness = &freshness
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.UpdateModel(ctx, principal, req.ProjectName, req.ModelName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateModel200JSONResponse{
		Body:    modelToAPI(*result),
		Headers: UpdateModel200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteModel implements the endpoint for deleting a transformation model.
func (h *APIHandler) DeleteModel(ctx context.Context, req DeleteModelRequestObject) (DeleteModelResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.models.DeleteModel(ctx, principal, req.ProjectName, req.ModelName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteModel204Response{
		Headers: DeleteModel204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModelDAG implements the endpoint for retrieving the model dependency DAG.
func (h *APIHandler) GetModelDAG(ctx context.Context, req GetModelDAGRequestObject) (GetModelDAGResponseObject, error) {
	tiers, err := h.models.GetDAG(ctx, req.Params.ProjectName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return GetModelDAG400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetModelDAG200JSONResponse{
		Body:    dagToAPI(tiers),
		Headers: GetModelDAG200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Model Runs ===

// TriggerModelRun implements the endpoint for triggering a model run.
func (h *APIHandler) TriggerModelRun(ctx context.Context, req TriggerModelRunRequestObject) (TriggerModelRunResponseObject, error) {
	domReq := domain.TriggerModelRunRequest{
		TargetCatalog: "memory",
		TargetSchema:  req.Body.ProjectName,
		TriggerType:   domain.ModelTriggerTypeManual,
	}
	if req.Body.ModelNames != nil && len(*req.Body.ModelNames) > 0 {
		domReq.Selector = strings.Join(*req.Body.ModelNames, ",")
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.TriggerRun(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return TriggerModelRun403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return TriggerModelRun400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return TriggerModelRun409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return TriggerModelRun400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return TriggerModelRun201JSONResponse{
		Body:    modelRunToAPI(*result),
		Headers: TriggerModelRun201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelRuns implements the endpoint for listing model runs.
func (h *APIHandler) ListModelRuns(ctx context.Context, req ListModelRunsRequestObject) (ListModelRunsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.ModelRunFilter{
		Page: page,
	}
	if req.Params.Status != nil {
		s := string(*req.Params.Status)
		if !isValidListModelRunsStatus(s) {
			return ListModelRuns400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "status must be one of: PENDING, RUNNING, SUCCESS, FAILED, CANCELLED"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		filter.Status = &s
	}

	runs, total, err := h.models.ListRuns(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]ModelRun, len(runs))
	for i, r := range runs {
		data[i] = modelRunToAPI(r)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListModelRuns200JSONResponse{
		Body:    PaginatedModelRuns{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListModelRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModelRun implements the endpoint for retrieving a model run.
func (h *APIHandler) GetModelRun(ctx context.Context, req GetModelRunRequestObject) (GetModelRunResponseObject, error) {
	result, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetModelRun404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetModelRun200JSONResponse{
		Body:    modelRunToAPI(*result),
		Headers: GetModelRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelRunSteps implements the endpoint for listing model run steps.
func (h *APIHandler) ListModelRunSteps(ctx context.Context, req ListModelRunStepsRequestObject) (ListModelRunStepsResponseObject, error) {
	steps, err := h.models.ListRunSteps(ctx, req.RunId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListModelRunSteps404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]ModelRunStep, len(steps))
	for i, s := range steps {
		data[i] = modelRunStepToAPI(s)
	}
	return ListModelRunSteps200JSONResponse{
		Body:    ModelRunStepList{Data: &data},
		Headers: ListModelRunSteps200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelModelRun implements the endpoint for cancelling a model run.
func (h *APIHandler) CancelModelRun(ctx context.Context, req CancelModelRunRequestObject) (CancelModelRunResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.models.CancelRun(ctx, principal, req.RunId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CancelModelRun403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CancelModelRun400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CancelModelRun404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CancelModelRun409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	// Re-fetch the run to return updated state.
	result, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, err
	}
	return CancelModelRun200JSONResponse{
		Body:    modelRunToAPI(*result),
		Headers: CancelModelRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Model Mappers ===

func modelToAPI(m domain.Model) Model {
	ct := m.CreatedAt
	ut := m.UpdatedAt
	mat := ModelMaterialization(m.Materialization)
	resp := Model{
		Id:              &m.ID,
		ProjectName:     &m.ProjectName,
		Name:            &m.Name,
		Sql:             &m.SQL,
		Materialization: &mat,
		Description:     &m.Description,
		Owner:           &m.Owner,
		CreatedBy:       &m.CreatedBy,
		CreatedAt:       &ct,
		UpdatedAt:       &ut,
	}
	if len(m.DependsOn) > 0 {
		resp.DependsOn = &m.DependsOn
	}
	if len(m.Tags) > 0 {
		resp.Tags = &m.Tags
	}
	if m.Config.UniqueKey != nil || m.Config.IncrementalStrategy != "" {
		cfg := apiModelConfig(m.Config)
		resp.Config = &cfg
	}
	if m.Contract != nil {
		contract := apiModelContract(*m.Contract)
		resp.Contract = &contract
	}
	if m.Freshness != nil {
		freshness := apiFreshnessPolicy(*m.Freshness)
		resp.FreshnessPolicy = &freshness
	}
	return resp
}

func modelRunToAPI(r domain.ModelRun) ModelRun {
	ct := r.CreatedAt
	status := ModelRunStatus(r.Status)
	triggerType := ModelRunTriggerType(r.TriggerType)
	resp := ModelRun{
		Id:          &r.ID,
		Status:      &status,
		TriggerType: &triggerType,
		TriggeredBy: &r.TriggeredBy,
		CreatedAt:   &ct,
	}
	if r.TargetSchema != "" {
		resp.ProjectName = &r.TargetSchema
	}
	if names := selectorToModelNames(r.ModelSelector); len(names) > 0 {
		resp.ModelNames = &names
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

func isValidListModelRunsStatus(status string) bool {
	switch status {
	case domain.ModelRunStatusPending,
		domain.ModelRunStatusRunning,
		domain.ModelRunStatusSuccess,
		domain.ModelRunStatusFailed,
		domain.ModelRunStatusCancelled:
		return true
	default:
		return false
	}
}

func selectorToModelNames(selector string) []string {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil
	}
	if strings.HasPrefix(selector, "tag:") || strings.HasPrefix(selector, "project:") {
		return nil
	}
	if strings.Contains(selector, "+") || selector == "*" {
		return nil
	}

	rawParts := strings.Split(selector, ",")
	names := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil
	}
	return names
}

func modelRunStepToAPI(s domain.ModelRunStep) ModelRunStep {
	ct := s.CreatedAt
	status := ModelRunStepStatus(s.Status)
	resp := ModelRunStep{
		Id:        &s.ID,
		RunId:     &s.RunID,
		ModelName: &s.ModelName,
		Status:    &status,
		CreatedAt: &ct,
	}
	if s.RowsAffected != nil {
		resp.RowsAffected = s.RowsAffected
	}
	if s.StartedAt != nil {
		resp.StartedAt = s.StartedAt
	}
	if s.FinishedAt != nil {
		resp.FinishedAt = s.FinishedAt
	}
	if s.ErrorMessage != nil {
		resp.ErrorMessage = s.ErrorMessage
	}
	return resp
}

func dagToAPI(tiers [][]model.DAGNode) ModelDAG {
	apiTiers := make([]ModelDAGTier, len(tiers))
	for i, tier := range tiers {
		tierNum := int32(i) //nolint:gosec // tier index is small
		nodes := make([]ModelDAGNode, len(tier))
		for j, node := range tier {
			mat := ModelDAGNodeMaterialization(node.Model.Materialization)
			n := ModelDAGNode{
				ProjectName:     &node.Model.ProjectName,
				ModelName:       optStr(node.Model.Name),
				Materialization: &mat,
			}
			if len(node.Model.DependsOn) > 0 {
				n.DependsOn = &node.Model.DependsOn
			}
			nodes[j] = n
		}
		apiTiers[i] = ModelDAGTier{
			Tier:  &tierNum,
			Nodes: &nodes,
		}
	}
	return ModelDAG{Tiers: &apiTiers}
}

func apiModelConfig(c domain.ModelConfig) ModelConfig {
	cfg := ModelConfig{}
	if len(c.UniqueKey) > 0 {
		cfg.UniqueKey = &c.UniqueKey
	}
	if c.IncrementalStrategy != "" {
		cfg.IncrementalStrategy = &c.IncrementalStrategy
	}
	return cfg
}

func domainModelConfig(c ModelConfig) domain.ModelConfig {
	cfg := domain.ModelConfig{}
	if c.UniqueKey != nil {
		cfg.UniqueKey = *c.UniqueKey
	}
	if c.IncrementalStrategy != nil {
		cfg.IncrementalStrategy = *c.IncrementalStrategy
	}
	return cfg
}

// === Model Tests ===

// CreateModelTest implements the endpoint for creating a model test.
func (h *APIHandler) CreateModelTest(ctx context.Context, req CreateModelTestRequestObject) (CreateModelTestResponseObject, error) {
	domReq := domain.CreateModelTestRequest{
		Name:     req.Body.Name,
		TestType: string(req.Body.TestType),
	}
	if req.Body.Column != nil {
		domReq.Column = *req.Body.Column
	}
	if req.Body.Config != nil {
		domReq.Config = domainModelTestConfig(*req.Body.Config)
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.CreateTest(ctx, principal, req.ProjectName, req.ModelName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateModelTest403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateModelTest404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateModelTest400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateModelTest409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateModelTest400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateModelTest201JSONResponse{
		Body:    modelTestToAPI(*result),
		Headers: CreateModelTest201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelTests implements the endpoint for listing tests for a model.
func (h *APIHandler) ListModelTests(ctx context.Context, req ListModelTestsRequestObject) (ListModelTestsResponseObject, error) {
	tests, err := h.models.ListTests(ctx, req.ProjectName, req.ModelName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListModelTests404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]ModelTest, len(tests))
	for i, t := range tests {
		data[i] = modelTestToAPI(t)
	}
	return ListModelTests200JSONResponse{
		Body:    ModelTestList{Data: &data},
		Headers: ListModelTests200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteModelTest implements the endpoint for deleting a model test.
func (h *APIHandler) DeleteModelTest(ctx context.Context, req DeleteModelTestRequestObject) (DeleteModelTestResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.models.DeleteTest(ctx, principal, req.ProjectName, req.ModelName, req.TestId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteModelTest403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteModelTest404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteModelTest204Response{
		Headers: DeleteModelTest204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelTestResults implements the endpoint for listing test results for a run step.
func (h *APIHandler) ListModelTestResults(ctx context.Context, req ListModelTestResultsRequestObject) (ListModelTestResultsResponseObject, error) {
	results, err := h.models.ListTestResults(ctx, req.RunId, req.StepId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListModelTestResults404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]ModelTestResult, len(results))
	for i, r := range results {
		data[i] = modelTestResultToAPI(r)
	}
	return ListModelTestResults200JSONResponse{
		Body:    ModelTestResultList{Data: &data},
		Headers: ListModelTestResults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Model Test Mappers ===

func modelTestToAPI(t domain.ModelTest) ModelTest {
	ct := t.CreatedAt
	tt := ModelTestTestType(t.TestType)
	resp := ModelTest{
		Id:        &t.ID,
		ModelId:   &t.ModelID,
		Name:      &t.Name,
		TestType:  &tt,
		CreatedAt: &ct,
	}
	if t.Column != "" {
		resp.Column = &t.Column
	}
	if t.Config.SQL != "" || t.Config.ToModel != "" || len(t.Config.Values) > 0 {
		cfg := apiModelTestConfig(t.Config)
		resp.Config = &cfg
	}
	return resp
}

func modelTestResultToAPI(r domain.ModelTestResult) ModelTestResult {
	ct := r.CreatedAt
	status := ModelTestResultStatus(r.Status)
	resp := ModelTestResult{
		Id:        &r.ID,
		RunStepId: &r.RunStepID,
		TestId:    &r.TestID,
		TestName:  &r.TestName,
		Status:    &status,
		CreatedAt: &ct,
	}
	if r.RowsReturned != nil {
		resp.RowsReturned = r.RowsReturned
	}
	if r.ErrorMessage != nil {
		resp.ErrorMessage = r.ErrorMessage
	}
	return resp
}

func apiModelTestConfig(c domain.ModelTestConfig) ModelTestConfig {
	cfg := ModelTestConfig{}
	if len(c.Values) > 0 {
		cfg.Values = &c.Values
	}
	if c.ToModel != "" {
		cfg.ToModel = &c.ToModel
	}
	if c.ToColumn != "" {
		cfg.ToColumn = &c.ToColumn
	}
	if c.SQL != "" {
		cfg.CustomSql = &c.SQL
	}
	return cfg
}

func domainModelTestConfig(c ModelTestConfig) domain.ModelTestConfig {
	cfg := domain.ModelTestConfig{}
	if c.Values != nil {
		cfg.Values = *c.Values
	}
	if c.ToModel != nil {
		cfg.ToModel = *c.ToModel
	}
	if c.ToColumn != nil {
		cfg.ToColumn = *c.ToColumn
	}
	if c.CustomSql != nil {
		cfg.SQL = *c.CustomSql
	}
	return cfg
}

func apiModelContract(c domain.ModelContract) ModelContract {
	resp := ModelContract{}
	resp.Enforce = &c.Enforce
	if len(c.Columns) > 0 {
		cols := make([]ModelContractColumn, len(c.Columns))
		for i, col := range c.Columns {
			nullable := col.Nullable
			cols[i] = ModelContractColumn{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: &nullable,
			}
		}
		resp.Columns = &cols
	}
	return resp
}

func domainModelContract(c ModelContract) domain.ModelContract {
	resp := domain.ModelContract{}
	if c.Enforce != nil {
		resp.Enforce = *c.Enforce
	}
	if c.Columns != nil {
		resp.Columns = make([]domain.ModelContractColumn, len(*c.Columns))
		for i, col := range *c.Columns {
			resp.Columns[i].Name = col.Name
			resp.Columns[i].Type = col.Type
			if col.Nullable != nil {
				resp.Columns[i].Nullable = *col.Nullable
			}
		}
	}
	return resp
}

func apiFreshnessPolicy(f domain.FreshnessPolicy) FreshnessPolicy {
	resp := FreshnessPolicy{}
	if f.MaxLagSeconds != 0 {
		resp.MaxLagSeconds = &f.MaxLagSeconds
	}
	if f.CronSchedule != "" {
		resp.CronSchedule = &f.CronSchedule
	}
	return resp
}

func domainFreshnessPolicy(f FreshnessPolicy) domain.FreshnessPolicy {
	resp := domain.FreshnessPolicy{}
	if f.MaxLagSeconds != nil {
		resp.MaxLagSeconds = *f.MaxLagSeconds
	}
	if f.CronSchedule != nil {
		resp.CronSchedule = *f.CronSchedule
	}
	return resp
}

// === Freshness ===

// CheckModelFreshness implements the endpoint for checking a model's freshness status.
func (h *APIHandler) CheckModelFreshness(ctx context.Context, req CheckModelFreshnessRequestObject) (CheckModelFreshnessResponseObject, error) {
	result, err := h.models.CheckFreshness(ctx, req.ProjectName, req.ModelName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CheckModelFreshness404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CheckModelFreshness200JSONResponse{
		Body:    freshnessStatusToAPI(*result),
		Headers: CheckModelFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func freshnessStatusToAPI(s domain.FreshnessStatus) FreshnessStatus {
	resp := FreshnessStatus{
		IsFresh:       &s.IsFresh,
		MaxLagSeconds: &s.MaxLagSeconds,
	}
	if s.LastRunAt != nil {
		resp.LastRunAt = s.LastRunAt
	}
	if s.StaleSince != nil {
		resp.StaleSince = s.StaleSince
	}
	return resp
}

// === Notebook Promotion ===

// PromoteNotebookToModel implements the endpoint for promoting a notebook cell to a model.
func (h *APIHandler) PromoteNotebookToModel(ctx context.Context, req PromoteNotebookToModelRequestObject) (PromoteNotebookToModelResponseObject, error) {
	domReq := domain.PromoteNotebookRequest{
		NotebookID:  req.Body.NotebookId,
		CellIndex:   int(req.Body.CellIndex),
		ProjectName: req.Body.ProjectName,
		Name:        req.Body.Name,
	}
	if req.Body.Materialization != nil {
		domReq.Materialization = string(*req.Body.Materialization)
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.PromoteNotebook(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return PromoteNotebookToModel403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return PromoteNotebookToModel404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return PromoteNotebookToModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return PromoteNotebookToModel409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return PromoteNotebookToModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return PromoteNotebookToModel201JSONResponse{
		Body:    modelToAPI(*result),
		Headers: PromoteNotebookToModel201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
