package api

import (
	"context"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/model"
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
	CheckSourceFreshness(ctx context.Context, principal, sourceSchema, sourceTable, timestampColumn string, maxLagSeconds int64) (*domain.SourceFreshnessStatus, error)
	PromoteNotebook(ctx context.Context, principal string, req domain.PromoteNotebookRequest) (*domain.Model, error)
	UnpublishNotebook(ctx context.Context, principal, notebookID string) error
	GetBuildLineage(ctx context.Context, buildID string, modelName *string) ([]domain.CompiledColumnLineage, error)
	GetCompilationLineage(ctx context.Context, compilationID string, modelName *string) ([]domain.CompiledColumnLineage, error)
	GetBuildDiagnostics(ctx context.Context, buildID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error)
	GetCompilationDiagnostics(ctx context.Context, compilationID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error)
	GetBuildSourceColumnImpact(ctx context.Context, buildID, schema, table, column string) ([]domain.CompiledColumnLineage, error)
	GetCompilationSourceColumnImpact(ctx context.Context, compilationID, schema, table, column string) ([]domain.CompiledColumnLineage, error)
	PlanRebuild(ctx context.Context, principal string, req domain.PlanRebuildRequest) (*domain.RebuildPlan, error)
	CompareBuilds(ctx context.Context, principal string, req domain.CompareBuildsRequest) (*domain.BuildCompareResult, error)
	GetModelImpact(ctx context.Context, projectName string, buildID *string, modelName string) (*domain.BuildImpactResult, error)
	GetMacroImpact(ctx context.Context, projectName string, buildID *string, macroName string) (*domain.BuildImpactResult, error)
	GetCompilation(ctx context.Context, compilationID string) (*domain.Compilation, error)
	ListCompilationsForEnvironment(ctx context.Context, projectName, environmentName string, page domain.PageRequest) ([]domain.Compilation, int64, error)
	GetCompilationModelImpact(ctx context.Context, compilationID string, modelName string) (*domain.BuildImpactResult, error)
	GetCompilationMacroImpact(ctx context.Context, compilationID string, macroName string) (*domain.BuildImpactResult, error)
	CreateCompilation(ctx context.Context, principal string, projectName string, environmentName string, req domain.CreateCompilationRequest) (*domain.Compilation, error)
	CreateEnvironmentBuild(ctx context.Context, principal string, projectName string, environmentName string, req domain.CreateCompilationRequest) (*domain.Build, error)
	ListRunsForBuild(ctx context.Context, buildID string, page domain.PageRequest) ([]domain.ModelRun, int64, error)
	CreateRunForBuild(ctx context.Context, principal string, build *domain.Build) (*domain.ModelRun, error)
}

// === Models ===

// ListModels implements the endpoint for listing transformation models.
func (h *APIHandler) ListModels(ctx context.Context, req GenListModelsRequest) (GenListModelsResponse, error) {
	if isNilService(h.models) {
		empty := []Model{}
		return GenListModels200JSONResponse{
			Body:    PaginatedModels{Data: empty, NextPageToken: nil},
			Headers: GenListModels200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
		}, nil
	}

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
	return GenListModels200JSONResponse{
		Body:    PaginatedModels{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListModels200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateModel implements the endpoint for creating a new transformation model.
func (h *APIHandler) CreateModel(ctx context.Context, req GenCreateModelRequest) (GenCreateModelResponse, error) {
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
		if resp, ok := respondDomainErrorForOperation[GenCreateModelResponse]("createModel", err, domainErrorResponder[GenCreateModelResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateModelResponse { return CreateModel400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateModelResponse { return CreateModel403JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateModelResponse { return CreateModel409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreateModel400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateModel201JSONResponse{
		Body:    modelToAPI(*result),
		Headers: GenCreateModel201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModel implements the endpoint for retrieving a model by project and name.
func (h *APIHandler) GetModel(ctx context.Context, req GenGetModelRequest) (GenGetModelResponse, error) {
	result, err := h.models.GetModel(ctx, req.ProjectName, req.ModelName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetModelResponse]("getModel", err, domainErrorResponder[GenGetModelResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetModelResponse {
				return GenGetModel404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetModel200JSONResponse{
		Body:    modelToAPI(*result),
		Headers: GenGetModel200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateModel implements the endpoint for updating a transformation model.
func (h *APIHandler) UpdateModel(ctx context.Context, req GenUpdateModelRequest) (GenUpdateModelResponse, error) {
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
		if resp, ok := respondDomainErrorForOperation[GenUpdateModelResponse]("updateModel", err, domainErrorResponder[GenUpdateModelResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateModelResponse { return UpdateModel400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenUpdateModelResponse { return UpdateModel403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenUpdateModelResponse { return UpdateModel404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateModel200JSONResponse{
		Body:    modelToAPI(*result),
		Headers: GenUpdateModel200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteModel implements the endpoint for deleting a transformation model.
func (h *APIHandler) DeleteModel(ctx context.Context, req GenDeleteModelRequest) (GenDeleteModelResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.models.DeleteModel(ctx, principal, req.ProjectName, req.ModelName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteModelResponse]("deleteModel", err, domainErrorResponder[GenDeleteModelResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteModelResponse { return DeleteModel403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteModelResponse { return DeleteModel404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteModel204Response{
		Headers: GenDeleteModel204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModelDAG implements the endpoint for retrieving the model dependency DAG.
func (h *APIHandler) GetModelDAG(ctx context.Context, req GenGetModelDAGRequest) (GenGetModelDAGResponse, error) {
	tiers, err := h.models.GetDAG(ctx, req.Params.ProjectName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetModelDAGResponse]("getModelDAG", err, domainErrorResponder[GenGetModelDAGResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetModelDAGResponse { return GetModelDAG400JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetModelDAG200JSONResponse{
		Body:    dagToAPI(tiers),
		Headers: GenGetModelDAG200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Model Runs ===

// TriggerModelRun implements the endpoint for triggering a model run.
func (h *APIHandler) TriggerModelRun(ctx context.Context, req GenTriggerModelRunRequest) (GenTriggerModelRunResponse, error) {
	domReq := domain.TriggerModelRunRequest{
		ProjectName: req.Body.ProjectName,
		TriggerType: domain.ModelTriggerTypeManual,
	}
	if req.Body.EnvironmentName != nil {
		domReq.EnvironmentName = *req.Body.EnvironmentName
	}
	if req.Body.FullRefresh != nil {
		domReq.FullRefresh = *req.Body.FullRefresh
	}
	if req.Body.ModelNames != nil && len(*req.Body.ModelNames) > 0 {
		domReq.Selector = strings.Join(*req.Body.ModelNames, ",")
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.TriggerRun(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenTriggerModelRunResponse]("triggerModelRun", err, domainErrorResponder[GenTriggerModelRunResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenTriggerModelRunResponse {
				return TriggerModelRun400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenTriggerModelRunResponse {
				return TriggerModelRun403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenTriggerModelRunResponse {
				return TriggerModelRun409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return TriggerModelRun400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenTriggerModelRun201JSONResponse{
		Body:    modelRunToAPI(*result),
		Headers: GenTriggerModelRun201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelRuns implements the endpoint for listing model runs.
func (h *APIHandler) ListModelRuns(ctx context.Context, req GenListModelRunsRequest) (GenListModelRunsResponse, error) {
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
	return GenListModelRuns200JSONResponse{
		Body:    PaginatedModelRuns{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListModelRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModelRun implements the endpoint for retrieving a model run.
func (h *APIHandler) GetModelRun(ctx context.Context, req GenGetModelRunRequest) (GenGetModelRunResponse, error) {
	result, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetModelRunResponse]("getModelRun", err, domainErrorResponder[GenGetModelRunResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetModelRunResponse {
				return GenGetModelRun404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetModelRun200JSONResponse{
		Body:    modelRunToAPI(*result),
		Headers: GenGetModelRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelRunSteps implements the endpoint for listing model run steps.
func (h *APIHandler) ListModelRunSteps(ctx context.Context, req GenListModelRunStepsRequest) (GenListModelRunStepsResponse, error) {
	steps, err := h.models.ListRunSteps(ctx, req.RunId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListModelRunStepsResponse]("listModelRunSteps", err, domainErrorResponder[GenListModelRunStepsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListModelRunStepsResponse {
				return GenListModelRunSteps404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ModelRunStep, len(steps))
	for i, s := range steps {
		data[i] = modelRunStepToAPI(s)
	}
	return GenListModelRunSteps200JSONResponse{
		Body:    ModelRunStepList{Data: data},
		Headers: GenListModelRunSteps200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelModelRun implements the endpoint for cancelling a model run.
func (h *APIHandler) CancelModelRun(ctx context.Context, req GenCancelModelRunRequest) (GenCancelModelRunResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.models.CancelRun(ctx, principal, req.RunId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCancelModelRunResponse]("cancelModelRun", err, domainErrorResponder[GenCancelModelRunResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCancelModelRunResponse {
				return CancelModelRun400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCancelModelRunResponse { return CancelModelRun403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenCancelModelRunResponse { return CancelModelRun404JSONResponse{resp} },
			Conflict:  func(resp ConflictJSONResponse) GenCancelModelRunResponse { return CancelModelRun409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
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
		CreatedAt:       formatTimePtr(&m.CreatedAt),
		UpdatedAt:       formatTimePtr(&m.UpdatedAt),
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
	resp := ModelRun{
		Id:          &r.ID,
		Status:      strPtrIfNonEmpty(r.Status),
		TriggerType: strPtrIfNonEmpty(r.TriggerType),
		TriggeredBy: &r.TriggeredBy,
		CreatedAt:   formatTimePtr(&r.CreatedAt),
	}
	if r.ProjectName != "" {
		resp.ProjectName = &r.ProjectName
	}
	if r.EnvironmentName != "" {
		resp.EnvironmentName = &r.EnvironmentName
	}
	if r.BuildID != nil {
		resp.BuildId = r.BuildID
	}
	resp.FullRefresh = &r.FullRefresh
	if r.CompileManifest != nil {
		resp.CompileManifest = r.CompileManifest
	}
	if r.CompileDiagnostics != nil {
		resp.CompileDiagnostics = modelCompileDiagnosticsToAPI(r.CompileDiagnostics)
	}
	if names := selectorToModelNames(r.ModelSelector); len(names) > 0 {
		resp.ModelNames = &names
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

func modelCompileDiagnosticsToAPI(item *domain.ModelCompileDiagnostics) *ModelRunCompileDiagnostics {
	if item == nil {
		return nil
	}
	warnings := append([]string(nil), item.Warnings...)
	errors := append([]string(nil), item.Errors...)
	diagnostics := make([]CompileDiagnostic, 0, len(item.Items))
	for _, entry := range item.Items {
		diagnostics = append(diagnostics, compileDiagnosticToAPI(entry))
	}
	return &ModelRunCompileDiagnostics{
		Items:    &diagnostics,
		Warnings: &warnings,
		Errors:   &errors,
	}
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
	if strings.HasPrefix(selector, "state:") {
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
	resp := ModelRunStep{
		Id:        &s.ID,
		RunId:     &s.RunID,
		ModelName: &s.ModelName,
		Status:    strPtrIfNonEmpty(s.Status),
		CreatedAt: formatTimePtr(&s.CreatedAt),
	}
	if s.CompiledSQL != nil {
		resp.CompiledSql = s.CompiledSQL
	}
	if s.CompiledHash != nil {
		resp.CompiledHash = s.CompiledHash
	}
	if len(s.DependsOn) > 0 {
		resp.DependsOn = &s.DependsOn
	}
	if len(s.VarsUsed) > 0 {
		resp.VarsUsed = &s.VarsUsed
	}
	if len(s.MacrosUsed) > 0 {
		resp.MacrosUsed = &s.MacrosUsed
	}
	if s.RowsAffected != nil {
		resp.RowsAffected = safeInt64ToInt32Ptr(s.RowsAffected)
	}
	if s.StartedAt != nil {
		resp.StartedAt = formatTimePtr(s.StartedAt)
	}
	if s.FinishedAt != nil {
		resp.FinishedAt = formatTimePtr(s.FinishedAt)
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
			mat := ModelMaterialization(node.Model.Materialization)
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
	if c.OnSchemaChange != "" {
		osc := ModelConfigOnSchemaChange(c.OnSchemaChange)
		cfg.OnSchemaChange = &osc
	}
	return cfg
}

func domainModelConfig(c GenSchemaModelConfig) domain.ModelConfig {
	cfg := domain.ModelConfig{}
	if c.UniqueKey != nil {
		cfg.UniqueKey = *c.UniqueKey
	}
	if c.IncrementalStrategy != nil {
		cfg.IncrementalStrategy = *c.IncrementalStrategy
	}
	if c.OnSchemaChange != nil {
		cfg.OnSchemaChange = string(*c.OnSchemaChange)
	}
	return cfg
}

// === Model Tests ===

// CreateModelTest implements the endpoint for creating a model test.
func (h *APIHandler) CreateModelTest(ctx context.Context, req GenCreateModelTestRequest) (GenCreateModelTestResponse, error) {
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
		if resp, ok := respondDomainErrorForOperation[GenCreateModelTestResponse]("createModelTest", err, domainErrorResponder[GenCreateModelTestResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateModelTestResponse {
				return CreateModelTest400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateModelTestResponse {
				return CreateModelTest403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateModelTestResponse {
				return CreateModelTest404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateModelTestResponse {
				return CreateModelTest409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CreateModelTest400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateModelTest201JSONResponse{
		Body:    modelTestToAPI(*result),
		Headers: GenCreateModelTest201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelTests implements the endpoint for listing tests for a model.
func (h *APIHandler) ListModelTests(ctx context.Context, req GenListModelTestsRequest) (GenListModelTestsResponse, error) {
	tests, err := h.models.ListTests(ctx, req.ProjectName, req.ModelName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListModelTestsResponse]("listModelTests", err, domainErrorResponder[GenListModelTestsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListModelTestsResponse {
				return GenListModelTests404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ModelTest, len(tests))
	for i, t := range tests {
		data[i] = modelTestToAPI(t)
	}
	return GenListModelTests200JSONResponse{
		Body:    ModelTestList{Data: data},
		Headers: GenListModelTests200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteModelTest implements the endpoint for deleting a model test.
func (h *APIHandler) DeleteModelTest(ctx context.Context, req GenDeleteModelTestRequest) (GenDeleteModelTestResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.models.DeleteTest(ctx, principal, req.ProjectName, req.ModelName, req.TestId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteModelTestResponse]("deleteModelTest", err, domainErrorResponder[GenDeleteModelTestResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteModelTestResponse {
				return DeleteModelTest403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteModelTestResponse {
				return DeleteModelTest404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteModelTest204Response{
		Headers: GenDeleteModelTest204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListModelTestResults implements the endpoint for listing test results for a run step.
func (h *APIHandler) ListModelTestResults(ctx context.Context, req GenListModelTestResultsRequest) (GenListModelTestResultsResponse, error) {
	results, err := h.models.ListTestResults(ctx, req.RunId, req.StepId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListModelTestResultsResponse]("listModelTestResults", err, domainErrorResponder[GenListModelTestResultsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListModelTestResultsResponse {
				return GenListModelTestResults404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]ModelTestResult, len(results))
	for i, r := range results {
		data[i] = modelTestResultToAPI(r)
	}
	return GenListModelTestResults200JSONResponse{
		Body:    ModelTestResultList{Data: data},
		Headers: GenListModelTestResults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Model Test Mappers ===

func modelTestToAPI(t domain.ModelTest) ModelTest {
	tt := ModelTestTestType(t.TestType)
	resp := ModelTest{
		Id:        &t.ID,
		ModelId:   &t.ModelID,
		Name:      &t.Name,
		TestType:  &tt,
		CreatedAt: formatTimePtr(&t.CreatedAt),
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
	status := ModelTestResultStatus(r.Status)
	resp := ModelTestResult{
		Id:        &r.ID,
		RunStepId: &r.RunStepID,
		TestId:    &r.TestID,
		TestName:  &r.TestName,
		Status:    &status,
		CreatedAt: formatTimePtr(&r.CreatedAt),
	}
	if r.RowsReturned != nil {
		resp.RowsReturned = safeInt64ToInt32Ptr(r.RowsReturned)
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

func domainModelTestConfig(c GenSchemaModelTestConfig) domain.ModelTestConfig {
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

func domainModelContract(c GenSchemaModelContract) domain.ModelContract {
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
		value := safeInt64ToInt32(f.MaxLagSeconds)
		resp.MaxLagSeconds = &value
	}
	if f.CronSchedule != "" {
		resp.CronSchedule = &f.CronSchedule
	}
	return resp
}

func domainFreshnessPolicy(f GenSchemaFreshnessPolicy) domain.FreshnessPolicy {
	resp := domain.FreshnessPolicy{}
	if f.MaxLagSeconds != nil {
		resp.MaxLagSeconds = int64(*f.MaxLagSeconds)
	}
	if f.CronSchedule != nil {
		resp.CronSchedule = *f.CronSchedule
	}
	return resp
}

// === Freshness ===

// CheckModelFreshness implements the endpoint for checking a model's freshness status.
func (h *APIHandler) CheckModelFreshness(ctx context.Context, req GenCheckModelFreshnessRequest) (GenCheckModelFreshnessResponse, error) {
	result, err := h.models.CheckFreshness(ctx, req.ProjectName, req.ModelName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCheckModelFreshnessResponse]("checkModelFreshness", err, domainErrorResponder[GenCheckModelFreshnessResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenCheckModelFreshnessResponse {
				return GenCheckModelFreshness404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCheckModelFreshness200JSONResponse{
		Body:    freshnessStatusToAPI(*result),
		Headers: GenCheckModelFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func freshnessStatusToAPI(s domain.FreshnessStatus) FreshnessStatus {
	resp := FreshnessStatus{
		IsFresh:       &s.IsFresh,
		MaxLagSeconds: safeInt64ToInt32Ptr(&s.MaxLagSeconds),
	}
	if s.LastRunAt != nil {
		resp.LastRunAt = formatTimePtr(s.LastRunAt)
	}
	if s.StaleSince != nil {
		resp.StaleSince = formatTimePtr(s.StaleSince)
	}
	return resp
}

// CheckSourceFreshness implements the endpoint for checking source freshness status.
func (h *APIHandler) CheckSourceFreshness(ctx context.Context, req GenCheckSourceFreshnessRequest) (GenCheckSourceFreshnessResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	maxLagSeconds := int64(3600)
	if req.Params.MaxLagSeconds != nil {
		maxLagSeconds = *req.Params.MaxLagSeconds
	}
	timestampColumn := ""
	if req.Params.TimestampColumn != nil {
		timestampColumn = *req.Params.TimestampColumn
	}

	result, err := h.models.CheckSourceFreshness(ctx, principal, req.SourceSchema, req.SourceTable, timestampColumn, maxLagSeconds)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCheckSourceFreshnessResponse]("checkSourceFreshness", err, domainErrorResponder[GenCheckSourceFreshnessResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCheckSourceFreshnessResponse {
				return CheckSourceFreshness400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCheckSourceFreshnessResponse {
				return GenCheckSourceFreshness404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenCheckSourceFreshness200JSONResponse{
		Body:    sourceFreshnessStatusToAPI(*result),
		Headers: GenCheckSourceFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func sourceFreshnessStatusToAPI(s domain.SourceFreshnessStatus) SourceFreshnessStatus {
	resp := SourceFreshnessStatus{
		IsFresh:       &s.IsFresh,
		SourceSchema:  &s.SourceSchema,
		SourceTable:   &s.SourceTable,
		MaxLagSeconds: safeInt64ToInt32Ptr(&s.MaxLagSeconds),
	}
	if s.TimestampCol != "" {
		resp.TimestampColumn = &s.TimestampCol
	}
	if s.LastLoadedAt != nil {
		resp.LastLoadedAt = formatTimePtr(s.LastLoadedAt)
	}
	if s.StaleSince != nil {
		resp.StaleSince = formatTimePtr(s.StaleSince)
	}
	return resp
}

// === Notebook Promotion ===

// PromoteNotebookToModel implements the endpoint for promoting a notebook cell to a model.
func (h *APIHandler) PromoteNotebookToModel(ctx context.Context, req GenPromoteNotebookToModelRequest) (GenPromoteNotebookToModelResponse, error) {
	_, cells, err := h.notebooks.GetNotebook(ctx, req.NotebookId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenPromoteNotebookToModelResponse]("promoteNotebookToModel", err, domainErrorResponder[GenPromoteNotebookToModelResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenPromoteNotebookToModelResponse {
				return PromoteNotebookToModel404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	var outputCellID string
	for i := range cells {
		if cells[i].Position == int(req.Body.CellIndex) {
			outputCellID = cells[i].ID
			break
		}
	}
	if outputCellID == "" {
		return PromoteNotebookToModel400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "output cell not found for requested cell_index"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	domReq := domain.PromoteNotebookRequest{
		NotebookID:   req.NotebookId,
		OutputCellID: outputCellID,
		ProjectName:  req.Body.ProjectName,
		Name:         req.Body.Name,
	}
	if req.Body.Materialization != nil {
		domReq.Materialization = string(*req.Body.Materialization)
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.models.PromoteNotebook(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenPromoteNotebookToModelResponse]("promoteNotebookToModel", err, domainErrorResponder[GenPromoteNotebookToModelResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenPromoteNotebookToModelResponse {
				return PromoteNotebookToModel400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenPromoteNotebookToModelResponse {
				return PromoteNotebookToModel403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenPromoteNotebookToModelResponse {
				return PromoteNotebookToModel404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenPromoteNotebookToModelResponse {
				return PromoteNotebookToModel409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return PromoteNotebookToModel400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenPromoteNotebookToModel201JSONResponse{
		Body:    modelToAPI(*result),
		Headers: GenPromoteNotebookToModel201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UnpublishNotebookModel implements the endpoint for removing a notebook-backed published model.
func (h *APIHandler) UnpublishNotebookModel(ctx context.Context, req GenUnpublishNotebookModelRequest) (GenUnpublishNotebookModelResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	if _, _, err := h.notebooks.GetNotebookForPrincipal(ctx, cp.Name, cp.IsAdmin, req.NotebookId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUnpublishNotebookModelResponse]("unpublishNotebookModel", err, domainErrorResponder[GenUnpublishNotebookModelResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUnpublishNotebookModelResponse {
				return UnpublishNotebookModel403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUnpublishNotebookModelResponse {
				return UnpublishNotebookModel404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	if err := h.models.UnpublishNotebook(ctx, cp.Name, req.NotebookId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUnpublishNotebookModelResponse]("unpublishNotebookModel", err, domainErrorResponder[GenUnpublishNotebookModelResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUnpublishNotebookModelResponse {
				return UnpublishNotebookModel400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUnpublishNotebookModelResponse {
				return UnpublishNotebookModel403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUnpublishNotebookModelResponse {
				return UnpublishNotebookModel404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GenUnpublishNotebookModel204Response{}, nil
}
