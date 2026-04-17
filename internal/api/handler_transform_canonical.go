package api

import (
	"context"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func (h *APIHandler) canonicalProject(ctx context.Context, projectID string) (*domain.ContextPrincipal, *domain.Project, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	project, err := h.projectsCtl.GetProjectForPrincipal(ctx, cp.Name, cp.IsAdmin, projectID)
	if err != nil {
		return nil, nil, err
	}
	return &cp, project, nil
}

func (h *APIHandler) canonicalEnvironment(ctx context.Context, projectID, environmentID string) (*domain.ContextPrincipal, *domain.Project, *domain.Environment, error) {
	cp, project, err := h.canonicalProject(ctx, projectID)
	if err != nil {
		return nil, nil, nil, err
	}
	environment, err := h.projectsCtl.GetEnvironmentForProject(ctx, cp.Name, cp.IsAdmin, project.ID, environmentID)
	if err != nil {
		return nil, nil, nil, err
	}
	return cp, project, environment, nil
}

func filterAndPaginateMacros(items []domain.Macro, projectName string, page domain.PageRequest) ([]domain.Macro, int64) {
	filtered := make([]domain.Macro, 0)
	for _, item := range items {
		if strings.TrimSpace(item.ProjectName) == strings.TrimSpace(projectName) {
			filtered = append(filtered, item)
		}
	}
	total := int64(len(filtered))
	start := page.Offset()
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + page.Limit()
	if end > len(filtered) {
		end = len(filtered)
	}
	return append([]domain.Macro(nil), filtered[start:end]...), total
}

func canonicalSourceOverride(overrides map[string]string, sourceName, tableName string) (string, bool) {
	if overrides == nil {
		return "", false
	}
	key := strings.ToLower(strings.TrimSpace(sourceName) + "." + strings.TrimSpace(tableName))
	value, ok := overrides[key]
	return strings.TrimSpace(value), ok
}

func canonicalParseRelationRef(relationRef, defaultCatalog, defaultSchema string) (string, string, string) {
	parts := make([]string, 0)
	for _, part := range strings.Split(strings.TrimSpace(relationRef), ".") {
		part = strings.TrimSpace(strings.Trim(part, `"`))
		if part != "" {
			parts = append(parts, part)
		}
	}
	switch len(parts) {
	case 1:
		return strings.TrimSpace(defaultCatalog), strings.TrimSpace(defaultSchema), parts[0]
	case 2:
		return strings.TrimSpace(defaultCatalog), parts[0], parts[1]
	default:
		return parts[0], parts[1], parts[2]
	}
}

func (h *APIHandler) canonicalSourceRelation(ctx context.Context, projectID, environmentID, sourceName, tableName string) (*domain.Environment, *domain.SourceDefinition, string, string, string, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, projectID, environmentID)
	if err != nil {
		return nil, nil, "", "", "", err
	}
	source, err := h.projectsCtl.GetSourceForProject(ctx, cp.Name, cp.IsAdmin, projectID, sourceName, tableName)
	if err != nil {
		return nil, nil, "", "", "", err
	}
	relationRef := source.RelationRef
	if override, ok := canonicalSourceOverride(environment.SourceOverrides, sourceName, tableName); ok {
		relationRef = override
	}
	catalog, schema, table := canonicalParseRelationRef(relationRef, environment.TargetCatalog, environment.TargetSchema)
	_ = catalog
	return environment, source, schema, table, relationRef, nil
}

func ensureRunScope(run *domain.ModelRun, projectName, environmentName string) error {
	if run == nil {
		return domain.ErrNotFound("run not found")
	}
	if strings.TrimSpace(run.ProjectName) != strings.TrimSpace(projectName) || strings.TrimSpace(run.EnvironmentName) != strings.TrimSpace(environmentName) {
		return domain.ErrNotFound("run not found")
	}
	return nil
}

func stringEnumPtr[T ~string](value *T) *string {
	if value == nil {
		return nil
	}
	str := string(*value)
	return &str
}

func derefStringMap(values *map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(*values))
	for key, value := range *values {
		cloned[key] = value
	}
	return cloned
}

// GetProjectDAGByID returns the DAG for a project-scoped transformation project.
func (h *APIHandler) GetProjectDAGByID(ctx context.Context, req GenGetProjectDAGByIDRequest) (GenGetProjectDAGByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	tiers, err := h.models.GetDAG(ctx, &project.Name)
	if err != nil {
		return nil, err
	}
	return GenGetProjectDAGByID200JSONResponse{
		Body:    dagToAPI(tiers),
		Headers: GenGetProjectDAGByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectModelsByID lists authored models for a project.
func (h *APIHandler) ListProjectModelsByID(ctx context.Context, req GenListProjectModelsByIDRequest) (GenListProjectModelsByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.models.ListModels(ctx, &project.Name, page)
	if err != nil {
		return nil, err
	}
	data := make([]Model, len(items))
	for i := range items {
		data[i] = modelToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListProjectModelsByID200JSONResponse{
		Body:    PaginatedModels{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListProjectModelsByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectModelByID creates a model within a project.
func (h *APIHandler) CreateProjectModelByID(ctx context.Context, req GenCreateProjectModelByIDRequest) (GenCreateProjectModelByIDResponse, error) {
	if req.Body == nil {
		return CreateProjectModelByID400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	domReq := domain.CreateModelRequest{
		ProjectName: project.Name,
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
	item, err := h.models.CreateModel(ctx, cp.Name, domReq)
	if err != nil {
		return nil, err
	}
	return GenCreateProjectModelByID201JSONResponse{
		Body:    modelToAPI(*item),
		Headers: GenCreateProjectModelByID201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectModelByID returns one project-scoped model.
func (h *APIHandler) GetProjectModelByID(ctx context.Context, req GenGetProjectModelByIDRequest) (GenGetProjectModelByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	item, err := h.models.GetModel(ctx, project.Name, req.ModelName)
	if err != nil {
		return nil, err
	}
	return GenGetProjectModelByID200JSONResponse{
		Body:    modelToAPI(*item),
		Headers: GenGetProjectModelByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateProjectModelByID updates one project-scoped model.
func (h *APIHandler) UpdateProjectModelByID(ctx context.Context, req GenUpdateProjectModelByIDRequest) (GenUpdateProjectModelByIDResponse, error) {
	if req.Body == nil {
		return UpdateProjectModelByID400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	domReq := domain.UpdateModelRequest{SQL: req.Body.Sql, Description: req.Body.Description}
	if req.Body.Materialization != nil {
		value := string(*req.Body.Materialization)
		domReq.Materialization = &value
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
	item, err := h.models.UpdateModel(ctx, cp.Name, project.Name, req.ModelName, domReq)
	if err != nil {
		return nil, err
	}
	return GenUpdateProjectModelByID200JSONResponse{
		Body:    modelToAPI(*item),
		Headers: GenUpdateProjectModelByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectModelByID deletes one project-scoped model.
func (h *APIHandler) DeleteProjectModelByID(ctx context.Context, req GenDeleteProjectModelByIDRequest) (GenDeleteProjectModelByIDResponse, error) {
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	if err := h.models.DeleteModel(ctx, cp.Name, project.Name, req.ModelName); err != nil {
		return nil, err
	}
	return GenDeleteProjectModelByID204Response{
		Headers: GenDeleteProjectModelByID204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectModelTestByID creates a model test beneath a project model.
func (h *APIHandler) CreateProjectModelTestByID(ctx context.Context, req GenCreateProjectModelTestByIDRequest) (GenCreateProjectModelTestByIDResponse, error) {
	if req.Body == nil {
		return CreateProjectModelTestByID400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	domReq := domain.CreateModelTestRequest{
		Name:     req.Body.Name,
		TestType: string(req.Body.TestType),
		Column:   derefString(req.Body.Column),
	}
	if req.Body.Config != nil {
		domReq.Config = domainModelTestConfig(*req.Body.Config)
	}
	item, err := h.models.CreateTest(ctx, cp.Name, project.Name, req.ModelName, domReq)
	if err != nil {
		return nil, err
	}
	return GenCreateProjectModelTestByID201JSONResponse{
		Body:    modelTestToAPI(*item),
		Headers: GenCreateProjectModelTestByID201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectModelTestsByID lists tests for a project model.
func (h *APIHandler) ListProjectModelTestsByID(ctx context.Context, req GenListProjectModelTestsByIDRequest) (GenListProjectModelTestsByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	items, err := h.models.ListTests(ctx, project.Name, req.ModelName)
	if err != nil {
		return nil, err
	}
	data := make([]ModelTest, len(items))
	for i := range items {
		data[i] = modelTestToAPI(items[i])
	}
	return GenListProjectModelTestsByID200JSONResponse{
		Body:    ModelTestList{Data: data},
		Headers: GenListProjectModelTestsByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectModelTestByID deletes a test from a project model.
func (h *APIHandler) DeleteProjectModelTestByID(ctx context.Context, req GenDeleteProjectModelTestByIDRequest) (GenDeleteProjectModelTestByIDResponse, error) {
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	if err := h.models.DeleteTest(ctx, cp.Name, project.Name, req.ModelName, req.TestId); err != nil {
		return nil, err
	}
	return GenDeleteProjectModelTestByID204Response{
		Headers: GenDeleteProjectModelTestByID204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectMacrosByID lists macros authored in a project.
func (h *APIHandler) ListProjectMacrosByID(ctx context.Context, req GenListProjectMacrosByIDRequest) (GenListProjectMacrosByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, _, err := h.macros.List(ctx, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}
	filtered, total := filterAndPaginateMacros(items, project.Name, page)
	data := make([]Macro, len(filtered))
	for i := range filtered {
		data[i] = macroToAPI(filtered[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListProjectMacrosByID200JSONResponse{
		Body:    PaginatedMacros{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListProjectMacrosByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectMacroByID creates a macro within a project.
func (h *APIHandler) CreateProjectMacroByID(ctx context.Context, req GenCreateProjectMacroByIDRequest) (GenCreateProjectMacroByIDResponse, error) {
	if req.Body == nil {
		return CreateProjectMacroByID400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	domReq := domain.CreateMacroRequest{
		Name:        req.Body.Name,
		Body:        req.Body.Body,
		ProjectName: project.Name,
	}
	if req.Body.MacroType != nil {
		domReq.MacroType = string(*req.Body.MacroType)
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	if req.Body.Parameters != nil {
		domReq.Parameters = *req.Body.Parameters
	}
	if req.Body.CatalogName != nil {
		domReq.CatalogName = *req.Body.CatalogName
	}
	if req.Body.Owner != nil {
		domReq.Owner = *req.Body.Owner
	}
	if req.Body.Properties != nil {
		domReq.Properties = anyMapToStringMap(req.Body.Properties)
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}
	if req.Body.Status != nil {
		domReq.Status = string(*req.Body.Status)
	}
	item, err := h.macros.Create(ctx, cp.Name, domReq)
	if err != nil {
		return nil, err
	}
	return GenCreateProjectMacroByID201JSONResponse{
		Body:    macroToAPI(*item),
		Headers: GenCreateProjectMacroByID201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectMacroByID returns one project-scoped macro.
func (h *APIHandler) GetProjectMacroByID(ctx context.Context, req GenGetProjectMacroByIDRequest) (GenGetProjectMacroByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	item, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(item.ProjectName) != strings.TrimSpace(project.Name) {
		return nil, domain.ErrNotFound("macro not found")
	}
	return GenGetProjectMacroByID200JSONResponse{
		Body:    macroToAPI(*item),
		Headers: GenGetProjectMacroByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateProjectMacroByID updates one project-scoped macro.
func (h *APIHandler) UpdateProjectMacroByID(ctx context.Context, req GenUpdateProjectMacroByIDRequest) (GenUpdateProjectMacroByIDResponse, error) {
	if req.Body == nil {
		return UpdateProjectMacroByID400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	current, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(current.ProjectName) != strings.TrimSpace(project.Name) {
		return nil, domain.ErrNotFound("macro not found")
	}
	projectName := project.Name
	domReq := domain.UpdateMacroRequest{
		Parameters:  derefStringSlice(req.Body.Parameters),
		Body:        req.Body.Body,
		Description: req.Body.Description,
		CatalogName: req.Body.CatalogName,
		ProjectName: &projectName,
		Owner:       req.Body.Owner,
		Properties:  derefStringMap(anyMapPtrToStringMap(req.Body.Properties)),
		Tags:        derefStringSlice(req.Body.Tags),
		Status:      stringEnumPtr(req.Body.Status),
	}
	item, err := h.macros.Update(ctx, cp.Name, req.MacroName, domReq)
	if err != nil {
		return nil, err
	}
	return GenUpdateProjectMacroByID200JSONResponse{
		Body:    macroToAPI(*item),
		Headers: GenUpdateProjectMacroByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectMacroByID deletes one project-scoped macro.
func (h *APIHandler) DeleteProjectMacroByID(ctx context.Context, req GenDeleteProjectMacroByIDRequest) (GenDeleteProjectMacroByIDResponse, error) {
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	current, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(current.ProjectName) != strings.TrimSpace(project.Name) {
		return nil, domain.ErrNotFound("macro not found")
	}
	if err := h.macros.Delete(ctx, cp.Name, req.MacroName); err != nil {
		return nil, err
	}
	return GenDeleteProjectMacroByID204Response{
		Headers: GenDeleteProjectMacroByID204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectMacroRevisionsByID lists revisions for a project macro.
func (h *APIHandler) ListProjectMacroRevisionsByID(ctx context.Context, req GenListProjectMacroRevisionsByIDRequest) (GenListProjectMacroRevisionsByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	current, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(current.ProjectName) != strings.TrimSpace(project.Name) {
		return nil, domain.ErrNotFound("macro not found")
	}
	items, err := h.macros.ListRevisions(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}
	data := make([]MacroRevision, len(items))
	for i := range items {
		data[i] = macroRevisionToAPI(items[i])
	}
	return GenListProjectMacroRevisionsByID200JSONResponse{
		Body:    MacroRevisionList{Data: data},
		Headers: GenListProjectMacroRevisionsByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DiffProjectMacroRevisionsByID diffs two revisions of a project macro.
func (h *APIHandler) DiffProjectMacroRevisionsByID(ctx context.Context, req GenDiffProjectMacroRevisionsByIDRequest) (GenDiffProjectMacroRevisionsByIDResponse, error) {
	_, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	current, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(current.ProjectName) != strings.TrimSpace(project.Name) {
		return nil, domain.ErrNotFound("macro not found")
	}
	fromVersion, toVersion := int(req.Params.FromVersion), int(req.Params.ToVersion)
	diff, err := h.macros.DiffRevisions(ctx, req.MacroName, fromVersion, toVersion)
	if err != nil {
		return nil, err
	}
	return GenDiffProjectMacroRevisionsByID200JSONResponse{
		Body:    macroRevisionDiffToAPI(*diff),
		Headers: GenDiffProjectMacroRevisionsByID200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CheckProjectEnvironmentModelFreshness returns freshness for a model in an environment.
func (h *APIHandler) CheckProjectEnvironmentModelFreshness(ctx context.Context, req GenCheckProjectEnvironmentModelFreshnessRequest) (GenCheckProjectEnvironmentModelFreshnessResponse, error) {
	_, project, _, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	item, err := h.models.CheckFreshness(ctx, project.Name, req.ModelName)
	if err != nil {
		return nil, err
	}
	return GenCheckProjectEnvironmentModelFreshness200JSONResponse{
		Body:    freshnessStatusToAPI(*item),
		Headers: GenCheckProjectEnvironmentModelFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CheckProjectEnvironmentSourceFreshness returns freshness for a source in an environment.
func (h *APIHandler) CheckProjectEnvironmentSourceFreshness(ctx context.Context, req GenCheckProjectEnvironmentSourceFreshnessRequest) (GenCheckProjectEnvironmentSourceFreshnessResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	source, err := h.projectsCtl.GetSourceForProject(ctx, cp.Name, cp.IsAdmin, project.ID, req.SourceName, req.TableName)
	if err != nil {
		return nil, err
	}
	relationRef := source.RelationRef
	if override, ok := canonicalSourceOverride(environment.SourceOverrides, req.SourceName, req.TableName); ok {
		relationRef = override
	}
	_, schema, table := canonicalParseRelationRef(relationRef, environment.TargetCatalog, environment.TargetSchema)
	timestampColumn := derefString(req.Params.TimestampColumn)
	maxLagSeconds := int64(0)
	if req.Params.MaxLagSeconds != nil {
		maxLagSeconds = *req.Params.MaxLagSeconds
	}
	if source.Freshness != nil {
		if timestampColumn == "" {
			timestampColumn = source.Freshness.TimestampColumn
		}
		if maxLagSeconds == 0 {
			maxLagSeconds = source.Freshness.MaxLagSeconds
		}
	}
	item, err := h.models.CheckSourceFreshness(ctx, cp.Name, schema, table, timestampColumn, maxLagSeconds)
	if err != nil {
		return nil, err
	}
	return GenCheckProjectEnvironmentSourceFreshness200JSONResponse{
		Body:    sourceFreshnessStatusToAPI(*item),
		Headers: GenCheckProjectEnvironmentSourceFreshness200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectEnvironmentCompilations lists compilations for a project environment.
func (h *APIHandler) ListProjectEnvironmentCompilations(ctx context.Context, req GenListProjectEnvironmentCompilationsRequest) (GenListProjectEnvironmentCompilationsResponse, error) {
	_, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.models.ListCompilationsForEnvironment(ctx, project.Name, environment.Name, page)
	if err != nil {
		return nil, err
	}
	data := make([]Compilation, len(items))
	for i := range items {
		data[i] = compilationToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListProjectEnvironmentCompilations200JSONResponse{
		Body:    PaginatedCompilations{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListProjectEnvironmentCompilations200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectEnvironmentCompilation creates a compilation for a project environment.
func (h *APIHandler) CreateProjectEnvironmentCompilation(ctx context.Context, req GenCreateProjectEnvironmentCompilationRequest) (GenCreateProjectEnvironmentCompilationResponse, error) {
	if req.Body == nil {
		return CreateProjectEnvironmentCompilation400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	item, err := h.models.CreateCompilation(ctx, cp.Name, project.Name, environment.Name, domain.CreateCompilationRequest{
		GitRef:        req.Body.GitRef,
		CommitSHA:     req.Body.CommitSha,
		Selector:      derefString(req.Body.Selector),
		TargetCatalog: derefString(req.Body.TargetCatalog),
		TargetSchema:  derefString(req.Body.TargetSchema),
	})
	if err != nil {
		return nil, err
	}
	return GenCreateProjectEnvironmentCompilation201JSONResponse{
		Body:    compilationToAPI(*item),
		Headers: GenCreateProjectEnvironmentCompilation201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentCompilation returns one compilation for a project environment.
func (h *APIHandler) GetProjectEnvironmentCompilation(ctx context.Context, req GenGetProjectEnvironmentCompilationRequest) (GenGetProjectEnvironmentCompilationResponse, error) {
	_, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	item, err := h.models.GetCompilation(ctx, req.CompilationId)
	if err != nil {
		return nil, err
	}
	if item.ProjectID != req.ProjectId || item.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("compilation not found")
	}
	return GenGetProjectEnvironmentCompilation200JSONResponse{
		Body:    compilationToAPI(*item),
		Headers: GenGetProjectEnvironmentCompilation200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentCompilationDiagnostics returns diagnostics for a compilation.
func (h *APIHandler) GetProjectEnvironmentCompilationDiagnostics(ctx context.Context, req GenGetProjectEnvironmentCompilationDiagnosticsRequest) (GenGetProjectEnvironmentCompilationDiagnosticsResponse, error) {
	_, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	compilation, err := h.models.GetCompilation(ctx, req.CompilationId)
	if err != nil {
		return nil, err
	}
	if compilation.ProjectID != req.ProjectId || compilation.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("compilation not found")
	}
	filter := domain.BuildDiagnosticsFilter{ModelName: req.Params.ModelName, Code: req.Params.Code}
	if req.Params.Severity != nil {
		severity := domain.DiagnosticSeverity(*req.Params.Severity)
		filter.Severity = &severity
	}
	items, err := h.models.GetCompilationDiagnostics(ctx, compilation.ID, filter)
	if err != nil {
		return nil, err
	}
	data := make([]CompileDiagnostic, 0, len(items))
	for _, item := range items {
		data = append(data, compileDiagnosticToAPI(item))
	}
	return GenGetProjectEnvironmentCompilationDiagnostics200JSONResponse{
		Body:    PaginatedCompileDiagnostics{Data: data},
		Headers: GenGetProjectEnvironmentCompilationDiagnostics200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentCompilationColumnLineage returns column lineage for a compilation.
func (h *APIHandler) GetProjectEnvironmentCompilationColumnLineage(ctx context.Context, req GenGetProjectEnvironmentCompilationColumnLineageRequest) (GenGetProjectEnvironmentCompilationColumnLineageResponse, error) {
	_, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	compilation, err := h.models.GetCompilation(ctx, req.CompilationId)
	if err != nil {
		return nil, err
	}
	if compilation.ProjectID != req.ProjectId || compilation.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("compilation not found")
	}
	items, err := h.models.GetCompilationLineage(ctx, compilation.ID, req.Params.ModelName)
	if err != nil {
		return nil, err
	}
	data := make([]CompiledColumnLineage, 0, len(items))
	for _, item := range items {
		data = append(data, compiledColumnLineageToAPI(item))
	}
	return GenGetProjectEnvironmentCompilationColumnLineage200JSONResponse{
		Body:    PaginatedCompiledColumnLineage{Data: data},
		Headers: GenGetProjectEnvironmentCompilationColumnLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentCompilationModelImpact returns model impact for a compilation.
func (h *APIHandler) GetProjectEnvironmentCompilationModelImpact(ctx context.Context, req GenGetProjectEnvironmentCompilationModelImpactRequest) (GenGetProjectEnvironmentCompilationModelImpactResponse, error) {
	_, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	compilation, err := h.models.GetCompilation(ctx, req.CompilationId)
	if err != nil {
		return nil, err
	}
	if compilation.ProjectID != req.ProjectId || compilation.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("compilation not found")
	}
	result, err := h.models.GetCompilationModelImpact(ctx, compilation.ID, compilation.ProjectName+"."+req.ModelName)
	if err != nil {
		return nil, err
	}
	return GenGetProjectEnvironmentCompilationModelImpact200JSONResponse{
		Body:    buildImpactResultToAPI(*result),
		Headers: GenGetProjectEnvironmentCompilationModelImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentCompilationMacroImpact returns macro impact for a compilation.
func (h *APIHandler) GetProjectEnvironmentCompilationMacroImpact(ctx context.Context, req GenGetProjectEnvironmentCompilationMacroImpactRequest) (GenGetProjectEnvironmentCompilationMacroImpactResponse, error) {
	_, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	compilation, err := h.models.GetCompilation(ctx, req.CompilationId)
	if err != nil {
		return nil, err
	}
	if compilation.ProjectID != req.ProjectId || compilation.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("compilation not found")
	}
	result, err := h.models.GetCompilationMacroImpact(ctx, compilation.ID, req.MacroName)
	if err != nil {
		return nil, err
	}
	return GenGetProjectEnvironmentCompilationMacroImpact200JSONResponse{
		Body:    buildImpactResultToAPI(*result),
		Headers: GenGetProjectEnvironmentCompilationMacroImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentCompilationSourceColumnImpact returns source-column impact for a compilation.
func (h *APIHandler) GetProjectEnvironmentCompilationSourceColumnImpact(ctx context.Context, req GenGetProjectEnvironmentCompilationSourceColumnImpactRequest) (GenGetProjectEnvironmentCompilationSourceColumnImpactResponse, error) {
	_, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	compilation, err := h.models.GetCompilation(ctx, req.CompilationId)
	if err != nil {
		return nil, err
	}
	if compilation.ProjectID != req.ProjectId || compilation.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("compilation not found")
	}
	_, _, schema, table, _, err := h.canonicalSourceRelation(ctx, req.ProjectId, req.EnvironmentId, req.SourceName, req.TableName)
	if err != nil {
		return nil, err
	}
	items, err := h.models.GetCompilationSourceColumnImpact(ctx, compilation.ID, schema, table, req.ColumnName)
	if err != nil {
		return nil, err
	}
	data := make([]CompiledColumnLineage, 0, len(items))
	for _, item := range items {
		data = append(data, compiledColumnLineageToAPI(item))
	}
	return GenGetProjectEnvironmentCompilationSourceColumnImpact200JSONResponse{
		Body:    PaginatedCompiledColumnLineage{Data: data},
		Headers: GenGetProjectEnvironmentCompilationSourceColumnImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectEnvironmentBuilds lists builds for a project environment.
func (h *APIHandler) ListProjectEnvironmentBuilds(ctx context.Context, req GenListProjectEnvironmentBuildsRequest) (GenListProjectEnvironmentBuildsResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListBuildsForEnvironment(ctx, cp.Name, cp.IsAdmin, project.ID, environment.ID, page)
	if err != nil {
		return nil, err
	}
	data := make([]Build, len(items))
	for i := range items {
		data[i] = buildToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListProjectEnvironmentBuilds200JSONResponse{
		Body:    PaginatedBuilds{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListProjectEnvironmentBuilds200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectEnvironmentBuild creates a build for a project environment.
func (h *APIHandler) CreateProjectEnvironmentBuild(ctx context.Context, req GenCreateProjectEnvironmentBuildRequest) (GenCreateProjectEnvironmentBuildResponse, error) {
	if req.Body == nil {
		return CreateProjectEnvironmentBuild400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	item, err := h.models.CreateEnvironmentBuild(ctx, cp.Name, project.Name, environment.Name, domain.CreateCompilationRequest{
		GitRef:        req.Body.GitRef,
		CommitSHA:     req.Body.CommitSha,
		Selector:      derefString(req.Body.Selector),
		TargetCatalog: derefString(req.Body.TargetCatalog),
		TargetSchema:  derefString(req.Body.TargetSchema),
	})
	if err != nil {
		return nil, err
	}
	return GenCreateProjectEnvironmentBuild201JSONResponse{
		Body:    buildToAPI(*item),
		Headers: GenCreateProjectEnvironmentBuild201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentBuild returns one build for a project environment.
func (h *APIHandler) GetProjectEnvironmentBuild(ctx context.Context, req GenGetProjectEnvironmentBuildRequest) (GenGetProjectEnvironmentBuildResponse, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	return GenGetProjectEnvironmentBuild200JSONResponse{
		Body:    buildToAPI(*build),
		Headers: GenGetProjectEnvironmentBuild200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentBuildDiagnostics returns diagnostics for a build.
func (h *APIHandler) GetProjectEnvironmentBuildDiagnostics(ctx context.Context, req GenGetProjectEnvironmentBuildDiagnosticsRequest) (GenGetProjectEnvironmentBuildDiagnosticsResponse, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	filter := domain.BuildDiagnosticsFilter{ModelName: req.Params.ModelName, Code: req.Params.Code}
	if req.Params.Severity != nil {
		severity := domain.DiagnosticSeverity(*req.Params.Severity)
		filter.Severity = &severity
	}
	items, err := h.models.GetBuildDiagnostics(ctx, build.ID, filter)
	if err != nil {
		return nil, err
	}
	data := make([]CompileDiagnostic, 0, len(items))
	for _, item := range items {
		data = append(data, compileDiagnosticToAPI(item))
	}
	return GenGetProjectEnvironmentBuildDiagnostics200JSONResponse{
		Body:    PaginatedCompileDiagnostics{Data: data},
		Headers: GenGetProjectEnvironmentBuildDiagnostics200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentBuildColumnLineage returns column lineage for a build.
func (h *APIHandler) GetProjectEnvironmentBuildColumnLineage(ctx context.Context, req GenGetProjectEnvironmentBuildColumnLineageRequest) (GenGetProjectEnvironmentBuildColumnLineageResponse, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	items, err := h.models.GetBuildLineage(ctx, build.ID, req.Params.ModelName)
	if err != nil {
		return nil, err
	}
	data := make([]CompiledColumnLineage, 0, len(items))
	for _, item := range items {
		data = append(data, compiledColumnLineageToAPI(item))
	}
	return GenGetProjectEnvironmentBuildColumnLineage200JSONResponse{
		Body:    PaginatedCompiledColumnLineage{Data: data},
		Headers: GenGetProjectEnvironmentBuildColumnLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentBuildModelImpact returns model impact for a build.
func (h *APIHandler) GetProjectEnvironmentBuildModelImpact(ctx context.Context, req GenGetProjectEnvironmentBuildModelImpactRequest) (GenGetProjectEnvironmentBuildModelImpactResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	buildID := build.ID
	result, err := h.models.GetModelImpact(ctx, project.Name, &buildID, project.Name+"."+req.ModelName)
	if err != nil {
		return nil, err
	}
	return GenGetProjectEnvironmentBuildModelImpact200JSONResponse{
		Body:    buildImpactResultToAPI(*result),
		Headers: GenGetProjectEnvironmentBuildModelImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentBuildMacroImpact returns macro impact for a build.
func (h *APIHandler) GetProjectEnvironmentBuildMacroImpact(ctx context.Context, req GenGetProjectEnvironmentBuildMacroImpactRequest) (GenGetProjectEnvironmentBuildMacroImpactResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	buildID := build.ID
	result, err := h.models.GetMacroImpact(ctx, project.Name, &buildID, req.MacroName)
	if err != nil {
		return nil, err
	}
	return GenGetProjectEnvironmentBuildMacroImpact200JSONResponse{
		Body:    buildImpactResultToAPI(*result),
		Headers: GenGetProjectEnvironmentBuildMacroImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentBuildSourceColumnImpact returns source-column impact for a build.
func (h *APIHandler) GetProjectEnvironmentBuildSourceColumnImpact(ctx context.Context, req GenGetProjectEnvironmentBuildSourceColumnImpactRequest) (GenGetProjectEnvironmentBuildSourceColumnImpactResponse, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	_, _, schema, table, _, err := h.canonicalSourceRelation(ctx, req.ProjectId, req.EnvironmentId, req.SourceName, req.TableName)
	if err != nil {
		return nil, err
	}
	items, err := h.models.GetBuildSourceColumnImpact(ctx, build.ID, schema, table, req.ColumnName)
	if err != nil {
		return nil, err
	}
	data := make([]CompiledColumnLineage, 0, len(items))
	for _, item := range items {
		data = append(data, compiledColumnLineageToAPI(item))
	}
	return GenGetProjectEnvironmentBuildSourceColumnImpact200JSONResponse{
		Body:    PaginatedCompiledColumnLineage{Data: data},
		Headers: GenGetProjectEnvironmentBuildSourceColumnImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectEnvironmentBuildRuns lists runs attached to a build.
func (h *APIHandler) ListProjectEnvironmentBuildRuns(ctx context.Context, req GenListProjectEnvironmentBuildRunsRequest) (GenListProjectEnvironmentBuildRunsResponse, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.models.ListRunsForBuild(ctx, build.ID, page)
	if err != nil {
		return nil, err
	}
	data := make([]ModelRun, len(items))
	for i := range items {
		data[i] = modelRunToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListProjectEnvironmentBuildRuns200JSONResponse{
		Body:    PaginatedModelRuns{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListProjectEnvironmentBuildRuns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectEnvironmentBuildRun creates a run attached to a build.
func (h *APIHandler) CreateProjectEnvironmentBuildRun(ctx context.Context, req GenCreateProjectEnvironmentBuildRunRequest) (GenCreateProjectEnvironmentBuildRunResponse, error) {
	cp, _, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	build, err := h.projectsCtl.GetBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.BuildId)
	if err != nil {
		return nil, err
	}
	if build.EnvironmentID != environment.ID {
		return nil, domain.ErrNotFound("build not found")
	}
	run, err := h.models.CreateRunForBuild(ctx, cp.Name, build)
	if err != nil {
		return nil, err
	}
	return GenCreateProjectEnvironmentBuildRun201JSONResponse{
		Body:    modelRunToAPI(*run),
		Headers: GenCreateProjectEnvironmentBuildRun201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectEnvironmentRun returns one environment-scoped run.
func (h *APIHandler) GetProjectEnvironmentRun(ctx context.Context, req GenGetProjectEnvironmentRunRequest) (GenGetProjectEnvironmentRunResponse, error) {
	_, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	run, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, err
	}
	if err := ensureRunScope(run, project.Name, environment.Name); err != nil {
		return nil, err
	}
	return GenGetProjectEnvironmentRun200JSONResponse{
		Body:    modelRunToAPI(*run),
		Headers: GenGetProjectEnvironmentRun200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelProjectEnvironmentRun cancels one environment-scoped run.
func (h *APIHandler) CancelProjectEnvironmentRun(ctx context.Context, req GenCancelProjectEnvironmentRunRequest) (GenCancelProjectEnvironmentRunResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	run, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, err
	}
	if err := ensureRunScope(run, project.Name, environment.Name); err != nil {
		return nil, err
	}
	if err := h.models.CancelRun(ctx, cp.Name, run.ID); err != nil {
		return nil, err
	}
	return GenCancelProjectEnvironmentRun204Response{
		Headers: GenCancelProjectEnvironmentRun204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectEnvironmentRunSteps lists steps for an environment-scoped run.
func (h *APIHandler) ListProjectEnvironmentRunSteps(ctx context.Context, req GenListProjectEnvironmentRunStepsRequest) (GenListProjectEnvironmentRunStepsResponse, error) {
	_, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	run, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, err
	}
	if err := ensureRunScope(run, project.Name, environment.Name); err != nil {
		return nil, err
	}
	items, err := h.models.ListRunSteps(ctx, run.ID)
	if err != nil {
		return nil, err
	}
	data := make([]ModelRunStep, len(items))
	for i := range items {
		data[i] = modelRunStepToAPI(items[i])
	}
	return GenListProjectEnvironmentRunSteps200JSONResponse{
		Body:    ModelRunStepList{Data: data},
		Headers: GenListProjectEnvironmentRunSteps200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectEnvironmentRunStepTestResults lists test results for a run step.
func (h *APIHandler) ListProjectEnvironmentRunStepTestResults(ctx context.Context, req GenListProjectEnvironmentRunStepTestResultsRequest) (GenListProjectEnvironmentRunStepTestResultsResponse, error) {
	_, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	run, err := h.models.GetRun(ctx, req.RunId)
	if err != nil {
		return nil, err
	}
	if err := ensureRunScope(run, project.Name, environment.Name); err != nil {
		return nil, err
	}
	items, err := h.models.ListTestResults(ctx, run.ID, req.StepId)
	if err != nil {
		return nil, err
	}
	data := make([]ModelTestResult, len(items))
	for i := range items {
		data[i] = modelTestResultToAPI(items[i])
	}
	return GenListProjectEnvironmentRunStepTestResults200JSONResponse{
		Body:    ModelTestResultList{Data: data},
		Headers: GenListProjectEnvironmentRunStepTestResults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectEnvironmentRebuildPlan creates a rebuild plan for a project environment.
func (h *APIHandler) CreateProjectEnvironmentRebuildPlan(ctx context.Context, req GenCreateProjectEnvironmentRebuildPlanRequest) (GenCreateProjectEnvironmentRebuildPlanResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	if req.Body == nil {
		return CreateProjectEnvironmentRebuildPlan400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.models.PlanRebuild(ctx, cp.Name, domain.PlanRebuildRequest{
		ProjectName:     project.Name,
		EnvironmentName: environment.Name,
		Selector:        derefString(req.Body.Selector),
	})
	if err != nil {
		return nil, err
	}
	return GenCreateProjectEnvironmentRebuildPlan200JSONResponse{
		Body:    rebuildPlanToAPI(*item),
		Headers: GenCreateProjectEnvironmentRebuildPlan200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectEnvironmentBuildComparison compares builds for a project environment.
func (h *APIHandler) CreateProjectEnvironmentBuildComparison(ctx context.Context, req GenCreateProjectEnvironmentBuildComparisonRequest) (GenCreateProjectEnvironmentBuildComparisonResponse, error) {
	cp, project, environment, err := h.canonicalEnvironment(ctx, req.ProjectId, req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	_ = environment
	if req.Body == nil {
		return CreateProjectEnvironmentBuildComparison400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	item, err := h.models.CompareBuilds(ctx, cp.Name, domain.CompareBuildsRequest{
		ProjectName:   project.Name,
		FromBuildID:   req.Body.FromBuildId,
		ToBuildID:     req.Body.ToBuildId,
		CompareToHead: derefBoolDefault(req.Body.CompareToHead, false),
	})
	if err != nil {
		return nil, err
	}
	return GenCreateProjectEnvironmentBuildComparison200JSONResponse{
		Body:    buildCompareResultToAPI(*item),
		Headers: GenCreateProjectEnvironmentBuildComparison200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectReleases lists releases for a project.
func (h *APIHandler) ListProjectReleases(ctx context.Context, req GenListProjectReleasesRequest) (GenListProjectReleasesResponse, error) {
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListReleasesForProject(ctx, cp.Name, cp.IsAdmin, project.ID, page)
	if err != nil {
		return nil, err
	}
	data := make([]ProjectRelease, len(items))
	for i := range items {
		data[i] = projectReleaseToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListProjectReleases200JSONResponse{
		Body:    PaginatedProjectReleases{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListProjectReleases200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectRelease creates a release for a project.
func (h *APIHandler) CreateProjectRelease(ctx context.Context, req GenCreateProjectReleaseRequest) (GenCreateProjectReleaseResponse, error) {
	if req.Body == nil {
		return CreateProjectRelease400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	item, err := h.projectsCtl.CreateReleaseForProject(ctx, cp.Name, cp.IsAdmin, project.ID, domain.CreateProjectReleaseRequest{
		Version:         req.Body.Version,
		ResolvedBuildID: req.Body.ResolvedBuildId,
		CompilationID:   req.Body.CompilationId,
	})
	if err != nil {
		return nil, err
	}
	return GenCreateProjectRelease201JSONResponse{
		Body:    projectReleaseToAPI(*item),
		Headers: GenCreateProjectRelease201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectRelease returns one project release.
func (h *APIHandler) GetProjectRelease(ctx context.Context, req GenGetProjectReleaseRequest) (GenGetProjectReleaseResponse, error) {
	cp, project, err := h.canonicalProject(ctx, req.ProjectId)
	if err != nil {
		return nil, err
	}
	item, err := h.projectsCtl.GetReleaseForProject(ctx, cp.Name, cp.IsAdmin, project.ID, req.ReleaseId)
	if err != nil {
		return nil, err
	}
	return GenGetProjectRelease200JSONResponse{
		Body:    projectReleaseToAPI(*item),
		Headers: GenGetProjectRelease200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
