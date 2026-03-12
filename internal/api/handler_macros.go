package api

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

// macroService defines the macro operations used by the API handler.
type macroService interface {
	Create(ctx context.Context, principal string, req domain.CreateMacroRequest) (*domain.Macro, error)
	Get(ctx context.Context, name string) (*domain.Macro, error)
	List(ctx context.Context, page domain.PageRequest) ([]domain.Macro, int64, error)
	Update(ctx context.Context, principal, name string, req domain.UpdateMacroRequest) (*domain.Macro, error)
	Delete(ctx context.Context, principal, name string) error
	ListRevisions(ctx context.Context, macroName string) ([]domain.MacroRevision, error)
	GetRevisionByVersion(ctx context.Context, macroName string, version int) (*domain.MacroRevision, error)
	DiffRevisions(ctx context.Context, macroName string, fromVersion, toVersion int) (*domain.MacroRevisionDiff, error)
}

// === Macros ===

// ListMacros implements the endpoint for listing SQL macros.
func (h *APIHandler) ListMacros(ctx context.Context, req GenListMacrosRequest) (GenListMacrosResponse, error) {
	if isNilService(h.macros) {
		empty := []Macro{}
		return GenListMacros200JSONResponse{
			Body:    PaginatedMacros{Data: empty, NextPageToken: nil},
			Headers: GenListMacros200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
		}, nil
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	macros, total, err := h.macros.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]Macro, len(macros))
	for i, m := range macros {
		data[i] = macroToAPI(m)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListMacros200JSONResponse{
		Body:    PaginatedMacros{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListMacros200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateMacro implements the endpoint for creating a new SQL macro.
func (h *APIHandler) CreateMacro(ctx context.Context, req GenCreateMacroRequest) (GenCreateMacroResponse, error) {
	domReq := domain.CreateMacroRequest{
		Name: req.Body.Name,
		Body: req.Body.Body,
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
	if req.Body.ProjectName != nil {
		domReq.ProjectName = *req.Body.ProjectName
	}
	if req.Body.Visibility != nil {
		domReq.Visibility = string(*req.Body.Visibility)
	}
	if req.Body.Owner != nil {
		domReq.Owner = *req.Body.Owner
	}
	if req.Body.Properties != nil {
		domReq.Properties = recordToStringMap(req.Body.Properties)
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}
	if req.Body.Status != nil {
		domReq.Status = string(*req.Body.Status)
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.macros.Create(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateMacroResponse](err, domainErrorResponder[GenCreateMacroResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateMacroResponse { return CreateMacro400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateMacroResponse { return CreateMacro403JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateMacroResponse { return CreateMacro409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreateMacro400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateMacro201JSONResponse{
		Body:    macroToAPI(*result),
		Headers: GenCreateMacro201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListMacroRevisions implements the endpoint for listing macro revisions.
func (h *APIHandler) ListMacroRevisions(ctx context.Context, req GenListMacroRevisionsRequest) (GenListMacroRevisionsResponse, error) {
	revisions, err := h.macros.ListRevisions(ctx, req.MacroName)
	if err != nil {
		if resp, ok := respondDomainError[GenListMacroRevisionsResponse](err, domainErrorResponder[GenListMacroRevisionsResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenListMacroRevisionsResponse {
				return GenListMacroRevisions404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	out := make([]MacroRevision, 0, len(revisions))
	for _, r := range revisions {
		out = append(out, macroRevisionToAPI(r))
	}
	return GenListMacroRevisions200JSONResponse{
		Body:    MacroRevisionList{Data: out},
		Headers: GenListMacroRevisions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DiffMacroRevisions implements the endpoint for comparing two macro revisions.
func (h *APIHandler) DiffMacroRevisions(ctx context.Context, req GenDiffMacroRevisionsRequest) (GenDiffMacroRevisionsResponse, error) {
	diff, err := h.macros.DiffRevisions(ctx, req.MacroName, int(req.Params.FromVersion), int(req.Params.ToVersion))
	if err != nil {
		if resp, ok := respondDomainError[GenDiffMacroRevisionsResponse](err, domainErrorResponder[GenDiffMacroRevisionsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDiffMacroRevisionsResponse {
				return DiffMacroRevisions400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDiffMacroRevisionsResponse {
				return GenDiffMacroRevisions404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	fromRev, err := h.macros.GetRevisionByVersion(ctx, req.MacroName, int(req.Params.FromVersion))
	if err != nil {
		if resp, ok := respondDomainError[GenDiffMacroRevisionsResponse](err, domainErrorResponder[GenDiffMacroRevisionsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDiffMacroRevisionsResponse {
				return DiffMacroRevisions400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDiffMacroRevisionsResponse {
				return GenDiffMacroRevisions404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	toRev, err := h.macros.GetRevisionByVersion(ctx, req.MacroName, int(req.Params.ToVersion))
	if err != nil {
		if resp, ok := respondDomainError[GenDiffMacroRevisionsResponse](err, domainErrorResponder[GenDiffMacroRevisionsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDiffMacroRevisionsResponse {
				return DiffMacroRevisions400JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDiffMacroRevisionsResponse {
				return GenDiffMacroRevisions404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	fromImpact, err := h.listMacroImpactAsOf(ctx, req.MacroName, &fromRev.CreatedAt)
	if err != nil {
		return nil, err
	}
	toImpact, err := h.listMacroImpactAsOf(ctx, req.MacroName, &toRev.CreatedAt)
	if err != nil {
		return nil, err
	}
	added, removed, unchanged := diffMacroImpactSets(fromImpact, toImpact)
	apiDiff := macroRevisionDiffToAPI(*diff)
	apiDiff.ImpactChanged = macroBoolPtr(len(added) > 0 || len(removed) > 0)
	if len(added) > 0 {
		models := macroImpactModelsToAPI(added)
		apiDiff.ImpactedModelsAdded = &models
	}
	if len(removed) > 0 {
		models := macroImpactModelsToAPI(removed)
		apiDiff.ImpactedModelsRemoved = &models
	}
	if len(unchanged) > 0 {
		models := macroImpactModelsToAPI(unchanged)
		apiDiff.ImpactedModelsUnchanged = &models
	}

	return GenDiffMacroRevisions200JSONResponse{
		Body:    apiDiff,
		Headers: GenDiffMacroRevisions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMacroImpact implements the endpoint for retrieving reverse macro impact.
func (h *APIHandler) GetMacroImpact(ctx context.Context, req GenGetMacroImpactRequest) (GenGetMacroImpactResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)

	if _, err := h.macros.Get(ctx, req.MacroName); err != nil {
		if resp, ok := respondDomainError[GenGetMacroImpactResponse](err, domainErrorResponder[GenGetMacroImpactResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetMacroImpactResponse {
				return GenGetMacroImpact404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	impacted, err := h.listMacroImpact(ctx, req.MacroName)
	if err != nil {
		return nil, err
	}

	start := page.Offset()
	if start > len(impacted) {
		start = len(impacted)
	}
	end := start + page.Limit()
	if end > len(impacted) {
		end = len(impacted)
	}

	data := make([]MacroImpactModel, 0, end-start)
	for _, impactedModel := range impacted[start:end] {
		data = append(data, macroImpactModelToAPI(impactedModel))
	}

	npt := domain.NextPageToken(start, page.Limit(), int64(len(impacted)))
	return GenGetMacroImpact200JSONResponse{
		Body:    MacroImpactList{Data: data, NextPageToken: optStr(npt)},
		Headers: GenGetMacroImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMacro implements the endpoint for retrieving a macro by name.
func (h *APIHandler) GetMacro(ctx context.Context, req GenGetMacroRequest) (GenGetMacroResponse, error) {
	result, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		if resp, ok := respondDomainError[GenGetMacroResponse](err, domainErrorResponder[GenGetMacroResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetMacroResponse {
				return GenGetMacro404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetMacro200JSONResponse{
		Body:    macroToAPI(*result),
		Headers: GenGetMacro200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateMacro implements the endpoint for updating a SQL macro.
func (h *APIHandler) UpdateMacro(ctx context.Context, req GenUpdateMacroRequest) (GenUpdateMacroResponse, error) {
	domReq := domain.UpdateMacroRequest{
		Body:        req.Body.Body,
		Description: req.Body.Description,
	}
	if req.Body.Parameters != nil {
		domReq.Parameters = *req.Body.Parameters
	}
	if req.Body.Status != nil {
		s := string(*req.Body.Status)
		domReq.Status = &s
	}
	if req.Body.CatalogName != nil {
		domReq.CatalogName = req.Body.CatalogName
	}
	if req.Body.ProjectName != nil {
		domReq.ProjectName = req.Body.ProjectName
	}
	if req.Body.Visibility != nil {
		v := string(*req.Body.Visibility)
		domReq.Visibility = &v
	}
	if req.Body.Owner != nil {
		domReq.Owner = req.Body.Owner
	}
	if req.Body.Properties != nil {
		domReq.Properties = recordToStringMap(req.Body.Properties)
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.macros.Update(ctx, principal, req.MacroName, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateMacroResponse](err, domainErrorResponder[GenUpdateMacroResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateMacroResponse { return UpdateMacro400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenUpdateMacroResponse { return UpdateMacro403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenUpdateMacroResponse { return UpdateMacro404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateMacro200JSONResponse{
		Body:    macroToAPI(*result),
		Headers: GenUpdateMacro200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteMacro implements the endpoint for deleting a SQL macro.
func (h *APIHandler) DeleteMacro(ctx context.Context, req GenDeleteMacroRequest) (GenDeleteMacroResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.macros.Delete(ctx, principal, req.MacroName); err != nil {
		if resp, ok := respondDomainError[GenDeleteMacroResponse](err, domainErrorResponder[GenDeleteMacroResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteMacroResponse { return DeleteMacro403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteMacroResponse { return DeleteMacro404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteMacro204Response{
		Headers: GenDeleteMacro204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Macro Mappers ===

func macroToAPI(m domain.Macro) Macro {
	resp := Macro{
		Id:          &m.ID,
		Name:        &m.Name,
		MacroType:   ptrMacroType(m.MacroType),
		Body:        &m.Body,
		Description: &m.Description,
		CreatedBy:   &m.CreatedBy,
		CreatedAt:   formatTimePtr(&m.CreatedAt),
		UpdatedAt:   formatTimePtr(&m.UpdatedAt),
	}
	if m.CatalogName != "" {
		resp.CatalogName = &m.CatalogName
	}
	if m.ProjectName != "" {
		resp.ProjectName = &m.ProjectName
	}
	if m.Visibility != "" {
		resp.Visibility = ptrMacroVisibility(m.Visibility)
	}
	if m.Owner != "" {
		resp.Owner = &m.Owner
	}
	if len(m.Properties) > 0 {
		resp.Properties = stringMapToRecord(m.Properties)
	}
	if len(m.Tags) > 0 {
		resp.Tags = &m.Tags
	}
	if m.Status != "" {
		resp.Status = ptrMacroStatus(m.Status)
	}
	if len(m.Parameters) > 0 {
		resp.Parameters = &m.Parameters
	}
	return resp
}

func macroRevisionToAPI(r domain.MacroRevision) MacroRevision {
	version := safeInt32(r.Version)
	resp := MacroRevision{
		Id:          &r.ID,
		MacroName:   &r.MacroName,
		Version:     &version,
		ContentHash: &r.ContentHash,
		Body:        &r.Body,
		Description: &r.Description,
		CreatedBy:   &r.CreatedBy,
		CreatedAt:   formatTimePtr(&r.CreatedAt),
	}
	if len(r.Parameters) > 0 {
		resp.Parameters = &r.Parameters
	}
	if r.Status != "" {
		resp.Status = ptrMacroStatus(r.Status)
	}
	return resp
}

func macroRevisionDiffToAPI(d domain.MacroRevisionDiff) MacroRevisionDiff {
	fromVersion := safeInt32(d.FromVersion)
	toVersion := safeInt32(d.ToVersion)
	resp := MacroRevisionDiff{
		MacroName:          &d.MacroName,
		FromVersion:        &fromVersion,
		ToVersion:          &toVersion,
		FromContentHash:    &d.FromContentHash,
		ToContentHash:      &d.ToContentHash,
		Changed:            &d.Changed,
		ParametersChanged:  &d.ParametersChanged,
		BodyChanged:        &d.BodyChanged,
		DescriptionChanged: &d.DescriptionChanged,
		StatusChanged:      &d.StatusChanged,
		FromBody:           &d.FromBody,
		ToBody:             &d.ToBody,
		FromDescription:    &d.FromDescription,
		ToDescription:      &d.ToDescription,
	}
	if len(d.FromParameters) > 0 {
		resp.FromParameters = &d.FromParameters
	}
	if len(d.ToParameters) > 0 {
		resp.ToParameters = &d.ToParameters
	}
	if d.FromStatus != "" {
		resp.FromStatus = ptrMacroStatus(d.FromStatus)
	}
	if d.ToStatus != "" {
		resp.ToStatus = ptrMacroStatus(d.ToStatus)
	}
	return resp
}

func safeInt32(v int) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v)
}

type macroImpactModel struct {
	TargetTable  string
	TargetSchema string
	ModelName    string
	LastSeenAt   time.Time
}

func (h *APIHandler) listMacroImpact(ctx context.Context, macroName string) ([]macroImpactModel, error) {
	return h.listMacroImpactAsOf(ctx, macroName, nil)
}

func (h *APIHandler) listMacroImpactAsOf(ctx context.Context, macroName string, asOf *time.Time) ([]macroImpactModel, error) {
	recordByTable := make(map[string]macroImpactModel)
	tableName := "macro." + macroName
	const batchSize = domain.MaxMaxResults

	offset := 0
	for {
		edges, total, err := h.lineage.GetDownstream(ctx, tableName, domain.PageRequest{
			MaxResults: batchSize,
			PageToken:  domain.EncodePageToken(offset),
		})
		if err != nil {
			return nil, err
		}

		for _, edge := range edges {
			if asOf != nil && edge.CreatedAt.After(*asOf) {
				continue
			}
			if edge.TargetTable == nil {
				continue
			}
			targetTable := strings.TrimSpace(*edge.TargetTable)
			if targetTable == "" {
				continue
			}
			targetSchema, modelName := parseLineageTargetTable(targetTable, edge.TargetSchema)
			qualifiedModelName := modelName
			if targetSchema != "" {
				qualifiedModelName = targetSchema + "." + modelName
			}
			current, exists := recordByTable[targetTable]
			if !exists || edge.CreatedAt.After(current.LastSeenAt) {
				recordByTable[targetTable] = macroImpactModel{
					TargetTable:  targetTable,
					TargetSchema: targetSchema,
					ModelName:    qualifiedModelName,
					LastSeenAt:   edge.CreatedAt,
				}
			}
		}

		offset += len(edges)
		if offset >= int(total) || len(edges) == 0 {
			break
		}
	}

	out := make([]macroImpactModel, 0, len(recordByTable))
	for _, item := range recordByTable {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TargetTable < out[j].TargetTable
	})

	return out, nil
}

func diffMacroImpactSets(fromImpact []macroImpactModel, toImpact []macroImpactModel) ([]macroImpactModel, []macroImpactModel, []macroImpactModel) {
	fromByTable := make(map[string]macroImpactModel, len(fromImpact))
	toByTable := make(map[string]macroImpactModel, len(toImpact))
	for _, m := range fromImpact {
		fromByTable[m.TargetTable] = m
	}
	for _, m := range toImpact {
		toByTable[m.TargetTable] = m
	}

	added := make([]macroImpactModel, 0)
	removed := make([]macroImpactModel, 0)
	unchanged := make([]macroImpactModel, 0)

	for table, model := range toByTable {
		if _, ok := fromByTable[table]; ok {
			unchanged = append(unchanged, model)
			continue
		}
		added = append(added, model)
	}
	for table, model := range fromByTable {
		if _, ok := toByTable[table]; ok {
			continue
		}
		removed = append(removed, model)
	}

	sort.Slice(added, func(i, j int) bool { return added[i].TargetTable < added[j].TargetTable })
	sort.Slice(removed, func(i, j int) bool { return removed[i].TargetTable < removed[j].TargetTable })
	sort.Slice(unchanged, func(i, j int) bool { return unchanged[i].TargetTable < unchanged[j].TargetTable })

	return added, removed, unchanged
}

func macroImpactModelsToAPI(in []macroImpactModel) []MacroImpactModel {
	out := make([]MacroImpactModel, 0, len(in))
	for _, m := range in {
		out = append(out, macroImpactModelToAPI(m))
	}
	return out
}

func macroBoolPtr(v bool) *bool {
	return &v
}

func macroImpactModelToAPI(m macroImpactModel) MacroImpactModel {
	resp := MacroImpactModel{
		TargetTable: &m.TargetTable,
		ModelName:   &m.ModelName,
	}
	if m.TargetSchema != "" {
		resp.TargetSchema = &m.TargetSchema
	}
	if !m.LastSeenAt.IsZero() {
		resp.LastSeenAt = formatTimePtr(&m.LastSeenAt)
	}
	return resp
}

func parseLineageTargetTable(targetTable, fallbackSchema string) (schema, table string) {
	parts := strings.Split(targetTable, ".")
	if len(parts) == 0 {
		return strings.TrimSpace(fallbackSchema), ""
	}
	if len(parts) == 1 {
		return strings.TrimSpace(fallbackSchema), strings.TrimSpace(parts[0])
	}
	return strings.TrimSpace(parts[len(parts)-2]), strings.TrimSpace(parts[len(parts)-1])
}
