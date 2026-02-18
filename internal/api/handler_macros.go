package api

import (
	"context"
	"errors"
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
func (h *APIHandler) ListMacros(ctx context.Context, req ListMacrosRequestObject) (ListMacrosResponseObject, error) {
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
	return ListMacros200JSONResponse{
		Body:    PaginatedMacros{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListMacros200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateMacro implements the endpoint for creating a new SQL macro.
func (h *APIHandler) CreateMacro(ctx context.Context, req CreateMacroRequestObject) (CreateMacroResponseObject, error) {
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
		domReq.Properties = map[string]string(*req.Body.Properties)
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateMacro403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateMacro400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateMacro409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateMacro400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateMacro201JSONResponse{
		Body:    macroToAPI(*result),
		Headers: CreateMacro201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListMacroRevisions implements the endpoint for listing macro revisions.
func (h *APIHandler) ListMacroRevisions(ctx context.Context, req ListMacroRevisionsRequestObject) (ListMacroRevisionsResponseObject, error) {
	revisions, err := h.macros.ListRevisions(ctx, req.MacroName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListMacroRevisions404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]MacroRevision, 0, len(revisions))
	for _, r := range revisions {
		out = append(out, macroRevisionToAPI(r))
	}
	return ListMacroRevisions200JSONResponse{
		Body:    MacroRevisionList{Data: &out},
		Headers: ListMacroRevisions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DiffMacroRevisions implements the endpoint for comparing two macro revisions.
func (h *APIHandler) DiffMacroRevisions(ctx context.Context, req DiffMacroRevisionsRequestObject) (DiffMacroRevisionsResponseObject, error) {
	diff, err := h.macros.DiffRevisions(ctx, req.MacroName, int(req.Params.FromVersion), int(req.Params.ToVersion))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DiffMacroRevisions404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return DiffMacroRevisions400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	fromRev, err := h.macros.GetRevisionByVersion(ctx, req.MacroName, int(req.Params.FromVersion))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DiffMacroRevisions404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return DiffMacroRevisions400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	toRev, err := h.macros.GetRevisionByVersion(ctx, req.MacroName, int(req.Params.ToVersion))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DiffMacroRevisions404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return DiffMacroRevisions400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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

	return DiffMacroRevisions200JSONResponse{
		Body:    apiDiff,
		Headers: DiffMacroRevisions200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMacroImpact implements the endpoint for retrieving reverse macro impact.
func (h *APIHandler) GetMacroImpact(ctx context.Context, req GetMacroImpactRequestObject) (GetMacroImpactResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)

	if _, err := h.macros.Get(ctx, req.MacroName); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetMacroImpact404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
	return GetMacroImpact200JSONResponse{
		Body:    MacroImpactList{Data: &data, NextPageToken: optStr(npt)},
		Headers: GetMacroImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMacro implements the endpoint for retrieving a macro by name.
func (h *APIHandler) GetMacro(ctx context.Context, req GetMacroRequestObject) (GetMacroResponseObject, error) {
	result, err := h.macros.Get(ctx, req.MacroName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetMacro404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetMacro200JSONResponse{
		Body:    macroToAPI(*result),
		Headers: GetMacro200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateMacro implements the endpoint for updating a SQL macro.
func (h *APIHandler) UpdateMacro(ctx context.Context, req UpdateMacroRequestObject) (UpdateMacroResponseObject, error) {
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
		domReq.Properties = map[string]string(*req.Body.Properties)
	}
	if req.Body.Tags != nil {
		domReq.Tags = *req.Body.Tags
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.macros.Update(ctx, principal, req.MacroName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateMacro403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateMacro404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateMacro400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateMacro200JSONResponse{
		Body:    macroToAPI(*result),
		Headers: UpdateMacro200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteMacro implements the endpoint for deleting a SQL macro.
func (h *APIHandler) DeleteMacro(ctx context.Context, req DeleteMacroRequestObject) (DeleteMacroResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.macros.Delete(ctx, principal, req.MacroName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteMacro403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteMacro404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteMacro204Response{
		Headers: DeleteMacro204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Macro Mappers ===

func macroToAPI(m domain.Macro) Macro {
	ct := m.CreatedAt
	ut := m.UpdatedAt
	mt := MacroMacroType(m.MacroType)
	resp := Macro{
		Id:          &m.ID,
		Name:        &m.Name,
		MacroType:   &mt,
		Body:        &m.Body,
		Description: &m.Description,
		CreatedBy:   &m.CreatedBy,
		CreatedAt:   &ct,
		UpdatedAt:   &ut,
	}
	if m.CatalogName != "" {
		resp.CatalogName = &m.CatalogName
	}
	if m.ProjectName != "" {
		resp.ProjectName = &m.ProjectName
	}
	if m.Visibility != "" {
		v := MacroVisibility(m.Visibility)
		resp.Visibility = &v
	}
	if m.Owner != "" {
		resp.Owner = &m.Owner
	}
	if len(m.Properties) > 0 {
		props := map[string]string(m.Properties)
		resp.Properties = &props
	}
	if len(m.Tags) > 0 {
		resp.Tags = &m.Tags
	}
	if m.Status != "" {
		s := MacroStatus(m.Status)
		resp.Status = &s
	}
	if len(m.Parameters) > 0 {
		resp.Parameters = &m.Parameters
	}
	return resp
}

func macroRevisionToAPI(r domain.MacroRevision) MacroRevision {
	ct := r.CreatedAt
	version := safeInt32(r.Version)
	resp := MacroRevision{
		Id:          &r.ID,
		MacroName:   &r.MacroName,
		Version:     &version,
		ContentHash: &r.ContentHash,
		Body:        &r.Body,
		Description: &r.Description,
		CreatedBy:   &r.CreatedBy,
		CreatedAt:   &ct,
	}
	if len(r.Parameters) > 0 {
		resp.Parameters = &r.Parameters
	}
	if r.Status != "" {
		s := MacroRevisionStatus(r.Status)
		resp.Status = &s
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
		s := MacroRevisionDiffFromStatus(d.FromStatus)
		resp.FromStatus = &s
	}
	if d.ToStatus != "" {
		s := MacroRevisionDiffToStatus(d.ToStatus)
		resp.ToStatus = &s
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
		lastSeen := m.LastSeenAt
		resp.LastSeenAt = &lastSeen
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
