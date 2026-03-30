package api

import (
	"context"
	"errors"
	"fmt"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
)

type dashboardService interface {
	CreateDashboard(ctx context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error)
	ListDashboards(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error)
	GetDashboard(ctx context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error)
	ResolveWidgets(ctx context.Context, principal string, widgets []domain.DashboardWidget) ([]dashboardsvc.ResolvedWidget, error)
	UpdateDashboard(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error)
	DeleteDashboard(ctx context.Context, principal string, isAdmin bool, id string) error
	CreateWidget(ctx context.Context, principal string, isAdmin bool, dashboardID string, req domain.CreateDashboardWidgetRequest) (*domain.DashboardWidget, error)
	UpdateWidget(ctx context.Context, principal string, isAdmin bool, widgetID string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error)
	DeleteWidget(ctx context.Context, principal string, isAdmin bool, widgetID string) error
}

// ListDashboards implements the endpoint for listing dashboards.
func (h *APIHandler) ListDashboards(ctx context.Context, req GenListDashboardsRequest) (GenListDashboardsResponse, error) {
	if isNilService(h.dashboards) {
		empty := []Dashboard{}
		return GenListDashboards200JSONResponse{
			Body:    PaginatedDashboards{Data: empty},
			Headers: GenListDashboards200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
		}, nil
	}

	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.dashboards.ListDashboards(ctx, req.Params.Owner, page)
	if err != nil {
		return nil, err
	}
	data := make([]Dashboard, len(items))
	for i, item := range items {
		data[i] = dashboardToAPI(item)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListDashboards200JSONResponse{
		Body:    PaginatedDashboards{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListDashboards200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateDashboard implements the endpoint for creating dashboards.
func (h *APIHandler) CreateDashboard(ctx context.Context, req GenCreateDashboardRequest) (GenCreateDashboardResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.dashboards.CreateDashboard(ctx, cp.Name, domain.CreateDashboardRequest{
		Name:        req.Body.Name,
		Description: valOrEmpty(req.Body.Description),
		FolderID:    req.Body.FolderId,
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreateDashboard400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateDashboard409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCreateDashboard201JSONResponse{
		Body:    dashboardToAPI(*item),
		Headers: GenCreateDashboard201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetDashboard implements the endpoint for retrieving a dashboard and its widgets.
func (h *APIHandler) GetDashboard(ctx context.Context, req GenGetDashboardRequest) (GenGetDashboardResponse, error) {
	item, widgets, err := h.dashboards.GetDashboard(ctx, req.DashboardId)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return GenGetDashboard404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}
	apiWidgets := make([]DashboardWidget, len(widgets))
	for i, widget := range widgets {
		apiWidgets[i] = dashboardWidgetToAPI(widget)
	}
	body := DashboardDetail{Dashboard: ptrDashboard(dashboardToAPI(*item))}
	if len(apiWidgets) > 0 {
		body.Widgets = &apiWidgets
	}
	return GenGetDashboard200JSONResponse{
		Body:    body,
		Headers: GenGetDashboard200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetResolvedDashboard implements the endpoint for retrieving a dashboard and its resolved widgets.
func (h *APIHandler) GetResolvedDashboard(ctx context.Context, req GenGetResolvedDashboardRequest) (GenGetResolvedDashboardResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	item, widgets, err := h.dashboards.GetDashboard(ctx, req.DashboardId)
	if err != nil {
		if errors.As(err, new(*domain.NotFoundError)) {
			return GenGetResolvedDashboard404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return nil, err
	}
	resolved, err := h.dashboards.ResolveWidgets(ctx, cp.Name, widgets)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GetResolvedDashboard403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetResolvedDashboard404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GetResolvedDashboard400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	apiWidgets := make([]ResolvedDashboardWidget, len(resolved))
	for i, widget := range resolved {
		apiWidgets[i] = resolvedDashboardWidgetToAPI(widget)
	}

	body := ResolvedDashboardDetail{Dashboard: ptrDashboard(dashboardToAPI(*item))}
	if len(apiWidgets) > 0 {
		body.Widgets = &apiWidgets
	}

	return GenGetResolvedDashboard200JSONResponse{
		Body:    body,
		Headers: GenGetResolvedDashboard200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateDashboard implements the endpoint for updating dashboard metadata.
func (h *APIHandler) UpdateDashboard(ctx context.Context, req GenUpdateDashboardRequest) (GenUpdateDashboardResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.dashboards.UpdateDashboard(ctx, cp.Name, cp.IsAdmin, req.DashboardId, domain.UpdateDashboardRequest{
		Name:        req.Body.Name,
		Description: req.Body.Description,
		FolderID:    req.Body.FolderId,
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateDashboard403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateDashboard404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateDashboard400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenUpdateDashboard200JSONResponse{
		Body:    dashboardToAPI(*item),
		Headers: GenUpdateDashboard200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteDashboard implements the endpoint for deleting dashboards.
func (h *APIHandler) DeleteDashboard(ctx context.Context, req GenDeleteDashboardRequest) (GenDeleteDashboardResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.dashboards.DeleteDashboard(ctx, cp.Name, cp.IsAdmin, req.DashboardId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteDashboard403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteDashboard404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteDashboard204Response{Headers: GenDeleteDashboard204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

// CreateDashboardWidget implements the endpoint for adding widgets to dashboards.
func (h *APIHandler) CreateDashboardWidget(ctx context.Context, req GenCreateDashboardWidgetRequest) (GenCreateDashboardWidgetResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.dashboards.CreateWidget(ctx, cp.Name, cp.IsAdmin, req.DashboardId, domain.CreateDashboardWidgetRequest{
		Name:        req.Body.Name,
		Description: valOrEmpty(req.Body.Description),
		Source:      dashboardWidgetSourceFromAPI(&req.Body.Source),
		VisualSpec:  visualSpecFromAPI(req.Body.VisualSpec),
		Layout:      dashboardWidgetLayoutFromAPI(&req.Body.Layout),
	})
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateDashboardWidget403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateDashboardWidget404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateDashboardWidget400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCreateDashboardWidget201JSONResponse{
		Body:    dashboardWidgetToAPI(*item),
		Headers: GenCreateDashboardWidget201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateDashboardWidget implements the endpoint for updating dashboard widgets.
func (h *APIHandler) UpdateDashboardWidget(ctx context.Context, req GenUpdateDashboardWidgetRequest) (GenUpdateDashboardWidgetResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	domReq := domain.UpdateDashboardWidgetRequest{
		Name:        req.Body.Name,
		Description: req.Body.Description,
		VisualSpec:  visualSpecFromAPI(req.Body.VisualSpec),
	}
	if req.Body.Source != nil {
		source := dashboardWidgetSourceFromAPI(req.Body.Source)
		domReq.Source = &source
	}
	if req.Body.Layout != nil {
		layout := dashboardWidgetLayoutFromAPI(req.Body.Layout)
		domReq.Layout = &layout
	}
	item, err := h.dashboards.UpdateWidget(ctx, cp.Name, cp.IsAdmin, req.WidgetId, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateDashboardWidget403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateDashboardWidget404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateDashboardWidget400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenUpdateDashboardWidget200JSONResponse{
		Body:    dashboardWidgetToAPI(*item),
		Headers: GenUpdateDashboardWidget200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteDashboardWidget implements the endpoint for deleting dashboard widgets.
func (h *APIHandler) DeleteDashboardWidget(ctx context.Context, req GenDeleteDashboardWidgetRequest) (GenDeleteDashboardWidgetResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.dashboards.DeleteWidget(ctx, cp.Name, cp.IsAdmin, req.WidgetId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteDashboardWidget403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteDashboardWidget404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteDashboardWidget204Response{Headers: GenDeleteDashboardWidget204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

func dashboardToAPI(item domain.Dashboard) Dashboard {
	return Dashboard{
		Id:          optStr(item.ID),
		Name:        optStr(item.Name),
		Description: optStr(item.Description),
		Owner:       optStr(item.Owner),
		FolderId:    optStr(item.FolderID),
		CreatedAt:   formatTimePtr(&item.CreatedAt),
		UpdatedAt:   formatTimePtr(&item.UpdatedAt),
	}
}

func dashboardWidgetToAPI(item domain.DashboardWidget) DashboardWidget {
	return DashboardWidget{
		Id:          optStr(item.ID),
		DashboardId: optStr(item.DashboardID),
		Name:        optStr(item.Name),
		Description: optStr(item.Description),
		Source:      ptrDashboardWidgetSource(dashboardWidgetSourceToAPI(item.Source)),
		VisualSpec:  visualSpecToAPI(item.VisualSpec),
		Layout:      ptrDashboardWidgetLayout(dashboardWidgetLayoutToAPI(item.Layout)),
		CreatedAt:   formatTimePtr(&item.CreatedAt),
		UpdatedAt:   formatTimePtr(&item.UpdatedAt),
	}
}

func dashboardWidgetSourceFromAPI(source *DashboardWidgetSource) domain.DashboardWidgetSource {
	if source == nil {
		return domain.DashboardWidgetSource{}
	}
	out := domain.DashboardWidgetSource{Kind: domain.DashboardWidgetSourceKind(source.Kind)}
	if source.SqlQuery != nil {
		out.SQLQuery = &domain.DashboardSQLQuerySource{
			SQL:     source.SqlQuery.Sql,
			Catalog: source.SqlQuery.Catalog,
			Schema:  source.SqlQuery.Schema,
		}
	}
	if source.NotebookCell != nil {
		out.NotebookCell = &domain.DashboardNotebookCellSource{
			NotebookID: source.NotebookCell.NotebookId,
			CellID:     source.NotebookCell.CellId,
		}
	}
	if source.SemanticQuery != nil {
		out.SemanticQuery = &domain.DashboardSemanticQuerySource{
			ProjectName:       source.SemanticQuery.ProjectName,
			SemanticModelName: source.SemanticQuery.SemanticModelName,
			Metrics:           source.SemanticQuery.Metrics,
			Dimensions:        sliceOrEmpty(source.SemanticQuery.Dimensions),
			Filters:           sliceOrEmpty(source.SemanticQuery.Filters),
			OrderBy:           sliceOrEmpty(source.SemanticQuery.OrderBy),
			TimeGrain:         source.SemanticQuery.TimeGrain,
		}
		if source.SemanticQuery.Limit != nil {
			limit := int(*source.SemanticQuery.Limit)
			out.SemanticQuery.Limit = &limit
		}
	}
	return out
}

func dashboardWidgetSourceToAPI(source domain.DashboardWidgetSource) DashboardWidgetSource {
	out := DashboardWidgetSource{Kind: DashboardWidgetSourceKind(source.Kind)}
	if source.SQLQuery != nil {
		out.SqlQuery = &DashboardSQLQuerySource{
			Sql:     source.SQLQuery.SQL,
			Catalog: source.SQLQuery.Catalog,
			Schema:  source.SQLQuery.Schema,
		}
	}
	if source.NotebookCell != nil {
		out.NotebookCell = &DashboardNotebookCellSource{
			NotebookId: source.NotebookCell.NotebookID,
			CellId:     source.NotebookCell.CellID,
		}
	}
	if source.SemanticQuery != nil {
		out.SemanticQuery = &DashboardSemanticQuerySource{
			ProjectName:       source.SemanticQuery.ProjectName,
			SemanticModelName: source.SemanticQuery.SemanticModelName,
			Metrics:           source.SemanticQuery.Metrics,
			Dimensions:        slicePtr(source.SemanticQuery.Dimensions),
			Filters:           slicePtr(source.SemanticQuery.Filters),
			OrderBy:           slicePtr(source.SemanticQuery.OrderBy),
			TimeGrain:         source.SemanticQuery.TimeGrain,
		}
		if source.SemanticQuery.Limit != nil {
			limit := safeIntToInt32(*source.SemanticQuery.Limit)
			out.SemanticQuery.Limit = &limit
		}
	}
	return out
}

func dashboardWidgetLayoutFromAPI(layout *DashboardWidgetLayout) domain.DashboardWidgetLayout {
	if layout == nil {
		return domain.DashboardWidgetLayout{}
	}
	return domain.DashboardWidgetLayout{
		X: int(layout.X),
		Y: int(layout.Y),
		W: int(layout.W),
		H: int(layout.H),
	}
}

func dashboardWidgetLayoutToAPI(layout domain.DashboardWidgetLayout) DashboardWidgetLayout {
	return DashboardWidgetLayout{
		X: safeIntToInt32(layout.X),
		Y: safeIntToInt32(layout.Y),
		W: safeIntToInt32(layout.W),
		H: safeIntToInt32(layout.H),
	}
}

func ptrDashboard(v Dashboard) *Dashboard { return &v }

func resolvedDashboardWidgetToAPI(item dashboardsvc.ResolvedWidget) ResolvedDashboardWidget {
	rowCount := int64(item.RowCount)
	return ResolvedDashboardWidget{
		Widget:       ptrDashboardWidget(dashboardWidgetToAPI(item.Widget)),
		Columns:      append([]string(nil), item.Columns...),
		Rows:         rowsToStringGrid(item.Rows),
		RowCount:     safeInt64ToInt32Ptr(&rowCount),
		GeneratedSql: optStr(item.GeneratedSQL),
	}
}

func ptrDashboardWidget(v DashboardWidget) *DashboardWidget { return &v }

func ptrDashboardWidgetSource(v DashboardWidgetSource) *DashboardWidgetSource { return &v }

func ptrDashboardWidgetLayout(v DashboardWidgetLayout) *DashboardWidgetLayout { return &v }

func slicePtr(items []string) *[]string {
	if len(items) == 0 {
		return nil
	}
	copyItems := append([]string(nil), items...)
	return &copyItems
}

func rowsToStringGrid(rows [][]interface{}) *[][]string {
	if len(rows) == 0 {
		return nil
	}
	out := make([][]string, len(rows))
	for i, row := range rows {
		out[i] = make([]string, len(row))
		for j, cell := range row {
			out[i][j] = fmt.Sprint(cell)
		}
	}
	return &out
}
