// Package dashboard implements dashboard CRUD and widget resolution over
// direct SQL, notebook outputs, and semantic queries.
package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
	"duck-demo/internal/service/semantic"
)

type queryExecutor interface {
	Execute(ctx context.Context, principalName, sqlQuery string) (*query.QueryResult, error)
}

// Service coordinates dashboard CRUD and widget resolution.
type Service struct {
	dashboards domain.DashboardRepository
	widgets    domain.DashboardWidgetRepository
	notebooks  domain.NotebookRepository
	folders    domain.FolderRepository
	audit      domain.AuditRepository
	queryExec  queryExecutor
	semantic   *semantic.Service
}

// NewService constructs a dashboard service.
func NewService(
	dashboards domain.DashboardRepository,
	widgets domain.DashboardWidgetRepository,
	notebooks domain.NotebookRepository,
	audit domain.AuditRepository,
	queryExec queryExecutor,
	semanticSvc *semantic.Service,
) *Service {
	return &Service{
		dashboards: dashboards,
		widgets:    widgets,
		notebooks:  notebooks,
		audit:      audit,
		queryExec:  queryExec,
		semantic:   semanticSvc,
	}
}

// SetFolderRepository enables folder-backed dashboard placement.
func (s *Service) SetFolderRepository(folders domain.FolderRepository) {
	s.folders = folders
}

// ResolvedWidget contains a widget definition plus resolved tabular data.
type ResolvedWidget struct {
	Widget       domain.DashboardWidget
	Columns      []string
	Rows         [][]interface{}
	RowCount     int
	GeneratedSQL string
	Interaction  *ResolvedWidgetInteraction
	Page         *ResolvedWidgetPage
	Sort         *ResolvedWidgetSort
}

// ResolvedWidgetPage describes a paged table slice emitted for a widget.
type ResolvedWidgetPage struct {
	Offset  int  `json:"offset"`
	Append  bool `json:"append"`
	HasMore bool `json:"has_more"`
}

// ResolvedWidgetSort captures the effective sort applied to a table widget.
type ResolvedWidgetSort struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
}

// PageState represents the resolved widgets and filters for one dashboard page.
type PageState struct {
	Dashboard     *domain.Dashboard
	PageName      string
	Widgets       []ResolvedWidget
	ActiveFilters []InteractiveFilter
}

// TablePageRequest describes a table widget page or sort request.
type TablePageRequest struct {
	Offset        int
	Limit         int
	Append        bool
	SortColumn    string
	SortDirection string
}

// CreateDashboard creates a new dashboard owned by the caller.
func (s *Service) CreateDashboard(ctx context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error) {
	if strings.TrimSpace(req.Owner) != "" {
		owner = strings.TrimSpace(req.Owner)
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	compute := domain.DashboardComputePolicy{}
	if req.Compute != nil {
		compute = req.Compute.Normalize()
	} else {
		compute = compute.Normalize()
	}
	folderID := ""
	if req.FolderID != nil && *req.FolderID != "" {
		folderID = *req.FolderID
	} else if s.folders != nil {
		root, err := s.folders.EnsurePersonalRoot(ctx, owner)
		if err != nil {
			return nil, err
		}
		folderID = root.ID
	}
	item, err := s.dashboards.Create(ctx, &domain.Dashboard{
		Name:                req.Name,
		Description:         req.Description,
		Owner:               owner,
		FolderID:            folderID,
		SemanticProjectName: req.SemanticProjectName,
		SemanticModelName:   req.SemanticModelName,
		Compute:             compute,
	})
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: owner, Action: "CREATE_DASHBOARD", Status: "ALLOWED"})
	return item, nil
}

// ListDashboards returns dashboards filtered by optional owner.
func (s *Service) ListDashboards(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error) {
	return s.dashboards.List(ctx, owner, page)
}

// GetDashboard loads a dashboard and its widgets.
func (s *Service) GetDashboard(ctx context.Context, id string) (*domain.Dashboard, []domain.DashboardWidget, error) {
	dashboard, err := s.dashboards.GetByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	widgets, err := s.widgets.ListByDashboard(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	return dashboard, widgets, nil
}

// ListWidgets loads widgets for a dashboard after confirming the dashboard exists.
func (s *Service) ListWidgets(ctx context.Context, dashboardID string) ([]domain.DashboardWidget, error) {
	if _, err := s.dashboards.GetByID(ctx, dashboardID); err != nil {
		return nil, err
	}
	return s.widgets.ListByDashboard(ctx, dashboardID)
}

// GetWidget loads a widget scoped to a dashboard.
func (s *Service) GetWidget(ctx context.Context, dashboardID, widgetID string) (*domain.DashboardWidget, error) {
	if _, err := s.dashboards.GetByID(ctx, dashboardID); err != nil {
		return nil, err
	}
	widget, err := s.widgets.GetByID(ctx, widgetID)
	if err != nil {
		return nil, err
	}
	if widget.DashboardID != dashboardID {
		return nil, domain.ErrNotFound("dashboard widget %q not found", widgetID)
	}
	return widget, nil
}

// UpdateDashboard updates dashboard metadata.
func (s *Service) UpdateDashboard(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error) {
	current, err := s.dashboards.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if current.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the dashboard owner or admin can update")
	}
	if req.Owner != nil {
		owner := strings.TrimSpace(*req.Owner)
		if owner == "" {
			return nil, domain.ErrValidation("dashboard owner is required")
		}
		if owner != current.Owner && !isAdmin {
			return nil, domain.ErrAccessDenied("only an admin can change dashboard owner")
		}
		req.Owner = &owner
	}
	if req.Compute != nil {
		compute := req.Compute.Normalize()
		if err := compute.Validate(); err != nil {
			return nil, err
		}
		req.Compute = &compute
	}
	item, err := s.dashboards.Update(ctx, id, req)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal, Action: "UPDATE_DASHBOARD", Status: "ALLOWED"})
	return item, nil
}

// DeleteDashboard deletes a dashboard after authorization checks.
func (s *Service) DeleteDashboard(ctx context.Context, principal string, isAdmin bool, id string) error {
	current, err := s.dashboards.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if current.Owner != principal && !isAdmin {
		return domain.ErrAccessDenied("only the dashboard owner or admin can delete")
	}
	if err := s.dashboards.Delete(ctx, id); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal, Action: "DELETE_DASHBOARD", Status: "ALLOWED"})
	return nil
}

// CreateWidget adds a widget to a dashboard.
func (s *Service) CreateWidget(ctx context.Context, principal string, isAdmin bool, dashboardID string, req domain.CreateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
	current, err := s.dashboards.GetByID(ctx, dashboardID)
	if err != nil {
		return nil, err
	}
	if current.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the dashboard owner or admin can add widgets")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	existingWidgets, err := s.widgets.ListByDashboard(ctx, dashboardID)
	if err != nil {
		return nil, err
	}
	filterOriginKey := strings.TrimSpace(req.FilterOriginKey)
	if filterOriginKey == "" {
		filterOriginKey = nextDashboardWidgetFilterOriginKey(req.Name, req.VisualSpec, existingWidgets)
	}
	item, err := s.widgets.Create(ctx, &domain.DashboardWidget{
		DashboardID:     dashboardID,
		FilterOriginKey: filterOriginKey,
		PageName:        domain.NormalizeDashboardPageName(req.PageName),
		Name:            req.Name,
		Description:     req.Description,
		Source:          req.Source,
		VisualSpec:      req.VisualSpec,
		Layout:          req.Layout,
	})
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal, Action: "CREATE_DASHBOARD_WIDGET", Status: "ALLOWED"})
	return item, nil
}

func nextDashboardWidgetFilterOriginKey(name string, visual *domain.VisualSpec, existing []domain.DashboardWidget) string {
	base := dashboardWidgetFilterOriginKeyBase(name, visual)
	if base == "" {
		base = "widget"
	}

	used := make(map[string]struct{}, len(existing))
	for _, widget := range existing {
		key := strings.TrimSpace(widget.FilterOriginKey)
		if key == "" {
			continue
		}
		used[key] = struct{}{}
	}
	if _, exists := used[base]; !exists {
		return base
	}

	for index := 2; ; index++ {
		candidate := fmt.Sprintf("%s-%d", base, index)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func dashboardWidgetFilterOriginKeyBase(name string, visual *domain.VisualSpec) string {
	prefix := "widget"
	if visual != nil {
		switch visual.Kind {
		case domain.VisualOutputChart:
			prefix = "chart"
		case domain.VisualOutputTable:
			prefix = "table"
		case domain.VisualOutputMetric:
			prefix = "metric"
		}
	}

	slug := slugifyDashboardWidgetOriginPart(name)
	if slug == "" {
		return prefix
	}
	return prefix + "-" + slug
}

func slugifyDashboardWidgetOriginPart(value string) string {
	var builder strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			builder.WriteRune(r)
			lastDash = false
		case !lastDash:
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

// UpdateWidget updates widget metadata and visualization settings.
func (s *Service) UpdateWidget(ctx context.Context, principal string, isAdmin bool, widgetID string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
	widget, err := s.widgets.GetByID(ctx, widgetID)
	if err != nil {
		return nil, err
	}
	dashboard, err := s.dashboards.GetByID(ctx, widget.DashboardID)
	if err != nil {
		return nil, err
	}
	if dashboard.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the dashboard owner or admin can update widgets")
	}
	if req.Source != nil {
		if err := req.Source.Validate(); err != nil {
			return nil, err
		}
	}
	if req.FilterOriginKey != nil {
		key := strings.TrimSpace(*req.FilterOriginKey)
		if err := domain.ValidateDashboardWidgetFilterOriginKey(key); err != nil {
			return nil, err
		}
		req.FilterOriginKey = &key
	}
	if req.VisualSpec != nil {
		if err := req.VisualSpec.Validate(); err != nil {
			return nil, err
		}
	}
	if req.Layout != nil {
		if err := req.Layout.Validate(); err != nil {
			return nil, err
		}
	}
	item, err := s.widgets.Update(ctx, widgetID, req)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal, Action: "UPDATE_DASHBOARD_WIDGET", Status: "ALLOWED"})
	return item, nil
}

// DeleteWidget deletes a dashboard widget after authorization checks.
func (s *Service) DeleteWidget(ctx context.Context, principal string, isAdmin bool, widgetID string) error {
	widget, err := s.widgets.GetByID(ctx, widgetID)
	if err != nil {
		return err
	}
	dashboard, err := s.dashboards.GetByID(ctx, widget.DashboardID)
	if err != nil {
		return err
	}
	if dashboard.Owner != principal && !isAdmin {
		return domain.ErrAccessDenied("only the dashboard owner or admin can delete widgets")
	}
	if err := s.widgets.Delete(ctx, widgetID); err != nil {
		return err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal, Action: "DELETE_DASHBOARD_WIDGET", Status: "ALLOWED"})
	return nil
}

// ResolveWidgets resolves a set of widgets to tabular data.
func (s *Service) ResolveWidgets(ctx context.Context, principal string, widgets []domain.DashboardWidget) ([]ResolvedWidget, error) {
	out := make([]ResolvedWidget, 0, len(widgets))
	for _, widget := range widgets {
		resolved, err := s.resolveWidgetWithContext(ctx, principal, widget, nil)
		if err != nil {
			return nil, fmt.Errorf("resolve widget %q: %w", widget.Name, err)
		}
		out = append(out, *resolved)
	}
	return out, nil
}

// ResolveWidget resolves a single widget to tabular data.
func (s *Service) ResolveWidget(ctx context.Context, principal string, widget domain.DashboardWidget) (*ResolvedWidget, error) {
	return s.resolveWidgetWithContext(ctx, principal, widget, nil)
}

// BuildDashboardPageState resolves the widgets and active filters for a single dashboard page.
func (s *Service) BuildDashboardPageState(ctx context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, pageName string, filters []InteractiveFilter, tablePages map[string]TablePageRequest) (*PageState, error) {
	if dashboard == nil {
		return nil, domain.ErrValidation("dashboard is required")
	}
	ctx = dashboardComputeContext(ctx, dashboard)

	pageName = normalizeDashboardStatePageName(pageName)
	pageWidgets := dashboardStateWidgetsForPage(widgets, pageName)
	resolved, err := s.ResolveWidgetsForDashboardPaged(ctx, principal, dashboard, pageWidgets, filters, tablePages)
	if err != nil {
		return nil, err
	}

	return &PageState{
		Dashboard:     dashboard,
		PageName:      pageName,
		Widgets:       resolved,
		ActiveFilters: cloneDashboardInteractiveFilters(filters),
	}, nil
}

// ResolveWidgetsForDashboardPaged resolves dashboard widgets with optional per-table page requests.
func (s *Service) ResolveWidgetsForDashboardPaged(ctx context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, filters []InteractiveFilter, tablePages map[string]TablePageRequest) ([]ResolvedWidget, error) {
	ctx = dashboardComputeContext(ctx, dashboard)
	interactionCtx, err := s.buildDashboardInteractionContext(ctx, dashboard, widgets, filters)
	if err != nil {
		return nil, err
	}

	out := make([]ResolvedWidget, 0, len(widgets))
	for _, widget := range widgets {
		var pageReq *TablePageRequest
		if widget.VisualSpec != nil && widget.VisualSpec.Kind == domain.VisualOutputTable {
			if page, ok := tablePages[widget.ID]; ok {
				pageCopy := page
				pageReq = &pageCopy
			}
		}
		resolved, err := s.resolveWidgetWithContextAndPage(ctx, principal, widget, interactionCtx, pageReq)
		if err != nil {
			return nil, fmt.Errorf("resolve widget %q: %w", widget.Name, err)
		}
		out = append(out, *resolved)
	}
	return out, nil
}

// ResolveWidgetForDashboardPage resolves one dashboard widget with the supplied page request in dashboard context.
func (s *Service) ResolveWidgetForDashboardPage(ctx context.Context, principal string, dashboard *domain.Dashboard, widgets []domain.DashboardWidget, widgetID string, filters []InteractiveFilter, page TablePageRequest) (*ResolvedWidget, error) {
	ctx = dashboardComputeContext(ctx, dashboard)
	interactionCtx, err := s.buildDashboardInteractionContext(ctx, dashboard, widgets, filters)
	if err != nil {
		return nil, err
	}

	for _, widget := range widgets {
		if widget.ID != widgetID {
			continue
		}
		return s.resolveWidgetWithContextAndPage(ctx, principal, widget, interactionCtx, &page)
	}

	return nil, domain.ErrNotFound("dashboard widget not found")
}

func (s *Service) resolveWidgetWithContext(ctx context.Context, principal string, widget domain.DashboardWidget, interactionCtx *dashboardInteractionContext) (*ResolvedWidget, error) {
	return s.resolveWidgetWithContextAndPage(ctx, principal, widget, interactionCtx, nil)
}

func (s *Service) resolveWidgetWithContextAndPage(ctx context.Context, principal string, widget domain.DashboardWidget, interactionCtx *dashboardInteractionContext, pageReq *TablePageRequest) (*ResolvedWidget, error) {
	switch widget.Source.Kind {
	case domain.DashboardWidgetSourceSQLQuery:
		if s.queryExec == nil {
			return nil, domain.ErrValidation("dashboard query execution is not configured")
		}
		result, err := s.queryExec.Execute(ctx, principal, widget.Source.SQLQuery.SQL)
		if err != nil {
			return nil, err
		}
		if err := widget.VisualSpec.ValidateColumns(result.Columns); err != nil {
			return nil, err
		}
		rows := result.Rows
		rowCount := result.RowCount
		var page *ResolvedWidgetPage
		sortState := resolveTablePageSort(result.Columns, pageReq)
		if sortState != nil {
			rows = sortWidgetRows(result.Columns, rows, *sortState)
		}
		if pageReq != nil && widget.VisualSpec != nil && widget.VisualSpec.Kind == domain.VisualOutputTable {
			rows, page = sliceWidgetRows(rows, rowCount, *pageReq)
		}
		resolved := &ResolvedWidget{
			Widget:       widget,
			Columns:      result.Columns,
			Rows:         rows,
			RowCount:     rowCount,
			GeneratedSQL: widget.Source.SQLQuery.SQL,
			Page:         page,
			Sort:         sortState,
		}
		if interactionCtx != nil && interactionCtx.Interactive {
			resolved.Interaction = buildResolvedWidgetInteraction(widget, interactionCtx)
		}
		return resolved, nil
	case domain.DashboardWidgetSourceNotebookCell:
		if s.notebooks == nil {
			return nil, domain.ErrValidation("dashboard notebook access is not configured")
		}
		cell, err := s.notebooks.GetCell(ctx, widget.Source.NotebookCell.CellID)
		if err != nil {
			return nil, err
		}
		if cell.LastResult == nil || *cell.LastResult == "" {
			return nil, domain.ErrValidation("notebook cell does not have a cached result yet")
		}
		result, err := parseCachedCellResult(*cell.LastResult)
		if err != nil {
			return nil, err
		}
		if err := widget.VisualSpec.ValidateColumns(result.Columns); err != nil {
			return nil, err
		}
		rows := result.Rows
		rowCount := result.RowCount
		var page *ResolvedWidgetPage
		sortState := resolveTablePageSort(result.Columns, pageReq)
		if sortState != nil {
			rows = sortWidgetRows(result.Columns, rows, *sortState)
		}
		if pageReq != nil && widget.VisualSpec != nil && widget.VisualSpec.Kind == domain.VisualOutputTable {
			rows, page = sliceWidgetRows(rows, rowCount, *pageReq)
		}
		resolved := &ResolvedWidget{
			Widget:   widget,
			Columns:  result.Columns,
			Rows:     rows,
			RowCount: rowCount,
			Page:     page,
			Sort:     sortState,
		}
		if interactionCtx != nil && interactionCtx.Interactive {
			resolved.Interaction = buildResolvedWidgetInteraction(widget, interactionCtx)
		}
		return resolved, nil
	case domain.DashboardWidgetSourceSemanticQuery:
		if s.semantic == nil {
			return nil, domain.ErrValidation("dashboard semantic query execution is not configured")
		}
		req := semantic.MetricQueryRequest{
			ProjectName:       widget.Source.SemanticQuery.ProjectName,
			SemanticModelName: widget.Source.SemanticQuery.SemanticModelName,
			SemanticModelID:   widget.Source.SemanticQuery.SemanticModelID,
			Metrics:           widget.Source.SemanticQuery.Metrics,
			RelationshipNames: widget.Source.SemanticQuery.RelationshipNames,
			Dimensions:        widget.Source.SemanticQuery.Dimensions,
			Filters:           append([]string(nil), widget.Source.SemanticQuery.Filters...),
			OrderBy:           widget.Source.SemanticQuery.OrderBy,
			Limit:             widget.Source.SemanticQuery.Limit,
			TimeGrain:         widget.Source.SemanticQuery.TimeGrain,
		}
		sortState := resolveTablePageSort(dashboardSemanticQueryColumns(widget), pageReq)
		if sortState != nil {
			req.OrderBy = []string{sortState.Column + " " + strings.ToUpper(sortState.Direction)}
		}
		if pageReq != nil && widget.VisualSpec != nil && widget.VisualSpec.Kind == domain.VisualOutputTable {
			limit := pageReq.Limit + 1
			offset := pageReq.Offset
			req.Limit = &limit
			req.Offset = &offset
		}
		if interactionCtx != nil && widgetParticipatesInDashboardInteraction(widget, interactionCtx) {
			req.Filters = append(req.Filters, buildDashboardFilterClauses(widgetQueryFilters(widget, interactionCtx), interactionCtx.FilterSpecs)...)
		}
		result, err := s.semantic.RunMetricQuery(ctx, principal, req)
		if err != nil {
			return nil, err
		}
		if err := widget.VisualSpec.ValidateColumns(result.Result.Columns); err != nil {
			return nil, err
		}
		rows := result.Result.Rows
		rowCount := result.Result.RowCount
		var page *ResolvedWidgetPage
		if pageReq != nil && widget.VisualSpec != nil && widget.VisualSpec.Kind == domain.VisualOutputTable {
			totalCount, countErr := s.countSemanticQueryRows(ctx, principal, req)
			if countErr == nil && totalCount >= len(rows) {
				rowCount = totalCount
			}
			rows, page = trimPagedSemanticRows(rows, *pageReq)
		}
		resolved := &ResolvedWidget{
			Widget:       widget,
			Columns:      result.Result.Columns,
			Rows:         rows,
			RowCount:     rowCount,
			GeneratedSQL: result.Plan.GeneratedSQL,
			Page:         page,
			Sort:         sortState,
		}
		if interactionCtx != nil && interactionCtx.Interactive {
			resolved.Interaction = buildResolvedWidgetInteraction(widget, interactionCtx)
		}
		return resolved, nil
	default:
		return nil, domain.ErrValidation("unsupported widget source kind %q", string(widget.Source.Kind))
	}
}

func resolveTablePageSort(columns []string, pageReq *TablePageRequest) *ResolvedWidgetSort {
	if pageReq == nil {
		return nil
	}
	column := strings.TrimSpace(pageReq.SortColumn)
	if column == "" {
		return nil
	}
	direction := strings.ToLower(strings.TrimSpace(pageReq.SortDirection))
	if direction != "asc" && direction != "desc" {
		direction = "asc"
	}
	for _, candidate := range columns {
		if strings.EqualFold(strings.TrimSpace(candidate), column) {
			return &ResolvedWidgetSort{
				Column:    candidate,
				Direction: direction,
			}
		}
	}
	return nil
}

func dashboardSemanticQueryColumns(widget domain.DashboardWidget) []string {
	if widget.Source.SemanticQuery == nil {
		return nil
	}
	columns := make([]string, 0, len(widget.Source.SemanticQuery.Dimensions)+len(widget.Source.SemanticQuery.Metrics))
	columns = append(columns, widget.Source.SemanticQuery.Dimensions...)
	columns = append(columns, widget.Source.SemanticQuery.Metrics...)
	return columns
}

func sortWidgetRows(columns []string, rows [][]interface{}, sortState ResolvedWidgetSort) [][]interface{} {
	if len(rows) <= 1 {
		return rows
	}

	columnIndex := -1
	for index, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), strings.TrimSpace(sortState.Column)) {
			columnIndex = index
			break
		}
	}
	if columnIndex < 0 {
		return rows
	}

	sorted := make([][]interface{}, len(rows))
	copy(sorted, rows)
	direction := 1
	if strings.EqualFold(sortState.Direction, "desc") {
		direction = -1
	}
	sort.SliceStable(sorted, func(i, j int) bool {
		left := interface{}(nil)
		right := interface{}(nil)
		if columnIndex < len(sorted[i]) {
			left = sorted[i][columnIndex]
		}
		if columnIndex < len(sorted[j]) {
			right = sorted[j][columnIndex]
		}
		return compareWidgetRowValues(left, right)*direction < 0
	})
	return sorted
}

func compareWidgetRowValues(left, right interface{}) int {
	leftNumber, leftOK := widgetRowNumberValue(left)
	rightNumber, rightOK := widgetRowNumberValue(right)
	if leftOK && rightOK {
		switch {
		case leftNumber < rightNumber:
			return -1
		case leftNumber > rightNumber:
			return 1
		default:
			return 0
		}
	}

	leftText := strings.TrimSpace(fmt.Sprint(left))
	rightText := strings.TrimSpace(fmt.Sprint(right))
	return strings.Compare(strings.ToLower(leftText), strings.ToLower(rightText))
}

func widgetRowNumberValue(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func sliceWidgetRows(rows [][]interface{}, total int, pageReq TablePageRequest) ([][]interface{}, *ResolvedWidgetPage) {
	if pageReq.Limit <= 0 {
		return rows, nil
	}
	if total <= 0 {
		total = len(rows)
	}
	offset := pageReq.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(rows) {
		return [][]interface{}{}, &ResolvedWidgetPage{Offset: offset, Append: pageReq.Append, HasMore: false}
	}
	end := offset + pageReq.Limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end], &ResolvedWidgetPage{
		Offset:  offset,
		Append:  pageReq.Append,
		HasMore: end < total,
	}
}

func trimPagedSemanticRows(rows [][]interface{}, pageReq TablePageRequest) ([][]interface{}, *ResolvedWidgetPage) {
	if pageReq.Limit <= 0 {
		return rows, nil
	}
	offset := pageReq.Offset
	if offset < 0 {
		offset = 0
	}
	hasMore := len(rows) > pageReq.Limit
	if hasMore {
		rows = rows[:pageReq.Limit]
	}
	return rows, &ResolvedWidgetPage{
		Offset:  offset,
		Append:  pageReq.Append,
		HasMore: hasMore,
	}
}

func (s *Service) countSemanticQueryRows(ctx context.Context, principal string, req semantic.MetricQueryRequest) (int, error) {
	if s.semantic == nil || s.queryExec == nil {
		return 0, domain.ErrValidation("dashboard semantic query counting is not configured")
	}

	countReq := req
	countReq.Limit = nil
	countReq.Offset = nil
	countReq.OrderBy = nil

	plan, err := s.semantic.ExplainMetricQuery(ctx, countReq)
	if err != nil {
		return 0, err
	}

	countSQL := fmt.Sprintf("SELECT COUNT(*) AS row_count FROM (%s) AS dashboard_widget_count", plan.GeneratedSQL)
	result, err := s.queryExec.Execute(ctx, principal, countSQL)
	if err != nil {
		return 0, err
	}
	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return 0, fmt.Errorf("count widget rows: empty result")
	}
	return countValue(result.Rows[0][0])
}

func dashboardComputeContext(ctx context.Context, dashboard *domain.Dashboard) context.Context {
	if dashboard == nil {
		return ctx
	}
	policy := dashboard.Compute.Normalize()
	req := domain.ComputeExecutionRequest{
		Mode:          policy.Mode,
		EndpointName:  policy.EndpointName,
		WorkloadType:  domain.ComputeWorkloadInteractive,
		FallbackLocal: policy.FallbackLocal,
	}
	if policy.Mode == domain.ComputeModeSharedEndpoint && policy.EndpointName != "" {
		req.AuthoritativeEndpoint = true
	}
	return domain.WithComputeExecutionRequest(ctx, req)
}

func countValue(value interface{}) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case float64:
		return int(typed), nil
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, fmt.Errorf("count widget rows: blank count")
		}
		var parsed int
		_, err := fmt.Sscanf(trimmed, "%d", &parsed)
		if err != nil {
			return 0, fmt.Errorf("count widget rows: %w", err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("count widget rows: unsupported count type %T", value)
	}
}

func normalizeDashboardStatePageName(pageName string) string {
	pageName = domain.NormalizeDashboardPageName(pageName)
	if pageName == "" {
		return domain.DefaultDashboardPageName
	}
	return pageName
}

func dashboardStateWidgetsForPage(widgets []domain.DashboardWidget, pageName string) []domain.DashboardWidget {
	pageName = normalizeDashboardStatePageName(pageName)
	filtered := make([]domain.DashboardWidget, 0, len(widgets))
	for _, widget := range widgets {
		if domain.NormalizeDashboardPageName(widget.PageName) == pageName {
			filtered = append(filtered, widget)
		}
	}
	return filtered
}

func cloneDashboardInteractiveFilters(filters []InteractiveFilter) []InteractiveFilter {
	if len(filters) == 0 {
		return nil
	}

	out := make([]InteractiveFilter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, InteractiveFilter{
			WidgetID:  filter.WidgetID,
			Dimension: filter.Dimension,
			Values:    append([]string(nil), filter.Values...),
		})
	}
	return out
}

type cachedCellResult struct {
	Columns  []string        `json:"Columns"`
	Rows     [][]interface{} `json:"Rows"`
	RowCount int             `json:"RowCount"`
}

func parseCachedCellResult(raw string) (*cachedCellResult, error) {
	var parsed cachedCellResult
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("parse cached notebook result: %w", err)
	}
	return &parsed, nil
}
