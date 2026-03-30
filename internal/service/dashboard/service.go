// Package dashboard implements dashboard CRUD and widget resolution over
// direct SQL, notebook outputs, and semantic queries.
package dashboard

import (
	"context"
	"encoding/json"
	"fmt"

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
}

// CreateDashboard creates a new dashboard owned by the caller.
func (s *Service) CreateDashboard(ctx context.Context, owner string, req domain.CreateDashboardRequest) (*domain.Dashboard, error) {
	if err := req.Validate(); err != nil {
		return nil, err
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
		Name:        req.Name,
		Description: req.Description,
		Owner:       owner,
		FolderID:    folderID,
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

// UpdateDashboard updates dashboard metadata.
func (s *Service) UpdateDashboard(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error) {
	current, err := s.dashboards.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if current.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the dashboard owner or admin can update")
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
	item, err := s.widgets.Create(ctx, &domain.DashboardWidget{
		DashboardID: dashboardID,
		Name:        req.Name,
		Description: req.Description,
		Source:      req.Source,
		VisualSpec:  req.VisualSpec,
		Layout:      req.Layout,
	})
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{PrincipalName: principal, Action: "CREATE_DASHBOARD_WIDGET", Status: "ALLOWED"})
	return item, nil
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
		resolved, err := s.ResolveWidget(ctx, principal, widget)
		if err != nil {
			return nil, fmt.Errorf("resolve widget %q: %w", widget.Name, err)
		}
		out = append(out, *resolved)
	}
	return out, nil
}

// ResolveWidget resolves a single widget to tabular data.
func (s *Service) ResolveWidget(ctx context.Context, principal string, widget domain.DashboardWidget) (*ResolvedWidget, error) {
	switch widget.Source.Kind {
	case domain.DashboardWidgetSourceSQLQuery:
		result, err := s.queryExec.Execute(ctx, principal, widget.Source.SQLQuery.SQL)
		if err != nil {
			return nil, err
		}
		if err := widget.VisualSpec.ValidateColumns(result.Columns); err != nil {
			return nil, err
		}
		return &ResolvedWidget{
			Widget:       widget,
			Columns:      result.Columns,
			Rows:         result.Rows,
			RowCount:     result.RowCount,
			GeneratedSQL: widget.Source.SQLQuery.SQL,
		}, nil
	case domain.DashboardWidgetSourceNotebookCell:
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
		return &ResolvedWidget{
			Widget:   widget,
			Columns:  result.Columns,
			Rows:     result.Rows,
			RowCount: result.RowCount,
		}, nil
	case domain.DashboardWidgetSourceSemanticQuery:
		req := semantic.MetricQueryRequest{
			ProjectName:       widget.Source.SemanticQuery.ProjectName,
			SemanticModelName: widget.Source.SemanticQuery.SemanticModelName,
			Metrics:           widget.Source.SemanticQuery.Metrics,
			Dimensions:        widget.Source.SemanticQuery.Dimensions,
			Filters:           widget.Source.SemanticQuery.Filters,
			OrderBy:           widget.Source.SemanticQuery.OrderBy,
			Limit:             widget.Source.SemanticQuery.Limit,
			TimeGrain:         widget.Source.SemanticQuery.TimeGrain,
		}
		result, err := s.semantic.RunMetricQuery(ctx, principal, req)
		if err != nil {
			return nil, err
		}
		if err := widget.VisualSpec.ValidateColumns(result.Result.Columns); err != nil {
			return nil, err
		}
		return &ResolvedWidget{
			Widget:       widget,
			Columns:      result.Result.Columns,
			Rows:         result.Result.Rows,
			RowCount:     result.Result.RowCount,
			GeneratedSQL: result.Plan.GeneratedSQL,
		}, nil
	default:
		return nil, domain.ErrValidation("unsupported widget source kind %q", string(widget.Source.Kind))
	}
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
