package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

var _ domain.DashboardRepository = (*DashboardRepo)(nil)
var _ domain.DashboardWidgetRepository = (*DashboardWidgetRepo)(nil)

// DashboardRepo persists dashboards in SQLite.
type DashboardRepo struct {
	q *dbstore.Queries
}

// NewDashboardRepo constructs a dashboard repository.
func NewDashboardRepo(db *sql.DB) *DashboardRepo {
	return &DashboardRepo{q: dbstore.New(db)}
}

// Create inserts a dashboard record.
func (r *DashboardRepo) Create(ctx context.Context, d *domain.Dashboard) (*domain.Dashboard, error) {
	row, err := r.q.CreateDashboard(ctx, dbstore.CreateDashboardParams{
		ID:          newID(),
		Name:        d.Name,
		Description: d.Description,
		Owner:       d.Owner,
		FolderID:    nullStringPtr(stringPtr(d.FolderID)),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return dashboardFromDB(row), nil
}

// GetByID loads a dashboard by ID.
func (r *DashboardRepo) GetByID(ctx context.Context, id string) (*domain.Dashboard, error) {
	row, err := r.q.GetDashboard(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return dashboardFromDB(row), nil
}

// List returns dashboards matching the optional owner filter.
func (r *DashboardRepo) List(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Dashboard, int64, error) {
	total, err := r.q.CountDashboards(ctx, dbstore.CountDashboardsParams{
		Owner:    nullableString(owner),
		FolderID: nil,
	})
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.q.ListDashboards(ctx, dbstore.ListDashboardsParams{
		Owner:    nullableString(owner),
		FolderID: nil,
		Limit:    int64(page.Limit()),
		Offset:   int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	out := make([]domain.Dashboard, 0, len(rows))
	for _, row := range rows {
		out = append(out, *dashboardFromDB(row))
	}
	return out, total, nil
}

func (r *DashboardRepo) ListByFolders(ctx context.Context, folderIDs []string) ([]domain.Dashboard, error) {
	if len(folderIDs) == 0 {
		return []domain.Dashboard{}, nil
	}
	params := make([]sql.NullString, 0, len(folderIDs))
	for _, folderID := range folderIDs {
		params = append(params, sql.NullString{String: folderID, Valid: folderID != ""})
	}
	rows, err := r.q.ListDashboardsByFolders(ctx, params)
	if err != nil {
		return nil, mapDBError(err)
	}
	out := make([]domain.Dashboard, 0, len(rows))
	for _, row := range rows {
		out = append(out, *dashboardFromDB(row))
	}
	return out, nil
}

// Update applies partial updates to a dashboard.
func (r *DashboardRepo) Update(ctx context.Context, id string, req domain.UpdateDashboardRequest) (*domain.Dashboard, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	name := current.Name
	if req.Name != nil {
		name = *req.Name
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	row, err := r.q.UpdateDashboard(ctx, dbstore.UpdateDashboardParams{
		Name:        name,
		Description: description,
		FolderID:    nullStringPtr(req.FolderID),
		ID:          id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return dashboardFromDB(row), nil
}

// Delete removes a dashboard by ID.
func (r *DashboardRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteDashboard(ctx, id))
}

// DashboardWidgetRepo persists dashboard widgets in SQLite.
type DashboardWidgetRepo struct {
	q *dbstore.Queries
}

// NewDashboardWidgetRepo constructs a dashboard widget repository.
func NewDashboardWidgetRepo(db *sql.DB) *DashboardWidgetRepo {
	return &DashboardWidgetRepo{q: dbstore.New(db)}
}

// Create inserts a dashboard widget record.
func (r *DashboardWidgetRepo) Create(ctx context.Context, w *domain.DashboardWidget) (*domain.DashboardWidget, error) {
	sourceJSON, visualJSON, err := marshalDashboardWidgetJSON(w.Source, w.VisualSpec)
	if err != nil {
		return nil, err
	}
	row, err := r.q.CreateDashboardWidget(ctx, dbstore.CreateDashboardWidgetParams{
		ID:          newID(),
		DashboardID: w.DashboardID,
		Name:        w.Name,
		Description: w.Description,
		SourceJson:  sourceJSON,
		VisualSpec:  visualJSON,
		LayoutX:     int64(w.Layout.X),
		LayoutY:     int64(w.Layout.Y),
		LayoutW:     int64(w.Layout.W),
		LayoutH:     int64(w.Layout.H),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return dashboardWidgetFromDB(row)
}

// GetByID loads a widget by ID.
func (r *DashboardWidgetRepo) GetByID(ctx context.Context, id string) (*domain.DashboardWidget, error) {
	row, err := r.q.GetDashboardWidget(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return dashboardWidgetFromDB(row)
}

// ListByDashboard returns widgets for the specified dashboard.
func (r *DashboardWidgetRepo) ListByDashboard(ctx context.Context, dashboardID string) ([]domain.DashboardWidget, error) {
	rows, err := r.q.ListDashboardWidgetsByDashboard(ctx, dashboardID)
	if err != nil {
		return nil, mapDBError(err)
	}
	out := make([]domain.DashboardWidget, 0, len(rows))
	for _, row := range rows {
		item, err := dashboardWidgetFromDB(row)
		if err != nil {
			return nil, err
		}
		out = append(out, *item)
	}
	return out, nil
}

// Update applies partial updates to a widget.
func (r *DashboardWidgetRepo) Update(ctx context.Context, id string, req domain.UpdateDashboardWidgetRequest) (*domain.DashboardWidget, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	name := current.Name
	if req.Name != nil {
		name = *req.Name
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	source := current.Source
	if req.Source != nil {
		source = *req.Source
	}
	visualSpec := current.VisualSpec
	if req.VisualSpec != nil {
		visualSpec = req.VisualSpec
	}
	layout := current.Layout
	if req.Layout != nil {
		layout = *req.Layout
	}
	sourceJSON, visualJSON, err := marshalDashboardWidgetJSON(source, visualSpec)
	if err != nil {
		return nil, err
	}
	row, err := r.q.UpdateDashboardWidget(ctx, dbstore.UpdateDashboardWidgetParams{
		Name:        name,
		Description: description,
		SourceJson:  sourceJSON,
		VisualSpec:  visualJSON,
		LayoutX:     int64(layout.X),
		LayoutY:     int64(layout.Y),
		LayoutW:     int64(layout.W),
		LayoutH:     int64(layout.H),
		ID:          id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return dashboardWidgetFromDB(row)
}

// Delete removes a widget by ID.
func (r *DashboardWidgetRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteDashboardWidget(ctx, id))
}

func marshalDashboardWidgetJSON(source domain.DashboardWidgetSource, visual *domain.VisualSpec) (string, string, error) {
	sourceBytes, err := json.Marshal(source)
	if err != nil {
		return "", "", fmt.Errorf("marshal widget source: %w", err)
	}
	visualJSON := ""
	if visual != nil {
		visualBytes, err := json.Marshal(visual)
		if err != nil {
			return "", "", fmt.Errorf("marshal widget visual spec: %w", err)
		}
		visualJSON = string(visualBytes)
	}
	return string(sourceBytes), visualJSON, nil
}

func nullableString(value *string) interface{} {
	if value == nil {
		return nil
	}
	return *value
}
