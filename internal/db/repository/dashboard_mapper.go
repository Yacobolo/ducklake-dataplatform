package repository

import (
	"encoding/json"
	"fmt"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

func dashboardFromDB(row dbstore.Dashboard) *domain.Dashboard {
	return &domain.Dashboard{
		ID:                  row.ID,
		Name:                row.Name,
		Description:         row.Description,
		Owner:               row.Owner,
		FolderID:            row.FolderID.String,
		SemanticProjectName: row.SemanticProjectName,
		SemanticModelName:   row.SemanticModelName,
		Compute: domain.DashboardComputePolicy{
			Mode:         row.ComputeMode,
			EndpointName: row.ComputeEndpointName,
			FallbackLocal: row.ComputeFallbackLocal != 0,
		}.Normalize(),
		CreatedAt:           parseDBTime(row.CreatedAt, "dashboards.created_at"),
		UpdatedAt:           parseDBTime(row.UpdatedAt, "dashboards.updated_at"),
	}
}

func dashboardWidgetFromDB(row dbstore.DashboardWidget) (*domain.DashboardWidget, error) {
	var source domain.DashboardWidgetSource
	if err := json.Unmarshal([]byte(row.SourceJson), &source); err != nil {
		return nil, fmt.Errorf("unmarshal dashboard widget source: %w", err)
	}

	var visualSpec *domain.VisualSpec
	if row.VisualSpec != "" {
		visualSpec = &domain.VisualSpec{}
		if err := json.Unmarshal([]byte(row.VisualSpec), visualSpec); err != nil {
			return nil, fmt.Errorf("unmarshal dashboard widget visual spec: %w", err)
		}
	}

	return &domain.DashboardWidget{
		ID:              row.ID,
		DashboardID:     row.DashboardID,
		FilterOriginKey: row.FilterOriginKey,
		PageName:        domain.NormalizeDashboardPageName(row.PageName),
		Name:            row.Name,
		Description:     row.Description,
		Source:          source,
		VisualSpec:      visualSpec,
		Layout: domain.DashboardWidgetLayout{
			X: int(row.LayoutX),
			Y: int(row.LayoutY),
			W: int(row.LayoutW),
			H: int(row.LayoutH),
		},
		CreatedAt: parseDBTime(row.CreatedAt, "dashboard_widgets.created_at"),
		UpdatedAt: parseDBTime(row.UpdatedAt, "dashboard_widgets.updated_at"),
	}, nil
}
