package ui

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
)

func visualSpecFromForm(values url.Values) (*domain.VisualSpec, error) {
	if values.Get("visual_kind") == "" &&
		values.Get("chart_type") == "" &&
		values.Get("visual_title") == "" &&
		values.Get("visual_x") == "" &&
		values.Get("visual_y") == "" &&
		values.Get("visual_value") == "" &&
		values.Get("visual_label") == "" {
		return nil, nil
	}
	kind := domain.VisualOutputKind(formString(values, "visual_kind"))
	if kind == "" {
		kind = domain.VisualOutputTable
	}
	spec := &domain.VisualSpec{
		Kind:         kind,
		Title:        formString(values, "visual_title"),
		Subtitle:     formString(values, "visual_subtitle"),
		ColorPalette: formString(values, "visual_palette"),
	}
	if legend := values.Get("visual_legend"); legend != "" {
		v := legend == "on" || legend == "true"
		spec.Legend = &v
	}
	if stacked := values.Get("visual_stacked"); stacked != "" {
		v := stacked == "on" || stacked == "true"
		spec.Stacked = &v
	}
	if chartType := formString(values, "chart_type"); chartType != "" {
		ct := domain.VisualChartType(chartType)
		spec.ChartType = &ct
	}
	if field := formString(values, "visual_x"); field != "" {
		spec.Encodings.X = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_y"); field != "" {
		spec.Encodings.Y = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_series"); field != "" {
		spec.Encodings.Series = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_label"); field != "" {
		spec.Encodings.Label = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_value"); field != "" {
		spec.Encodings.Value = &domain.VisualFieldBinding{Field: field}
	}
	if field := formString(values, "visual_secondary"); field != "" {
		spec.Encodings.Secondary = &domain.VisualFieldBinding{Field: field}
	}
	return spec, spec.Validate()
}

func dashboardWidgetSourceFromForm(values url.Values) (domain.DashboardWidgetSource, error) {
	kind := domain.DashboardWidgetSourceKind(formString(values, "source_kind"))
	switch kind {
	case domain.DashboardWidgetSourceSQLQuery:
		return domain.DashboardWidgetSource{
			Kind: kind,
			SQLQuery: &domain.DashboardSQLQuerySource{
				SQL: formString(values, "sql"),
			},
		}, nil
	case domain.DashboardWidgetSourceNotebookCell:
		return domain.DashboardWidgetSource{
			Kind: kind,
			NotebookCell: &domain.DashboardNotebookCellSource{
				NotebookID: formString(values, "notebook_id"),
				CellID:     formString(values, "cell_id"),
			},
		}, nil
	case domain.DashboardWidgetSourceSemanticQuery:
		source := domain.DashboardWidgetSource{
			Kind: kind,
			SemanticQuery: &domain.DashboardSemanticQuerySource{
				ProjectName:       formString(values, "project_name"),
				SemanticModelName: formString(values, "semantic_model_name"),
				Metrics:           formCSV(values, "metrics"),
				Dimensions:        formCSV(values, "dimensions"),
				Filters:           formCSV(values, "filters"),
				OrderBy:           formCSV(values, "order_by"),
			},
		}
		if rawLimit := formString(values, "limit"); rawLimit != "" {
			limit, err := strconv.Atoi(rawLimit)
			if err != nil {
				return domain.DashboardWidgetSource{}, fmt.Errorf("limit must be an integer")
			}
			source.SemanticQuery.Limit = &limit
		}
		if timeGrain := strings.TrimSpace(formString(values, "time_grain")); timeGrain != "" {
			source.SemanticQuery.TimeGrain = &timeGrain
		}
		return source, nil
	default:
		return domain.DashboardWidgetSource{}, fmt.Errorf("unsupported source kind %q", string(kind))
	}
}
