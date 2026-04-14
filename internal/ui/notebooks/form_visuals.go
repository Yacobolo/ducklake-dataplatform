package notebooks

import (
	"net/url"

	"github.com/Yacobolo/quackstack/internal/domain"
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
