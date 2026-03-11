package api

import "duck-demo/internal/domain"

func visualSpecFromAPI(spec *VisualSpec) *domain.VisualSpec {
	if spec == nil {
		return nil
	}
	out := &domain.VisualSpec{
		Kind:         domain.VisualOutputKind(spec.Kind),
		Title:        valOrEmpty(spec.Title),
		Subtitle:     valOrEmpty(spec.Subtitle),
		ColorPalette: valOrEmpty(spec.ColorPalette),
	}
	if spec.ChartType != nil {
		chartType := domain.VisualChartType(*spec.ChartType)
		out.ChartType = &chartType
	}
	if spec.Legend != nil {
		legend := *spec.Legend
		out.Legend = &legend
	}
	if spec.Stacked != nil {
		stacked := *spec.Stacked
		out.Stacked = &stacked
	}
	if spec.Encodings != nil {
		out.Encodings = domain.VisualEncodings{
			X:         visualFieldBindingFromAPI(spec.Encodings.X),
			Y:         visualFieldBindingFromAPI(spec.Encodings.Y),
			Series:    visualFieldBindingFromAPI(spec.Encodings.Series),
			Label:     visualFieldBindingFromAPI(spec.Encodings.Label),
			Value:     visualFieldBindingFromAPI(spec.Encodings.Value),
			Secondary: visualFieldBindingFromAPI(spec.Encodings.Secondary),
		}
	}
	return out
}

func visualSpecToAPI(spec *domain.VisualSpec) *VisualSpec {
	if spec == nil {
		return nil
	}
	out := &VisualSpec{
		Kind:         VisualOutputKind(spec.Kind),
		Title:        optStr(spec.Title),
		Subtitle:     optStr(spec.Subtitle),
		ColorPalette: optStr(spec.ColorPalette),
		Encodings: &VisualEncodings{
			X:         visualFieldBindingToAPI(spec.Encodings.X),
			Y:         visualFieldBindingToAPI(spec.Encodings.Y),
			Series:    visualFieldBindingToAPI(spec.Encodings.Series),
			Label:     visualFieldBindingToAPI(spec.Encodings.Label),
			Value:     visualFieldBindingToAPI(spec.Encodings.Value),
			Secondary: visualFieldBindingToAPI(spec.Encodings.Secondary),
		},
	}
	if spec.ChartType != nil {
		chartType := VisualChartType(*spec.ChartType)
		out.ChartType = &chartType
	}
	if spec.Legend != nil {
		legend := *spec.Legend
		out.Legend = &legend
	}
	if spec.Stacked != nil {
		stacked := *spec.Stacked
		out.Stacked = &stacked
	}
	return out
}

func visualFieldBindingFromAPI(binding *VisualFieldBinding) *domain.VisualFieldBinding {
	if binding == nil {
		return nil
	}
	return &domain.VisualFieldBinding{Field: binding.Field}
}

func visualFieldBindingToAPI(binding *domain.VisualFieldBinding) *VisualFieldBinding {
	if binding == nil {
		return nil
	}
	return &VisualFieldBinding{Field: binding.Field}
}
