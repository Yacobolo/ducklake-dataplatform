package domain

import "strings"

// VisualOutputKind controls how a result should be rendered.
type VisualOutputKind string

const (
	// VisualOutputTable renders the result as a table.
	VisualOutputTable VisualOutputKind = "table"
	// VisualOutputMetric renders the result as a KPI-style metric.
	VisualOutputMetric VisualOutputKind = "metric"
	// VisualOutputChart renders the result as a chart.
	VisualOutputChart VisualOutputKind = "chart"
)

// VisualChartType defines supported chart renderers for structured visuals.
type VisualChartType string

const (
	// VisualChartBar renders a bar chart.
	VisualChartBar VisualChartType = "bar"
	// VisualChartLine renders a line chart.
	VisualChartLine VisualChartType = "line"
	// VisualChartArea renders an area chart.
	VisualChartArea VisualChartType = "area"
	// VisualChartPie renders a pie chart.
	VisualChartPie VisualChartType = "pie"
	// VisualChartDoughnut renders a doughnut chart.
	VisualChartDoughnut VisualChartType = "doughnut"
	// VisualChartScatter renders a scatter plot.
	VisualChartScatter VisualChartType = "scatter"
	// VisualChartStackedBar renders a stacked bar chart.
	VisualChartStackedBar VisualChartType = "stacked_bar"
)

// VisualFieldBinding maps semantic meaning to a result column name.
type VisualFieldBinding struct {
	Field string `json:"field"`
}

// VisualEncodings maps result columns to visual channels.
type VisualEncodings struct {
	X         *VisualFieldBinding `json:"x,omitempty"`
	Y         *VisualFieldBinding `json:"y,omitempty"`
	Series    *VisualFieldBinding `json:"series,omitempty"`
	Label     *VisualFieldBinding `json:"label,omitempty"`
	Value     *VisualFieldBinding `json:"value,omitempty"`
	Secondary *VisualFieldBinding `json:"secondary,omitempty"`
}

// VisualSpec is the product-owned visualization contract used by notebooks and dashboards.
type VisualSpec struct {
	Kind         VisualOutputKind `json:"kind"`
	ChartType    *VisualChartType `json:"chart_type,omitempty"`
	Encodings    VisualEncodings  `json:"encodings,omitempty"`
	Title        string           `json:"title,omitempty"`
	Subtitle     string           `json:"subtitle,omitempty"`
	Legend       *bool            `json:"legend,omitempty"`
	Stacked      *bool            `json:"stacked,omitempty"`
	ColorPalette string           `json:"color_palette,omitempty"`
}

// Validate checks that the visualization spec is structurally sound.
func (s *VisualSpec) Validate() error {
	if s == nil {
		return nil
	}
	switch s.Kind {
	case VisualOutputTable:
		return nil
	case VisualOutputMetric:
		if s.Encodings.Value == nil || strings.TrimSpace(s.Encodings.Value.Field) == "" {
			return ErrValidation("metric visuals require a value encoding")
		}
		return nil
	case VisualOutputChart:
		if s.ChartType == nil {
			return ErrValidation("chart visuals require chart_type")
		}
		switch *s.ChartType {
		case VisualChartBar, VisualChartLine, VisualChartArea, VisualChartStackedBar:
			if err := requireVisualField("x", s.Encodings.X); err != nil {
				return err
			}
			if err := requireVisualField("y", s.Encodings.Y); err != nil {
				return err
			}
			return nil
		case VisualChartScatter:
			if err := requireVisualField("x", s.Encodings.X); err != nil {
				return err
			}
			if err := requireVisualField("y", s.Encodings.Y); err != nil {
				return err
			}
			return nil
		case VisualChartPie, VisualChartDoughnut:
			if err := requireVisualField("label", s.Encodings.Label); err != nil {
				return err
			}
			if err := requireVisualField("value", s.Encodings.Value); err != nil {
				return err
			}
			return nil
		default:
			return ErrValidation("unsupported chart_type %q", string(*s.ChartType))
		}
	default:
		return ErrValidation("unsupported visual kind %q", string(s.Kind))
	}
}

// ValidateColumns checks that all referenced fields exist in the result set.
func (s *VisualSpec) ValidateColumns(columns []string) error {
	if s == nil {
		return nil
	}
	if err := s.Validate(); err != nil {
		return err
	}

	colSet := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		colSet[col] = struct{}{}
	}

	for _, binding := range []*VisualFieldBinding{
		s.Encodings.X,
		s.Encodings.Y,
		s.Encodings.Series,
		s.Encodings.Label,
		s.Encodings.Value,
		s.Encodings.Secondary,
	} {
		if binding == nil {
			continue
		}
		field := strings.TrimSpace(binding.Field)
		if field == "" {
			return ErrValidation("visual field bindings must not be empty")
		}
		if _, ok := colSet[field]; !ok {
			return ErrValidation("visual field %q does not exist in the result columns", field)
		}
	}

	return nil
}

func requireVisualField(name string, binding *VisualFieldBinding) error {
	if binding == nil || strings.TrimSpace(binding.Field) == "" {
		return ErrValidation("%s encoding is required", name)
	}
	return nil
}
