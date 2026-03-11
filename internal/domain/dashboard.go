package domain

import (
	"strings"
	"time"
)

// Dashboard is a persisted dashboard resource.
type Dashboard struct {
	ID          string
	Name        string
	Description string
	Owner       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// DashboardWidgetSourceKind defines where a widget gets its data.
type DashboardWidgetSourceKind string

const (
	// DashboardWidgetSourceSQLQuery uses a direct SQL query as the widget source.
	DashboardWidgetSourceSQLQuery DashboardWidgetSourceKind = "sql_query"
	// DashboardWidgetSourceNotebookCell uses a notebook cell's cached result as the widget source.
	DashboardWidgetSourceNotebookCell DashboardWidgetSourceKind = "notebook_cell"
	// DashboardWidgetSourceSemanticQuery uses semantic query intent as the widget source.
	DashboardWidgetSourceSemanticQuery DashboardWidgetSourceKind = "semantic_query"
)

// DashboardWidgetLayout defines fixed-grid widget positioning.
type DashboardWidgetLayout struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

// DashboardSQLQuerySource represents a direct SQL widget source.
type DashboardSQLQuerySource struct {
	SQL     string  `json:"sql"`
	Catalog *string `json:"catalog,omitempty"`
	Schema  *string `json:"schema,omitempty"`
}

// DashboardNotebookCellSource references a notebook cell's cached output.
type DashboardNotebookCellSource struct {
	NotebookID string `json:"notebook_id"`
	CellID     string `json:"cell_id"`
}

// DashboardSemanticQuerySource represents dashboard semantic intent.
type DashboardSemanticQuerySource struct {
	ProjectName       string   `json:"project_name"`
	SemanticModelName string   `json:"semantic_model_name"`
	Metrics           []string `json:"metrics,omitempty"`
	Dimensions        []string `json:"dimensions,omitempty"`
	Filters           []string `json:"filters,omitempty"`
	OrderBy           []string `json:"order_by,omitempty"`
	Limit             *int     `json:"limit,omitempty"`
	TimeGrain         *string  `json:"time_grain,omitempty"`
}

// DashboardWidgetSource is a discriminated union of widget data sources.
type DashboardWidgetSource struct {
	Kind          DashboardWidgetSourceKind     `json:"kind"`
	SQLQuery      *DashboardSQLQuerySource      `json:"sql_query,omitempty"`
	NotebookCell  *DashboardNotebookCellSource  `json:"notebook_cell,omitempty"`
	SemanticQuery *DashboardSemanticQuerySource `json:"semantic_query,omitempty"`
}

// DashboardWidget is a persisted widget inside a dashboard.
type DashboardWidget struct {
	ID          string
	DashboardID string
	Name        string
	Description string
	Source      DashboardWidgetSource
	VisualSpec  *VisualSpec
	Layout      DashboardWidgetLayout
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateDashboardRequest creates a dashboard.
type CreateDashboardRequest struct {
	Name        string
	Description string
}

// UpdateDashboardRequest applies partial dashboard updates.
type UpdateDashboardRequest struct {
	Name        *string
	Description *string
}

// CreateDashboardWidgetRequest creates a dashboard widget.
type CreateDashboardWidgetRequest struct {
	Name        string
	Description string
	Source      DashboardWidgetSource
	VisualSpec  *VisualSpec
	Layout      DashboardWidgetLayout
}

// UpdateDashboardWidgetRequest applies partial widget updates.
type UpdateDashboardWidgetRequest struct {
	Name        *string
	Description *string
	Source      *DashboardWidgetSource
	VisualSpec  *VisualSpec
	Layout      *DashboardWidgetLayout
}

// Validate validates dashboard creation.
func (r *CreateDashboardRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("dashboard name is required")
	}
	return nil
}

// Validate validates a widget creation request.
func (r *CreateDashboardWidgetRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("widget name is required")
	}
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if r.VisualSpec != nil {
		if err := r.VisualSpec.Validate(); err != nil {
			return err
		}
	}
	return r.Layout.Validate()
}

// Validate validates the widget layout.
func (l DashboardWidgetLayout) Validate() error {
	if l.W < 1 || l.H < 1 {
		return ErrValidation("widget layout width and height must be >= 1")
	}
	if l.X < 0 || l.Y < 0 {
		return ErrValidation("widget layout coordinates must be >= 0")
	}
	return nil
}

// Validate validates the widget source.
func (s DashboardWidgetSource) Validate() error {
	switch s.Kind {
	case DashboardWidgetSourceSQLQuery:
		if s.SQLQuery == nil || strings.TrimSpace(s.SQLQuery.SQL) == "" {
			return ErrValidation("sql_query source requires sql")
		}
	case DashboardWidgetSourceNotebookCell:
		if s.NotebookCell == nil || strings.TrimSpace(s.NotebookCell.NotebookID) == "" || strings.TrimSpace(s.NotebookCell.CellID) == "" {
			return ErrValidation("notebook_cell source requires notebook_id and cell_id")
		}
	case DashboardWidgetSourceSemanticQuery:
		if s.SemanticQuery == nil {
			return ErrValidation("semantic_query source requires semantic query details")
		}
		if strings.TrimSpace(s.SemanticQuery.ProjectName) == "" || strings.TrimSpace(s.SemanticQuery.SemanticModelName) == "" {
			return ErrValidation("semantic_query source requires project_name and semantic_model_name")
		}
		if len(s.SemanticQuery.Metrics) == 0 {
			return ErrValidation("semantic_query source requires at least one metric")
		}
	default:
		return ErrValidation("unsupported widget source kind %q", string(s.Kind))
	}
	return nil
}
