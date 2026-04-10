package domain

import (
	"strings"
	"time"
	"unicode"
)

// DefaultDashboardPageName is used when a widget or request does not specify a page.
const DefaultDashboardPageName = "Overview"

// Dashboard is a persisted dashboard resource.
type Dashboard struct {
	ID                  string
	Name                string
	Description         string
	Owner               string
	FolderID            string
	SemanticProjectName string
	SemanticModelName   string
	Compute             DashboardComputePolicy
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// DashboardComputePolicy defines how dashboard reads are routed.
type DashboardComputePolicy struct {
	Mode          string `json:"mode,omitempty" yaml:"mode,omitempty"`
	EndpointName  string `json:"endpoint_name,omitempty" yaml:"endpoint_name,omitempty"`
	FallbackLocal bool   `json:"fallback_local,omitempty" yaml:"fallback_local,omitempty"`
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
	X int `json:"x" yaml:"x"`
	Y int `json:"y" yaml:"y"`
	W int `json:"w" yaml:"w"`
	H int `json:"h" yaml:"h"`
}

// DashboardSQLQuerySource represents a direct SQL widget source.
type DashboardSQLQuerySource struct {
	SQL     string  `json:"sql" yaml:"sql"`
	Catalog *string `json:"catalog,omitempty" yaml:"catalog,omitempty"`
	Schema  *string `json:"schema,omitempty" yaml:"schema,omitempty"`
}

// DashboardNotebookCellSource references a notebook cell's cached output.
type DashboardNotebookCellSource struct {
	NotebookID string `json:"notebook_id"`
	CellID     string `json:"cell_id"`
}

// DashboardSemanticQuerySource represents dashboard semantic intent.
type DashboardSemanticQuerySource struct {
	ProjectName       string   `json:"project_name,omitempty"`
	SemanticModelName string   `json:"semantic_model_name,omitempty"`
	SemanticModelID   string   `json:"semantic_model_id,omitempty"`
	Metrics           []string `json:"metrics,omitempty"`
	RelationshipNames []string `json:"relationship_names,omitempty"`
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
	ID              string
	DashboardID     string
	FilterOriginKey string
	PageName        string
	Name            string
	Description     string
	Source          DashboardWidgetSource
	VisualSpec      *VisualSpec
	Layout          DashboardWidgetLayout
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// CreateDashboardRequest creates a dashboard.
type CreateDashboardRequest struct {
	Owner               string
	Name                string
	Description         string
	FolderID            *string
	SemanticProjectName string
	SemanticModelName   string
	Compute             *DashboardComputePolicy
}

// UpdateDashboardRequest applies partial dashboard updates.
type UpdateDashboardRequest struct {
	Owner               *string
	Name                *string
	Description         *string
	FolderID            *string
	SemanticProjectName *string
	SemanticModelName   *string
	Compute             *DashboardComputePolicy
}

// CreateDashboardWidgetRequest creates a dashboard widget.
type CreateDashboardWidgetRequest struct {
	FilterOriginKey string
	PageName        string
	Name            string
	Description     string
	Source          DashboardWidgetSource
	VisualSpec      *VisualSpec
	Layout          DashboardWidgetLayout
}

// UpdateDashboardWidgetRequest applies partial widget updates.
type UpdateDashboardWidgetRequest struct {
	FilterOriginKey *string
	PageName        *string
	Name            *string
	Description     *string
	Source          *DashboardWidgetSource
	VisualSpec      *VisualSpec
	Layout          *DashboardWidgetLayout
}

// Validate validates dashboard creation.
func (r *CreateDashboardRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("dashboard name is required")
	}
	if owner := strings.TrimSpace(r.Owner); owner != "" {
		r.Owner = owner
	}
	if err := ValidateDashboardSemanticBinding(r.SemanticProjectName, r.SemanticModelName); err != nil {
		return err
	}
	if r.Compute != nil {
		if err := r.Compute.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Normalize returns a copy with defaulted and canonicalized compute policy fields.
func (p DashboardComputePolicy) Normalize() DashboardComputePolicy {
	req := ComputeExecutionRequest{
		Mode:         p.Mode,
		EndpointName: p.EndpointName,
	}
	req = req.Normalize()
	if req.Mode == "" {
		req.Mode = ComputeModeAuto
	}
	return DashboardComputePolicy{
		Mode:          req.Mode,
		EndpointName:  req.EndpointName,
		FallbackLocal: p.FallbackLocal,
	}
}

// Validate validates the dashboard compute policy.
func (p DashboardComputePolicy) Validate() error {
	norm := p.Normalize()
	switch norm.Mode {
	case ComputeModeAuto:
		if norm.EndpointName != "" {
			return ErrValidation("endpoint_name cannot be set when dashboard compute mode is AUTO")
		}
		if norm.FallbackLocal {
			return ErrValidation("fallback_local cannot be set when dashboard compute mode is AUTO")
		}
	case ComputeModeByocLocal:
		if norm.EndpointName != "" {
			return ErrValidation("endpoint_name cannot be set when dashboard compute mode is BYOC_LOCAL")
		}
		if norm.FallbackLocal {
			return ErrValidation("fallback_local cannot be set when dashboard compute mode is BYOC_LOCAL")
		}
	case ComputeModeSharedEndpoint:
		if norm.EndpointName == "" {
			return ErrValidation("endpoint_name is required when dashboard compute mode is SHARED_ENDPOINT")
		}
	default:
		return ErrValidation("dashboard compute mode must be AUTO, BYOC_LOCAL, or SHARED_ENDPOINT, got %q", p.Mode)
	}
	return nil
}

// ValidateDashboardSemanticBinding validates the optional dashboard-level semantic model binding.
func ValidateDashboardSemanticBinding(projectName, modelName string) error {
	projectName = strings.TrimSpace(projectName)
	modelName = strings.TrimSpace(modelName)
	if (projectName == "") != (modelName == "") {
		return ErrValidation("dashboard semantic binding requires both project_name and semantic_model_name")
	}
	return nil
}

// Validate validates a widget creation request.
func (r *CreateDashboardWidgetRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("widget name is required")
	}
	if strings.TrimSpace(r.FilterOriginKey) != "" {
		if err := ValidateDashboardWidgetFilterOriginKey(r.FilterOriginKey); err != nil {
			return err
		}
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

// NormalizeDashboardPageName trims dashboard page names and applies the default when blank.
func NormalizeDashboardPageName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return DefaultDashboardPageName
	}
	return name
}

// ValidateDashboardWidgetFilterOriginKey validates the stable widget key used for interaction routing.
func ValidateDashboardWidgetFilterOriginKey(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrValidation("widget key is required")
	}
	for _, r := range key {
		switch {
		case unicode.IsLower(r), unicode.IsDigit(r), r == '-':
		default:
			return ErrValidation("widget key must contain only lowercase letters, digits, and hyphens")
		}
	}
	return nil
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
		if strings.TrimSpace(s.SemanticQuery.SemanticModelID) == "" && strings.TrimSpace(s.SemanticQuery.SemanticModelName) == "" {
			return ErrValidation("semantic_query source requires semantic_model_id or semantic_model_name")
		}
		if len(s.SemanticQuery.Metrics) == 0 {
			return ErrValidation("semantic_query source requires at least one metric")
		}
	default:
		return ErrValidation("unsupported widget source kind %q", string(s.Kind))
	}
	return nil
}
