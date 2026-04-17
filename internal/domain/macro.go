package domain

import "time"

// Macro types.
const (
	MacroTypeScalar = "SCALAR"
	MacroTypeTable  = "TABLE"

	MacroVisibilityProject = "project"

	MacroStatusActive     = "ACTIVE"
	MacroStatusDeprecated = "DEPRECATED"
)

// Macro represents a DuckDB SQL macro definition.
type Macro struct {
	ID          string
	Name        string
	MacroType   string // SCALAR or TABLE
	Parameters  []string
	Body        string
	Description string
	CatalogName string
	ProjectName string
	Visibility  string
	Owner       string
	Properties  map[string]string
	Tags        []string
	Status      string
	CreatedBy   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// MacroRevision captures a point-in-time macro definition snapshot.
type MacroRevision struct {
	ID          string
	MacroID     string
	MacroName   string
	Version     int
	ContentHash string
	Parameters  []string
	Body        string
	Description string
	Status      string
	CreatedBy   string
	CreatedAt   time.Time
}

// MacroRevisionDiff captures the semantic difference between two revisions.
type MacroRevisionDiff struct {
	MacroName          string
	FromVersion        int
	ToVersion          int
	FromContentHash    string
	ToContentHash      string
	Changed            bool
	ParametersChanged  bool
	BodyChanged        bool
	DescriptionChanged bool
	StatusChanged      bool
	FromParameters     []string
	ToParameters       []string
	FromBody           string
	ToBody             string
	FromDescription    string
	ToDescription      string
	FromStatus         string
	ToStatus           string
}

// CreateMacroRequest holds parameters for creating a macro.
type CreateMacroRequest struct {
	Name        string
	MacroType   string
	Parameters  []string
	Body        string
	Description string
	CatalogName string
	ProjectName string
	Visibility  string
	Owner       string
	Properties  map[string]string
	Tags        []string
	Status      string
}

// Validate checks that the request is well-formed.
func (r *CreateMacroRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.Body == "" {
		return ErrValidation("body is required")
	}
	if r.MacroType == "" {
		r.MacroType = MacroTypeScalar
	}
	if r.MacroType != MacroTypeScalar && r.MacroType != MacroTypeTable {
		return ErrValidation("macro_type must be SCALAR or TABLE")
	}
	if r.Visibility == "" {
		r.Visibility = MacroVisibilityProject
	}
	if r.Visibility != MacroVisibilityProject {
		return ErrValidation("visibility must be project")
	}
	if r.ProjectName == "" {
		return ErrValidation("project_name is required")
	}
	if r.Status == "" {
		r.Status = MacroStatusActive
	}
	if r.Status != MacroStatusActive && r.Status != MacroStatusDeprecated {
		return ErrValidation("status must be ACTIVE or DEPRECATED")
	}
	return nil
}

// UpdateMacroRequest holds partial-update parameters.
type UpdateMacroRequest struct {
	Body        *string
	Description *string
	Parameters  []string
	Status      *string
	CatalogName *string
	ProjectName *string
	Visibility  *string
	Owner       *string
	Properties  map[string]string
	Tags        []string
}
