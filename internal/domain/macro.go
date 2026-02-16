package domain

import "time"

// Macro types.
const (
	MacroTypeScalar = "SCALAR"
	MacroTypeTable  = "TABLE"
)

// Macro represents a DuckDB SQL macro definition.
type Macro struct {
	ID          string
	Name        string
	MacroType   string // SCALAR or TABLE
	Parameters  []string
	Body        string
	Description string
	CreatedBy   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateMacroRequest holds parameters for creating a macro.
type CreateMacroRequest struct {
	Name        string
	MacroType   string
	Parameters  []string
	Body        string
	Description string
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
	return nil
}

// UpdateMacroRequest holds partial-update parameters.
type UpdateMacroRequest struct {
	Body        *string
	Description *string
	Parameters  []string
}

// PromoteNotebookRequest holds parameters for promoting a notebook cell to a model.
type PromoteNotebookRequest struct {
	NotebookID      string
	CellIndex       int
	ProjectName     string
	Name            string
	Materialization string
}

// Validate checks that the request is well-formed.
func (r *PromoteNotebookRequest) Validate() error {
	if r.NotebookID == "" {
		return ErrValidation("notebook_id is required")
	}
	if r.ProjectName == "" {
		return ErrValidation("project_name is required")
	}
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.CellIndex < 0 {
		return ErrValidation("cell_index must be non-negative")
	}
	if r.Materialization == "" {
		r.Materialization = MaterializationTable
	}
	validMat := map[string]bool{
		MaterializationView: true, MaterializationTable: true,
		MaterializationIncremental: true, MaterializationEphemeral: true,
	}
	if !validMat[r.Materialization] {
		return ErrValidation("materialization must be VIEW, TABLE, INCREMENTAL, or EPHEMERAL")
	}
	return nil
}
