package domain

import (
	"time"
	"unicode/utf8"
)

// Materialization strategy constants.
const (
	MaterializationView        = "VIEW"
	MaterializationTable       = "TABLE"
	MaterializationIncremental = "INCREMENTAL"
	MaterializationEphemeral   = "EPHEMERAL"
	MaxModelNameLength         = 255
)

// Model run status constants.
const (
	ModelRunStatusPending   = "PENDING"
	ModelRunStatusRunning   = "RUNNING"
	ModelRunStatusSuccess   = "SUCCESS"
	ModelRunStatusFailed    = "FAILED"
	ModelRunStatusCancelled = "CANCELLED"
	ModelRunStatusSkipped   = "SKIPPED"
)

// Model trigger type constants.
const (
	ModelTriggerTypeManual    = "MANUAL"
	ModelTriggerTypeScheduled = "SCHEDULED"
	ModelTriggerTypePipeline  = "PIPELINE"
)

// FreshnessPolicy defines staleness thresholds for a model.
type FreshnessPolicy struct {
	MaxLagSeconds int64  `json:"max_lag_seconds,omitempty"`
	CronSchedule  string `json:"cron_schedule,omitempty"`
}

// Model represents a transformation model definition.
type Model struct {
	ID              string
	ProjectName     string // e.g. "sales"
	Name            string // e.g. "stg_orders" — derived from filename
	SQL             string // the transformation SQL
	Materialization string // VIEW, TABLE, INCREMENTAL, EPHEMERAL
	Description     string
	Owner           string
	Tags            []string    // for selector syntax
	DependsOn       []string    // auto-extracted: ["sales.stg_customers", "warehouse.orders"]
	Config          ModelConfig // materialization-specific config
	Contract        *ModelContract
	Freshness       *FreshnessPolicy
	CreatedBy       string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ModelConfig holds materialization-specific configuration.
type ModelConfig struct {
	// For INCREMENTAL: the unique key columns for MERGE.
	UniqueKey []string `json:"unique_key,omitempty"`
	// For INCREMENTAL: the strategy (merge, delete+insert).
	IncrementalStrategy string `json:"incremental_strategy,omitempty"`
}

// QualifiedName returns "project.name" for cross-project references.
func (m *Model) QualifiedName() string {
	return m.ProjectName + "." + m.Name
}

// CreateModelRequest holds parameters for creating a model.
type CreateModelRequest struct {
	ProjectName     string
	Name            string
	SQL             string
	Materialization string
	Description     string
	Tags            []string
	Config          ModelConfig
	Contract        *ModelContract
}

// Validate checks that the request is well-formed.
func (r *CreateModelRequest) Validate() error {
	if r.ProjectName == "" {
		return ErrValidation("project_name is required")
	}
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if utf8.RuneCountInString(r.Name) > MaxModelNameLength {
		return ErrValidation("name must be <= %d characters", MaxModelNameLength)
	}
	if r.SQL == "" {
		return ErrValidation("sql is required")
	}
	validMat := map[string]bool{
		MaterializationView: true, MaterializationTable: true,
		MaterializationIncremental: true, MaterializationEphemeral: true,
	}
	if r.Materialization == "" {
		r.Materialization = MaterializationView
	}
	if !validMat[r.Materialization] {
		return ErrValidation("materialization must be VIEW, TABLE, INCREMENTAL, or EPHEMERAL")
	}
	return nil
}

// UpdateModelRequest holds partial-update parameters.
type UpdateModelRequest struct {
	SQL             *string
	Materialization *string
	Description     *string
	Tags            []string // nil = no change, empty = clear
	Config          *ModelConfig
	Contract        *ModelContract
	Freshness       *FreshnessPolicy
}

// FreshnessStatus holds the result of a freshness check.
type FreshnessStatus struct {
	IsFresh       bool
	LastRunAt     *time.Time
	MaxLagSeconds int64
	StaleSince    *time.Time
}

// ModelRun represents a single execution of the model DAG.
type ModelRun struct {
	ID            string
	Status        string
	TriggerType   string // "MANUAL", "SCHEDULED", "PIPELINE"
	TriggeredBy   string
	TargetCatalog string
	TargetSchema  string
	ModelSelector string // which models: "" = all, "stg_orders+", "tag:finance"
	Variables     map[string]string
	StartedAt     *time.Time
	FinishedAt    *time.Time
	ErrorMessage  *string
	CreatedAt     time.Time
}

// ModelRunStep represents a single model's execution within a run.
type ModelRunStep struct {
	ID           string
	RunID        string
	ModelID      string
	ModelName    string // "project.name" qualified
	Status       string
	Tier         int // DAG tier (0 = roots)
	RowsAffected *int64
	StartedAt    *time.Time
	FinishedAt   *time.Time
	ErrorMessage *string
	CreatedAt    time.Time
}

// ModelRunFilter holds filter parameters for querying model runs.
type ModelRunFilter struct {
	Status *string
	Page   PageRequest
}

// TriggerModelRunRequest holds parameters for triggering a model run.
type TriggerModelRunRequest struct {
	TargetCatalog string
	TargetSchema  string
	Selector      string
	TriggerType   string
	Variables     map[string]string
}

// Validate checks that the request is well-formed.
func (r *TriggerModelRunRequest) Validate() error {
	if r.TargetCatalog == "" {
		return ErrValidation("target_catalog is required")
	}
	if r.TargetSchema == "" {
		return ErrValidation("target_schema is required")
	}
	return nil
}

// ModelTest defines a test assertion for a model's output.
type ModelTest struct {
	ID        string
	ModelID   string
	Name      string
	TestType  string // "not_null", "unique", "accepted_values", "relationships", "custom_sql"
	Column    string // for not_null, unique, accepted_values
	Config    ModelTestConfig
	CreatedAt time.Time
}

// ModelTestConfig holds test-type-specific configuration.
type ModelTestConfig struct {
	Values   []string `json:"values,omitempty"`    // For accepted_values
	ToModel  string   `json:"to_model,omitempty"`  // For relationships
	ToColumn string   `json:"to_column,omitempty"` // For relationships
	SQL      string   `json:"sql,omitempty"`       // For custom_sql
}

// ModelTestResult represents the result of a test execution.
type ModelTestResult struct {
	ID           string
	RunStepID    string
	TestID       string
	TestName     string
	Status       string // "PASS", "FAIL", "ERROR"
	RowsReturned *int64
	ErrorMessage *string
	CreatedAt    time.Time
}

// Model test result status constants.
const (
	TestResultPass  = "PASS"
	TestResultFail  = "FAIL"
	TestResultError = "ERROR"
)

// Valid test type constants.
const (
	TestTypeNotNull        = "not_null"
	TestTypeUnique         = "unique"
	TestTypeAcceptedValues = "accepted_values"
	TestTypeRelationships  = "relationships"
	TestTypeCustomSQL      = "custom_sql"
)

// CreateModelTestRequest holds parameters for creating a model test.
type CreateModelTestRequest struct {
	Name     string
	TestType string
	Column   string
	Config   ModelTestConfig
}

// Validate checks that the request is well-formed.
func (r *CreateModelTestRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	validTypes := map[string]bool{
		TestTypeNotNull: true, TestTypeUnique: true,
		TestTypeAcceptedValues: true, TestTypeRelationships: true,
		TestTypeCustomSQL: true,
	}
	if !validTypes[r.TestType] {
		return ErrValidation("test_type must be not_null, unique, accepted_values, relationships, or custom_sql")
	}
	switch r.TestType {
	case TestTypeNotNull, TestTypeUnique:
		if r.Column == "" {
			return ErrValidation("column is required for %s tests", r.TestType)
		}
	case TestTypeAcceptedValues:
		if r.Column == "" {
			return ErrValidation("column is required for accepted_values tests")
		}
		if len(r.Config.Values) == 0 {
			return ErrValidation("values are required for accepted_values tests")
		}
	case TestTypeRelationships:
		if r.Column == "" {
			return ErrValidation("column is required for relationships tests")
		}
		if r.Config.ToModel == "" || r.Config.ToColumn == "" {
			return ErrValidation("to_model and to_column are required for relationships tests")
		}
	case TestTypeCustomSQL:
		if r.Config.SQL == "" {
			return ErrValidation("sql is required for custom_sql tests")
		}
	}
	return nil
}

// ModelContract defines enforced output column types.
type ModelContract struct {
	Enforce bool                  `json:"enforce"`
	Columns []ModelContractColumn `json:"columns,omitempty"`
}

// ModelContractColumn defines an expected column in a model contract.
type ModelContractColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}
