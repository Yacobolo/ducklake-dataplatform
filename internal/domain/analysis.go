package domain

import "time"

// DiagnosticSeverity classifies compile diagnostics.
type DiagnosticSeverity string

const (
	// DiagnosticSeverityInfo is an informational diagnostic.
	DiagnosticSeverityInfo DiagnosticSeverity = "INFO"
	// DiagnosticSeverityWarning is a warning diagnostic.
	DiagnosticSeverityWarning DiagnosticSeverity = "WARNING"
	// DiagnosticSeverityError is an error diagnostic.
	DiagnosticSeverityError DiagnosticSeverity = "ERROR"
)

// CompileDiagnosticLocation identifies an approximate source location.
type CompileDiagnosticLocation struct {
	Line   int `json:"line,omitempty"`
	Column int `json:"column,omitempty"`
}

// CompileDiagnostic is a machine-readable compile or analysis diagnostic.
type CompileDiagnostic struct {
	Severity       DiagnosticSeverity         `json:"severity"`
	Code           string                     `json:"code"`
	Message        string                     `json:"message"`
	ModelName      string                     `json:"model_name,omitempty"`
	ColumnName     string                     `json:"column_name,omitempty"`
	Location       *CompileDiagnosticLocation `json:"location,omitempty"`
	RelatedObjects []string                   `json:"related_objects,omitempty"`
}

// ColumnLineageSourceRef identifies a single upstream source column.
type ColumnLineageSourceRef struct {
	Catalog   string `json:"catalog,omitempty"`
	Schema    string `json:"schema,omitempty"`
	Table     string `json:"table,omitempty"`
	Column    string `json:"column,omitempty"`
	Kind      string `json:"kind,omitempty"`
	ModelName string `json:"model_name,omitempty"`
}

// ColumnSensitivityInfo captures advisory sensitive-data propagation metadata.
type ColumnSensitivityInfo struct {
	Status       string                   `json:"status,omitempty"`
	Partial      bool                     `json:"partial,omitempty"`
	Reasons      []string                 `json:"reasons,omitempty"`
	SourceFields []ColumnLineageSourceRef `json:"source_fields,omitempty"`
}

// CompiledColumnLineage describes one analyzed target column for a build/model.
type CompiledColumnLineage struct {
	BuildID       string                   `json:"build_id,omitempty"`
	CompilationID string                   `json:"compilation_id,omitempty"`
	ProjectName   string                   `json:"project_name,omitempty"`
	ModelName     string                   `json:"model_name,omitempty"`
	TargetCatalog string                   `json:"target_catalog,omitempty"`
	TargetSchema  string                   `json:"target_schema,omitempty"`
	TargetTable   string                   `json:"target_table,omitempty"`
	TargetColumn  string                   `json:"target_column"`
	TransformType TransformType            `json:"transform_type,omitempty"`
	Function      string                   `json:"function,omitempty"`
	Partial       bool                     `json:"partial,omitempty"`
	Sources       []ColumnLineageSourceRef `json:"sources,omitempty"`
	Sensitivity   *ColumnSensitivityInfo   `json:"sensitivity,omitempty"`
}

// BuildSourceStateSnapshot captures source freshness/data state at compile time.
type BuildSourceStateSnapshot struct {
	SourceKey         string     `json:"source_key"`
	RelationRef       string     `json:"relation_ref,omitempty"`
	TimestampColumn   string     `json:"timestamp_column,omitempty"`
	LastLoadedAt      *time.Time `json:"last_loaded_at,omitempty"`
	MaxLagSeconds     int64      `json:"max_lag_seconds,omitempty"`
	FreshnessBreached bool       `json:"freshness_breached,omitempty"`
	StaleSince        *time.Time `json:"stale_since,omitempty"`
}

// BuildStateSnapshot stores build-time signals used for later planning.
type BuildStateSnapshot struct {
	Version         int                        `json:"version"`
	ProjectName     string                     `json:"project_name,omitempty"`
	EnvironmentName string                     `json:"environment_name,omitempty"`
	Sources         []BuildSourceStateSnapshot `json:"sources,omitempty"`
}

// RebuildReason identifies why a model should be selected for rebuild.
type RebuildReason string

const (
	// RebuildReasonCodeModified indicates the model changed directly.
	RebuildReasonCodeModified RebuildReason = "CODE_MODIFIED"
	// RebuildReasonUpstreamDataChanged indicates upstream data changed.
	RebuildReasonUpstreamDataChanged RebuildReason = "UPSTREAM_DATA_CHANGED"
	// RebuildReasonFreshnessBreached indicates source freshness is breached.
	RebuildReasonFreshnessBreached RebuildReason = "SOURCE_FRESHNESS_BREACHED"
	// RebuildReasonUpstreamCodeChanged indicates upstream code changed.
	RebuildReasonUpstreamCodeChanged RebuildReason = "UPSTREAM_CODE_CHANGED"
)

// RebuildPlanItem is one selected model in a rebuild plan.
type RebuildPlanItem struct {
	ModelName string          `json:"model_name"`
	Reasons   []RebuildReason `json:"reasons,omitempty"`
}

// RebuildPlan is the machine-readable selection result for a project/environment.
type RebuildPlan struct {
	ProjectName     string            `json:"project_name"`
	EnvironmentName string            `json:"environment_name"`
	BaselineBuildID *string           `json:"baseline_build_id,omitempty"`
	SelectedModels  []RebuildPlanItem `json:"selected_models,omitempty"`
	UnchangedModels []string          `json:"unchanged_models,omitempty"`
}

// PlanRebuildRequest requests a code+data-aware rebuild plan.
type PlanRebuildRequest struct {
	ProjectName     string `json:"project_name"`
	EnvironmentName string `json:"environment_name"`
	Selector        string `json:"selector,omitempty"`
}

// BuildCompareModelDiff summarizes changes for one model across builds.
type BuildCompareModelDiff struct {
	ModelName        string   `json:"model_name"`
	ChangeType       string   `json:"change_type,omitempty"`
	FromCompiledHash string   `json:"from_compiled_hash,omitempty"`
	ToCompiledHash   string   `json:"to_compiled_hash,omitempty"`
	AddedColumns     []string `json:"added_columns,omitempty"`
	RemovedColumns   []string `json:"removed_columns,omitempty"`
	ChangedColumns   []string `json:"changed_columns,omitempty"`
	ImpactedModels   []string `json:"impacted_models,omitempty"`
	ImpactedTests    []string `json:"impacted_tests,omitempty"`
	ImpactedProducts []string `json:"impacted_products,omitempty"`
}

// BuildCompareResult is a machine-readable comparison between two build states.
type BuildCompareResult struct {
	ProjectName        string                  `json:"project_name,omitempty"`
	FromBuildID        string                  `json:"from_build_id"`
	ToBuildID          *string                 `json:"to_build_id,omitempty"`
	ComparedToHead     bool                    `json:"compared_to_head,omitempty"`
	ModelDiffs         []BuildCompareModelDiff `json:"model_diffs,omitempty"`
	DiagnosticsAdded   []CompileDiagnostic     `json:"diagnostics_added,omitempty"`
	DiagnosticsRemoved []CompileDiagnostic     `json:"diagnostics_removed,omitempty"`
}

// CompareBuildsRequest requests a machine-readable diff between builds or head.
type CompareBuildsRequest struct {
	ProjectName   string  `json:"project_name,omitempty"`
	FromBuildID   string  `json:"from_build_id"`
	ToBuildID     *string `json:"to_build_id,omitempty"`
	CompareToHead bool    `json:"compare_to_head,omitempty"`
}

// BuildImpactResult summarizes downstream impact for a model/source/macro change.
type BuildImpactResult struct {
	ProjectName      string                  `json:"project_name,omitempty"`
	Kind             string                  `json:"kind"`
	Key              string                  `json:"key"`
	BuildID          *string                 `json:"build_id,omitempty"`
	ImpactedModels   []string                `json:"impacted_models,omitempty"`
	ImpactedColumns  []CompiledColumnLineage `json:"impacted_columns,omitempty"`
	ImpactedTests    []string                `json:"impacted_tests,omitempty"`
	ImpactedProducts []string                `json:"impacted_products,omitempty"`
	Partial          bool                    `json:"partial,omitempty"`
}

// BuildDiagnosticsFilter scopes build diagnostic reads.
type BuildDiagnosticsFilter struct {
	ModelName *string
	Severity  *DiagnosticSeverity
	Code      *string
}
