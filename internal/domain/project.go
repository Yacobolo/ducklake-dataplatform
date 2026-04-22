package domain

import (
	"strings"
	"time"
)

const (
	// ProjectKindPersonal is a per-user draft authoring workspace.
	ProjectKindPersonal = "personal"
	// ProjectKindShared is a team-owned authoring workspace that can back products.
	ProjectKindShared = "shared"
	// ProjectKindTransform is the canonical project kind for transformation authoring.
	ProjectKindTransform = "transform"
	// ProjectKindLibrary is a reusable compile-time workspace for shared macros and checks.
	ProjectKindLibrary = "library"
)

const (
	// EnvironmentKindDevelopment is the non-production execution context for authoring.
	EnvironmentKindDevelopment = "development"
	// EnvironmentKindStaging is the pre-production execution context for shared validation.
	EnvironmentKindStaging = "staging"
	// EnvironmentKindProduction is the production execution context for released builds.
	EnvironmentKindProduction = "production"
)

const (
	// BuildStateDraft is an internal placeholder state before a build is ready.
	BuildStateDraft = "draft"
	// BuildStateReady means a build can be used for validation or publication.
	BuildStateReady = "ready"
)

// Project is an internal execution/build unit within a workspace.
type Project struct {
	ID             string
	WorkspaceID    string
	Name           string
	Kind           string
	Description    string
	OwnerGroupID   *string
	OwnerPrincipal *string
	DefaultBranch  string
	CreatedBy      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateProjectRequest defines the input for creating an internal authoring project.
type CreateProjectRequest struct {
	WorkspaceID    string
	Name           string
	Kind           string
	Description    string
	OwnerGroupID   *string
	OwnerPrincipal *string
	DefaultBranch  string
}

// UpdateProjectRequest defines mutable project fields.
type UpdateProjectRequest struct {
	Description   *string
	DefaultBranch *string
}

// Environment is an internal execution context for a project.
type Environment struct {
	ID                 string
	ProjectID          string
	ProjectName        string
	Name               string
	Kind               string
	Description        string
	TargetCatalog      string
	TargetSchema       string
	ComputeEndpoint    *string
	DeferToEnvironment *string
	Variables          map[string]string
	SourceOverrides    map[string]string
	CreatedBy          string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// ProjectDependency declares that a project may compile against another project.
// Library/shared projects act as the package mechanism for transformation authoring.
type ProjectDependency struct {
	ID                  string
	ProjectID           string
	ProjectName         string
	DependencyProjectID string
	DependencyProject   string
	DependencyKind      string
	VersionConstraint   string
	ResolvedReleaseID   *string
	Position            int
	CreatedBy           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// SourceFreshnessPolicy defines freshness thresholds for a registered source.
type SourceFreshnessPolicy struct {
	TimestampColumn string `json:"timestamp_column,omitempty"`
	MaxLagSeconds   int64  `json:"max_lag_seconds,omitempty"`
}

// SourceDefinition is a first-class project-owned source declaration.
type SourceDefinition struct {
	ID          string
	ProjectName string
	SourceName  string
	TableName   string
	RelationRef string
	Description string
	Freshness   *SourceFreshnessPolicy
	CreatedBy   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// CreateProjectDependencyRequest defines the input for attaching a dependency project.
type CreateProjectDependencyRequest struct {
	DependencyProjectID string
	DependencyProject   string
	DependencyKind      string
	VersionConstraint   string
	Position            int
}

// CreateSourceDefinitionRequest defines the input for creating a project-owned source.
type CreateSourceDefinitionRequest struct {
	SourceName  string
	TableName   string
	RelationRef string
	Description string
	Freshness   *SourceFreshnessPolicy
}

// UpdateSourceDefinitionRequest defines mutable source fields.
type UpdateSourceDefinitionRequest struct {
	RelationRef *string
	Description *string
	Freshness   *SourceFreshnessPolicy
}

// CreateEnvironmentRequest defines the input for creating a project execution environment.
type CreateEnvironmentRequest struct {
	Name               string
	Kind               string
	Description        string
	TargetCatalog      string
	TargetSchema       string
	ComputeEndpoint    *string
	DeferToEnvironment *string
	Variables          map[string]string
	SourceOverrides    map[string]string
}

// UpdateEnvironmentRequest defines mutable environment fields.
type UpdateEnvironmentRequest struct {
	Description        *string
	TargetCatalog      *string
	TargetSchema       *string
	ComputeEndpoint    *string
	DeferToEnvironment *string
	Variables          *map[string]string
	SourceOverrides    *map[string]string
}

// Build is an internal immutable compilation snapshot.
type Build struct {
	ID                 string
	ProjectID          string
	ProjectName        string
	EnvironmentID      string
	EnvironmentName    string
	State              string
	GitRef             string
	CommitSHA          *string
	Selector           string
	TargetCatalog      string
	TargetSchema       string
	SourceModelRunID   *string
	ResolvedReleaseID  *string
	CompileManifest    string
	CompileDiagnostics *string
	StateSnapshot      *string
	CreatedBy          string
	CreatedAt          time.Time
}

// Compilation is an immutable environment-scoped compile artifact for preview and analysis.
type Compilation struct {
	ID                 string
	ProjectID          string
	ProjectName        string
	EnvironmentID      string
	EnvironmentName    string
	GitRef             string
	CommitSHA          *string
	Selector           string
	TargetCatalog      string
	TargetSchema       string
	ResolvedReleaseID  *string
	CompileManifest    string
	CompileDiagnostics *string
	StateSnapshot      *string
	CreatedBy          string
	CreatedAt          time.Time
}

// CreateCompilationRequest defines the input for creating a compilation artifact.
type CreateCompilationRequest struct {
	GitRef        string
	CommitSHA     *string
	Selector      string
	TargetCatalog string
	TargetSchema  string
}

// ProjectRelease is an immutable published snapshot for dependency resolution.
type ProjectRelease struct {
	ID                string
	ProjectID         string
	ProjectName       string
	Version           string
	ResolvedBuildID   *string
	ResolvedCompileID *string
	Snapshot          *ProjectReleaseSnapshot
	CreatedBy         string
	CreatedAt         time.Time
}

// ProjectReleaseSnapshot captures immutable authoring resources for a released project.
type ProjectReleaseSnapshot struct {
	ProjectName string             `json:"project_name,omitempty"`
	Kind        string             `json:"kind,omitempty"`
	Models      []Model            `json:"models,omitempty"`
	Macros      []Macro            `json:"macros,omitempty"`
	Sources     []SourceDefinition `json:"sources,omitempty"`
	Seeds       []Seed             `json:"seeds,omitempty"`
}

// CreateProjectReleaseRequest defines the input for creating a project release.
type CreateProjectReleaseRequest struct {
	Version         string
	ResolvedBuildID *string
	CompilationID   *string
}

// CreateBuildRequest defines the input for creating an immutable project build.
type CreateBuildRequest struct {
	EnvironmentName    string
	GitRef             string
	CommitSHA          *string
	Selector           string
	TargetCatalog      string
	TargetSchema       string
	SourceModelRunID   *string
	CompileManifest    string
	CompileDiagnostics *string
	StateSnapshot      *string
}

// Validate validates a project creation request.
func (r *CreateProjectRequest) Validate() error {
	return ValidateCreateProjectRequest(*r)
}

// ValidateCreateProjectRequest validates a project creation request.
func ValidateCreateProjectRequest(req CreateProjectRequest) error {
	if strings.TrimSpace(req.WorkspaceID) == "" {
		return ErrValidation("workspace_id is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		return ErrValidation("project name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("project name must be at most 128 characters")
	}
	switch normalizeProjectKind(req.Kind) {
	case ProjectKindPersonal, ProjectKindShared, ProjectKindTransform, ProjectKindLibrary:
	default:
		return ErrValidation("unsupported project kind %q", req.Kind)
	}
	if strings.TrimSpace(defaultProjectBranch(req.DefaultBranch)) == "" {
		return ErrValidation("default branch is required")
	}

	return nil
}

// ValidateUpdateProjectRequest validates a project update request.
func ValidateUpdateProjectRequest(req UpdateProjectRequest) error {
	if req.DefaultBranch != nil && strings.TrimSpace(*req.DefaultBranch) == "" {
		return ErrValidation("default branch cannot be empty")
	}
	return nil
}

// Validate validates a dependency creation request.
func (r *CreateProjectDependencyRequest) Validate() error {
	if strings.TrimSpace(r.DependencyProjectID) == "" && strings.TrimSpace(r.DependencyProject) == "" {
		return ErrValidation("dependency_project_id is required")
	}
	return nil
}

// Validate validates a compilation creation request.
func (r *CreateCompilationRequest) Validate() error {
	if strings.TrimSpace(r.GitRef) == "" {
		return ErrValidation("git_ref is required")
	}
	return nil
}

// Validate validates a project release creation request.
func (r *CreateProjectReleaseRequest) Validate() error {
	if strings.TrimSpace(r.Version) == "" {
		return ErrValidation("version is required")
	}
	if trimmedPtr(r.ResolvedBuildID) == nil && trimmedPtr(r.CompilationID) == nil {
		return ErrValidation("resolved_build_id or compilation_id is required")
	}
	return nil
}

// Validate validates a source definition creation request.
func (r *CreateSourceDefinitionRequest) Validate() error {
	if strings.TrimSpace(r.SourceName) == "" {
		return ErrValidation("source_name is required")
	}
	if strings.TrimSpace(r.TableName) == "" {
		return ErrValidation("table_name is required")
	}
	if strings.TrimSpace(r.RelationRef) == "" {
		return ErrValidation("relation_ref is required")
	}
	return nil
}

// Validate validates a source definition update request.
func (r *UpdateSourceDefinitionRequest) Validate() error {
	if r.RelationRef != nil && strings.TrimSpace(*r.RelationRef) == "" {
		return ErrValidation("relation_ref cannot be empty")
	}
	if r.Description != nil && strings.TrimSpace(*r.Description) == "" {
		return ErrValidation("description cannot be empty")
	}
	return nil
}

// Validate validates an environment creation request.
func (r *CreateEnvironmentRequest) Validate() error {
	return ValidateCreateEnvironmentRequest(*r)
}

// ValidateCreateEnvironmentRequest validates an environment creation request.
func ValidateCreateEnvironmentRequest(req CreateEnvironmentRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return ErrValidation("environment name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("environment name must be at most 128 characters")
	}
	switch normalizeEnvironmentKind(req.Kind) {
	case EnvironmentKindDevelopment, EnvironmentKindStaging, EnvironmentKindProduction:
	default:
		return ErrValidation("unsupported environment kind %q", req.Kind)
	}
	if strings.TrimSpace(req.TargetCatalog) == "" {
		return ErrValidation("target catalog is required")
	}
	if strings.TrimSpace(req.TargetSchema) == "" {
		return ErrValidation("target schema is required")
	}
	return nil
}

// ValidateUpdateEnvironmentRequest validates an environment update request.
func ValidateUpdateEnvironmentRequest(req UpdateEnvironmentRequest) error {
	if req.TargetCatalog != nil && strings.TrimSpace(*req.TargetCatalog) == "" {
		return ErrValidation("target catalog cannot be empty")
	}
	if req.TargetSchema != nil && strings.TrimSpace(*req.TargetSchema) == "" {
		return ErrValidation("target schema cannot be empty")
	}
	return nil
}

// Validate validates a build creation request.
func (r *CreateBuildRequest) Validate() error {
	return ValidateCreateBuildRequest(*r)
}

// ValidateCreateBuildRequest validates a build creation request.
func ValidateCreateBuildRequest(req CreateBuildRequest) error {
	if strings.TrimSpace(req.EnvironmentName) == "" {
		return ErrValidation("environment_name is required")
	}
	if strings.TrimSpace(req.GitRef) == "" {
		return ErrValidation("git_ref is required")
	}
	if strings.TrimSpace(req.TargetCatalog) == "" {
		return ErrValidation("target catalog is required")
	}
	if strings.TrimSpace(req.TargetSchema) == "" {
		return ErrValidation("target schema is required")
	}
	if strings.TrimSpace(req.CompileManifest) == "" {
		return ErrValidation("compile_manifest is required")
	}
	return nil
}

func normalizeProjectKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", ProjectKindShared:
		return ProjectKindShared
	case ProjectKindPersonal:
		return ProjectKindPersonal
	case ProjectKindLibrary:
		return ProjectKindLibrary
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func normalizeEnvironmentKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", EnvironmentKindDevelopment:
		return EnvironmentKindDevelopment
	case EnvironmentKindStaging:
		return EnvironmentKindStaging
	case EnvironmentKindProduction:
		return EnvironmentKindProduction
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func defaultProjectBranch(branch string) string {
	if strings.TrimSpace(branch) == "" {
		return "main"
	}
	return strings.TrimSpace(branch)
}

func trimmedPtr(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
