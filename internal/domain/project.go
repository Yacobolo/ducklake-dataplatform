package domain

import (
	"strings"
	"time"
)

const (
	// ProjectKindPersonal is a per-user draft authoring workspace.
	ProjectKindPersonal = "personal"
	// ProjectKindShared is a team-owned authoring workspace that can back products.
	ProjectKindShared   = "shared"
	// ProjectKindLibrary is a reusable compile-time workspace for shared macros and checks.
	ProjectKindLibrary  = "library"
)

const (
	// EnvironmentKindDevelopment is the non-production execution context for authoring.
	EnvironmentKindDevelopment = "development"
	// EnvironmentKindStaging is the pre-production execution context for shared validation.
	EnvironmentKindStaging     = "staging"
	// EnvironmentKindProduction is the production execution context for released builds.
	EnvironmentKindProduction  = "production"
)

const (
	// BuildStateDraft is an internal placeholder state before a build is ready.
	BuildStateDraft      = "draft"
	// BuildStateReady means a build can be used for validation or publication.
	BuildStateReady      = "ready"
	// BuildStateReleased marks the build currently backing a published product version.
	BuildStateReleased   = "released"
	// BuildStateSuperseded marks a build replaced by a newer released product version.
	BuildStateSuperseded = "superseded"
)

// Project is an internal authoring workspace that can produce product-backed builds.
type Project struct {
	ID             string
	Name           string
	Kind           string
	Description    string
	OwnerTeamID    *string
	OwnerPrincipal *string
	ProductID      *string
	DefaultBranch  string
	CreatedBy      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateProjectRequest defines the input for creating an internal authoring project.
type CreateProjectRequest struct {
	Name           string
	Kind           string
	Description    string
	OwnerTeamID    *string
	OwnerPrincipal *string
	ProductID      *string
	DefaultBranch  string
}

// UpdateProjectRequest defines mutable project fields.
type UpdateProjectRequest struct {
	Description   *string
	DefaultBranch *string
	ProductID     *string
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

// Build is an internal immutable compilation snapshot used to back product versions.
type Build struct {
	ID                 string
	ProjectID          string
	ProjectName        string
	ProductID          *string
	EnvironmentID      string
	EnvironmentName    string
	State              string
	GitRef             string
	CommitSHA          *string
	Selector           string
	TargetCatalog      string
	TargetSchema       string
	SourceModelRunID   *string
	CompileManifest    string
	CompileDiagnostics *string
	CreatedBy          string
	CreatedAt          time.Time
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
}

// Validate validates a project creation request.
func (r *CreateProjectRequest) Validate() error {
	return ValidateCreateProjectRequest(*r)
}

// ValidateCreateProjectRequest validates a project creation request.
func ValidateCreateProjectRequest(req CreateProjectRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return ErrValidation("project name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("project name must be at most 128 characters")
	}
	switch normalizeProjectKind(req.Kind) {
	case ProjectKindPersonal, ProjectKindShared, ProjectKindLibrary:
	default:
		return ErrValidation("unsupported project kind %q", req.Kind)
	}
	if strings.TrimSpace(defaultProjectBranch(req.DefaultBranch)) == "" {
		return ErrValidation("default branch is required")
	}

	ownerTeam := trimmedPtr(req.OwnerTeamID)
	ownerPrincipal := trimmedPtr(req.OwnerPrincipal)
	productID := trimmedPtr(req.ProductID)

	switch normalizeProjectKind(req.Kind) {
	case ProjectKindPersonal:
		if ownerPrincipal == nil {
			return ErrValidation("personal projects require owner_principal")
		}
		if ownerTeam != nil {
			return ErrValidation("personal projects cannot set owner_team_id")
		}
		if productID != nil {
			return ErrValidation("personal projects cannot attach to a product")
		}
	case ProjectKindShared:
		if ownerTeam == nil {
			return ErrValidation("shared projects require owner_team_id")
		}
		if ownerPrincipal != nil {
			return ErrValidation("shared projects cannot set owner_principal")
		}
	case ProjectKindLibrary:
		if ownerTeam == nil {
			return ErrValidation("library projects require owner_team_id")
		}
		if ownerPrincipal != nil {
			return ErrValidation("library projects cannot set owner_principal")
		}
		if productID != nil {
			return ErrValidation("library projects cannot attach to a product")
		}
	}
	return nil
}

// ValidateUpdateProjectRequest validates a project update request.
func ValidateUpdateProjectRequest(req UpdateProjectRequest) error {
	if req.DefaultBranch != nil && strings.TrimSpace(*req.DefaultBranch) == "" {
		return ErrValidation("default branch cannot be empty")
	}
	if req.ProductID != nil && strings.TrimSpace(*req.ProductID) == "" {
		return ErrValidation("product_id cannot be empty")
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
