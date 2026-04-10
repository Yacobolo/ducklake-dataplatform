package domain

import (
	"strings"
	"time"
)

const (
	// WorkspaceKindPersonal is a user-owned authoring workspace.
	WorkspaceKindPersonal = "personal"
	// WorkspaceKindShared is a team-owned collaborative authoring workspace.
	WorkspaceKindShared = "shared"
	// WorkspaceKindLibrary is a reusable shared authoring workspace.
	WorkspaceKindLibrary = "library"
)

// Workspace defines the top-level ownership and policy boundary for authored work.
type Workspace struct {
	ID                   string
	Name                 string
	Kind                 string
	OwnerTeamID          *string
	OwnerPrincipal       *string
	DefaultProjectID     *string
	DefaultEnvironmentID *string
	GitRepoID            *string
	GitRootPath          *string
	CreatedBy            string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// WorkspaceMember grants baseline access to a workspace.
type WorkspaceMember struct {
	WorkspaceID    string
	PrincipalName  string
	Role           string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CreateWorkspaceRequest defines the input for creating a workspace.
type CreateWorkspaceRequest struct {
	Name                 string
	Kind                 string
	OwnerTeamID          *string
	OwnerPrincipal       *string
	DefaultProjectID     *string
	DefaultEnvironmentID *string
	GitRepoID            *string
	GitRootPath          *string
}

// UpdateWorkspaceRequest defines mutable workspace fields.
type UpdateWorkspaceRequest struct {
	Name                 *string
	DefaultProjectID     *string
	DefaultEnvironmentID *string
	GitRepoID            *string
	GitRootPath          *string
}

// AddWorkspaceMemberRequest defines the input for adding a workspace member.
type AddWorkspaceMemberRequest struct {
	PrincipalName string
	Role          string
}

// Validate validates a workspace creation request.
func (r *CreateWorkspaceRequest) Validate() error {
	return ValidateCreateWorkspaceRequest(*r)
}

// ValidateCreateWorkspaceRequest validates a workspace creation request.
func ValidateCreateWorkspaceRequest(req CreateWorkspaceRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return ErrValidation("workspace name is required")
	}
	switch normalizeWorkspaceKind(req.Kind) {
	case WorkspaceKindPersonal, WorkspaceKindShared, WorkspaceKindLibrary:
	default:
		return ErrValidation("unsupported workspace kind %q", req.Kind)
	}

	ownerTeam := trimmedPtr(req.OwnerTeamID)
	ownerPrincipal := trimmedPtr(req.OwnerPrincipal)

	switch normalizeWorkspaceKind(req.Kind) {
	case WorkspaceKindPersonal:
		if ownerPrincipal == nil {
			return ErrValidation("personal workspaces require owner_principal")
		}
		if ownerTeam != nil {
			return ErrValidation("personal workspaces cannot set owner_team_id")
		}
	case WorkspaceKindShared, WorkspaceKindLibrary:
		if ownerTeam == nil {
			return ErrValidation("%s workspaces require owner_team_id", normalizeWorkspaceKind(req.Kind))
		}
		if ownerPrincipal != nil {
			return ErrValidation("%s workspaces cannot set owner_principal", normalizeWorkspaceKind(req.Kind))
		}
	}
	return nil
}

// ValidateUpdateWorkspaceRequest validates a workspace update request.
func ValidateUpdateWorkspaceRequest(req UpdateWorkspaceRequest) error {
	if req.Name != nil && strings.TrimSpace(*req.Name) == "" {
		return ErrValidation("workspace name cannot be empty")
	}
	return nil
}

// Validate validates an add-workspace-member request.
func (r *AddWorkspaceMemberRequest) Validate() error {
	if strings.TrimSpace(r.PrincipalName) == "" {
		return ErrValidation("principal_name is required")
	}
	if NormalizeShareRole(r.Role) == "" {
		return ErrValidation("role must be viewer, editor, or manager")
	}
	return nil
}

func normalizeWorkspaceKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", WorkspaceKindShared:
		return WorkspaceKindShared
	case WorkspaceKindPersonal:
		return WorkspaceKindPersonal
	case WorkspaceKindLibrary:
		return WorkspaceKindLibrary
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}
